"""
TC Energy eConnects Infopost Data Fetcher
11 pipelines at ebb.tceconnects.com/infopost/ — all public, no login needed.

Locations: SSRS ReportViewer CSV (direct download, 30-60s for large pipelines)
IOC: S3-backed document store, TAB files in FERC H/D/A/P format
OAC/Unsub: NOT available (behind eConnects Angular app login)

Runs weekly via GitHub Actions (tce-refresh.yml).
"""

import requests
import csv
import json
import os
import io
import re
import time
from datetime import datetime, timedelta
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
OUTPUT_FILE = os.path.join(DATA_DIR, 'gas_interconnects.json')
TRACKER_FILE = os.path.join(DATA_DIR, 'corridor_pipeline_tracker.json')
COUNTY_CACHE = os.path.join(DATA_DIR, 'gas_county_coords.json')
TODAY = datetime.now().strftime('%Y-%m-%d')

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
LOC_TIMEOUT = 90   # Locations CSV can be very slow
IOC_TIMEOUT = 60

PIPELINES = [
    {'name': 'ANR Pipeline', 'asset_id': 3005, 'folder': 'anr', 'short': 'ANR', 'tracker_name': 'ANR Pipeline', 'hifld_company': 'ANR PIPELINE COMPANY'},
    {'name': 'ANR Storage', 'asset_id': 3009, 'folder': 'anrsc', 'short': 'ANR Storage', 'tracker_name': 'ANR Storage', 'hifld_company': 'ANR STORAGE COMPANY'},
    {'name': 'Bison Pipeline', 'asset_id': 3031, 'folder': 'bison', 'short': 'Bison', 'tracker_name': 'Bison Pipeline', 'hifld_company': 'BISON PIPELINE LLC'},
    {'name': 'Blue Lake Gas Storage', 'asset_id': 3014, 'folder': 'blgsc', 'short': 'Blue Lake', 'tracker_name': 'Blue Lake Gas Storage', 'hifld_company': 'BLUE LAKE GAS STORAGE COMPANY'},
    {'name': 'Columbia Gas Transmission', 'asset_id': 51, 'folder': 'tco', 'short': 'Columbia Gas', 'tracker_name': 'Columbia Gas Transmission', 'hifld_company': 'COLUMBIA GAS TRANSMISSION, LLC'},
    {'name': 'Columbia Gulf Transmission', 'asset_id': 14, 'folder': 'cgt', 'short': 'Columbia Gulf', 'tracker_name': 'Columbia Gulf Transmission', 'hifld_company': 'COLUMBIA GULF TRANSMISSION, LLC'},
    {'name': 'Crossroads Pipeline', 'asset_id': 44, 'folder': 'xrd', 'short': 'Crossroads', 'tracker_name': 'Crossroads Pipeline', 'hifld_company': 'CROSSROADS PIPELINE COMPANY'},
    {'name': 'Hardy Storage', 'asset_id': 465, 'folder': 'hrd', 'short': 'Hardy Storage', 'tracker_name': 'Hardy Storage', 'hifld_company': 'HARDY STORAGE COMPANY, LLC'},
    {'name': 'Millennium Pipeline', 'asset_id': 26, 'folder': 'mpl', 'short': 'Millennium', 'tracker_name': 'Millennium Pipeline', 'hifld_company': 'MILLENNIUM PIPELINE COMPANY, L.L.C.'},
    {'name': 'Northern Border Pipeline', 'asset_id': 3029, 'folder': 'nbpl', 'short': 'Northern Border', 'tracker_name': 'Northern Border Pipeline', 'hifld_company': 'NORTHERN BORDER PIPELINE COMPANY'},
    {'name': 'TC Louisiana Intrastate', 'asset_id': 3119, 'folder': 'tcli', 'short': 'TC Louisiana', 'tracker_name': 'TC Louisiana Intrastate', 'hifld_company': 'TC LOUISIANA INTRASTATE PIPELINE LLC', 'no_ioc': True},
]


# ============================================================
# HELPERS
# ============================================================

def parse_int_safe(val):
    if val is None:
        return 0
    try:
        return int(str(val).replace(',', '').replace('$', '').strip())
    except (ValueError, TypeError):
        return 0


def make_session():
    s = requests.Session()
    s.headers['User-Agent'] = UA
    return s


# ============================================================
# LOCATIONS CSV
# ============================================================

def fetch_locations(asset_id):
    """Download Locations CSV via SSRS ReportViewer. Can take 30-60s."""
    s = make_session()
    url = (
        f'https://ebb.tceconnects.com/infopost/ReportViewer.aspx'
        f'?/InfoPost/LocationDataDownload&assetNbr={asset_id}'
        f'&rs:Format=CSV&rc:NoHeader=true'
    )
    r = s.get(url, timeout=LOC_TIMEOUT)
    if r.status_code != 200:
        print(f"    Locations returned {r.status_code}")
        return None
    # SSRS may return HTML error page instead of CSV
    if '<html' in r.text[:200].lower():
        print(f"    Locations returned HTML error page")
        return None
    print(f"    Locations: {len(r.content):,} bytes")
    return r.content


def parse_locations_csv(content):
    """Parse TC Energy Locations CSV into dict indexed by Loc."""
    text = content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(text))
    locs = {}
    for row in reader:
        loc_id = str(row.get('Loc', '')).strip()
        if not loc_id:
            continue
        locs[loc_id] = {
            'Loc': loc_id,
            'Loc Name': str(row.get('Loc Name', '')).strip(),
            'Loc Type Ind': str(row.get('Loc Type Ind', '')).strip(),
            'Dir Flo': str(row.get('Dir Flo', '')).strip(),
            'Loc Zone': str(row.get('Loc Zone', '')).strip(),
            'Loc Cnty': str(row.get('Loc Cnty', '')).strip(),
            'Loc St Abbrev': str(row.get('Loc St Abbrev', '')).strip(),
            'Up/Dn Name': str(row.get('Up/Dn Name', '')).strip(),
        }
    return locs


# ============================================================
# IOC (two-step: document list → download TAB)
# ============================================================

def fetch_ioc_file_list(folder):
    """Get IOC document list. CRITICAL: single backslash in folder path."""
    s = make_session()
    # Single backslash path: \folder\indexofcustomers
    url = f'https://ebb.tceconnects.com/infopost/webmethods/Documents_List.aspx?Folder=\\{folder}\\indexofcustomers'
    r = s.get(url, timeout=IOC_TIMEOUT)
    if r.status_code != 200:
        print(f"    IOC doc list returned {r.status_code}")
        return []

    try:
        data = r.json()
    except Exception:
        print(f"    IOC doc list: invalid JSON")
        return []

    rows = data.get('rows', [])
    return rows


def find_tab_file(rows):
    """Find the .TAB file from document list rows."""
    for row in rows:
        cells = row.get('cell', [])
        if len(cells) < 3:
            continue
        filename = cells[2] if len(cells) > 2 else ''
        if filename.upper().endswith('.TAB'):
            key = cells[0]  # S3 key path
            return key, filename
    return None, None


def download_ioc_tab(key, filename):
    """Download an IOC TAB file from S3 via DownloadFile.aspx."""
    s = make_session()
    url = f'https://ebb.tceconnects.com/infopost/webmethods/DownloadFile.aspx?Mode=V&S3K={key}&S3FN={filename}'
    r = s.get(url, timeout=IOC_TIMEOUT)
    if r.status_code != 200:
        print(f"    IOC download returned {r.status_code}")
        return None
    print(f"    IOC: {len(r.content):,} bytes ({filename})")
    return r.content


def parse_ioc_tab(content):
    """Parse FERC H/D/A/P tab-delimited IOC file."""
    text = content.decode('utf-8-sig')
    reader = csv.reader(io.StringIO(text), delimiter='\t')
    cutoff = datetime.now() + timedelta(days=730)

    contracts = {}
    by_point = defaultdict(lambda: {'firm_mdq': 0, 'expiring_2yr': 0, 'num_contracts': 0, 'shippers': set()})
    all_shippers = set()
    total_mdq = 0
    current_contract = None

    for row in reader:
        if not row:
            continue
        row_type = row[0].strip()

        if row_type == 'D':
            if len(row) < 11:
                continue
            shipper = row[1].strip()
            rate = row[4].strip()
            contract_id = row[5].strip()
            begin_date = row[6].strip()
            end_date = row[7].strip()
            mdq = parse_int_safe(row[10])

            if not shipper or mdq == 0:
                continue

            all_shippers.add(shipper)
            total_mdq += mdq

            current_contract = {
                'shipper': shipper,
                'rate_schedule': rate,
                'contract_id': contract_id,
                'begin_date': begin_date,
                'end_date': end_date,
                'mdq_dth': mdq,
                'points': [],
                '_is_firm': 'FT' in rate.upper() or 'FIRM' in rate.upper(),
                '_expiring': False,
            }

            if end_date:
                try:
                    ed = datetime.strptime(end_date.strip()[:10], '%m/%d/%Y')
                    if ed <= cutoff:
                        current_contract['_expiring'] = True
                except (ValueError, IndexError):
                    pass

            if contract_id not in contracts:
                contracts[contract_id] = current_contract

        elif row_type == 'P' and current_contract:
            if len(row) < 7:
                continue
            point_id = row[1].strip()
            point_name = row[2].strip()
            pt_mdq = parse_int_safe(row[6])

            if not point_id:
                continue

            current_contract['points'].append({
                'loc_id': point_id,
                'loc_nm': point_name,
                'qty': pt_mdq or current_contract['mdq_dth'],
            })

            qty = pt_mdq or current_contract['mdq_dth']
            by_point[point_id]['num_contracts'] += 1
            by_point[point_id]['shippers'].add(current_contract['shipper'])
            if current_contract['_is_firm']:
                by_point[point_id]['firm_mdq'] += qty
            if current_contract['_expiring']:
                by_point[point_id]['expiring_2yr'] += qty

    by_point_out = {}
    for loc_id, info in by_point.items():
        by_point_out[loc_id] = {
            'firm_mdq': info['firm_mdq'],
            'expiring_2yr': info['expiring_2yr'],
            'num_contracts': info['num_contracts'],
            'num_shippers': len(info['shippers']),
        }

    contract_list = list(contracts.values())
    for c in contract_list:
        c.pop('_is_firm', None)
        c.pop('_expiring', None)

    return {
        'contracts': contract_list,
        'by_point': by_point_out,
        'total_mdq': total_mdq,
        'num_contracts': len(contract_list),
        'num_shippers': len(all_shippers),
    }


EMPTY_IOC = {'contracts': [], 'by_point': {}, 'total_mdq': 0, 'num_contracts': 0, 'num_shippers': 0}


# ============================================================
# COUNTY GEOCODING
# ============================================================

def load_county_coords():
    if os.path.exists(COUNTY_CACHE):
        with open(COUNTY_CACHE) as f:
            return json.load(f)
    return {}


def save_county_coords(coords):
    with open(COUNTY_CACHE, 'w') as f:
        json.dump(coords, f)


def geocode_county(county, state):
    try:
        url = 'https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress'
        params = {
            'address': f'{county} County, {state}',
            'benchmark': 'Public_AR_Current',
            'vintage': 'Current_Current',
            'format': 'json',
        }
        r = requests.get(url, params=params, timeout=10)
        d = r.json()
        matches = d.get('result', {}).get('addressMatches', [])
        if matches:
            coords = matches[0].get('coordinates', {})
            return coords.get('y'), coords.get('x')
    except Exception:
        pass
    return None, None


# ============================================================
# POINT BUILDING
# ============================================================

def build_points(loc_data, by_point, county_coords):
    """Build points from Locations CSV, enriched with IOC data."""
    new_counties = []
    points = []
    point_ids_seen = set()

    for loc_id, loc in loc_data.items():
        county = loc.get('Loc Cnty', '')
        state = loc.get('Loc St Abbrev', '')
        flow = loc.get('Dir Flo', '')
        loc_name = loc.get('Loc Name', '')
        connected = loc.get('Up/Dn Name', '')[:50]

        ptype = 'delivery'
        if flow in ('R', 'Receipt'):
            ptype = 'receipt'
        elif flow in ('B', 'Both', 'Bidirectional'):
            ptype = 'bidirectional'

        lat, lng = None, None
        if county and state:
            key = f"{county.upper()}|{state}"
            if key in county_coords:
                lat, lng = county_coords[key].get('lat'), county_coords[key].get('lng')
            else:
                new_counties.append(key)

        ioc_info = by_point.get(loc_id, {})

        pt = {
            'id': loc_id,
            'name': loc_name[:50],
            'type': ptype,
            'county': county,
            'state': state,
            'design': 0,
            'scheduled': 0,
            'available': 0,
            'utilization': 0,
            'connected': connected,
            'firm_contracted': ioc_info.get('firm_mdq', 0),
            'num_contracts': ioc_info.get('num_contracts', 0),
            'num_shippers': ioc_info.get('num_shippers', 0),
            'expiring_2yr': ioc_info.get('expiring_2yr', 0),
        }
        if lat and lng:
            pt['lat'] = lat
            pt['lng'] = lng
            pt['loc_accuracy'] = 'county_centroid'

        points.append(pt)
        point_ids_seen.add(loc_id)

    # Add IOC-only points not in locations
    for loc_id, info in by_point.items():
        if loc_id not in point_ids_seen:
            points.append({
                'id': loc_id, 'name': '', 'type': 'other',
                'county': '', 'state': '',
                'design': 0, 'scheduled': 0, 'available': 0, 'utilization': 0,
                'connected': '',
                'firm_contracted': info.get('firm_mdq', 0),
                'num_contracts': info.get('num_contracts', 0),
                'num_shippers': info.get('num_shippers', 0),
                'expiring_2yr': info.get('expiring_2yr', 0),
            })

    return points, list(set(new_counties))


# ============================================================
# PIPELINE PROCESSING
# ============================================================

def process_pipeline(pl, county_coords):
    short = pl['short']
    asset_id = pl['asset_id']
    folder = pl['folder']

    print(f"\n--- {short} (asset={asset_id}, folder={folder}) ---")

    # Locations
    print("  Fetching Locations...")
    try:
        loc_content = fetch_locations(asset_id)
        loc_data = parse_locations_csv(loc_content) if loc_content else {}
    except Exception as e:
        print(f"    Locations error: {e}")
        loc_data = {}
    print(f"    Locations: {len(loc_data)} points")

    time.sleep(2)

    # IOC
    ioc_data = EMPTY_IOC.copy()
    if not pl.get('no_ioc'):
        print("  Fetching IOC...")
        try:
            rows = fetch_ioc_file_list(folder)
            if rows:
                key, filename = find_tab_file(rows)
                if key and filename:
                    content = download_ioc_tab(key, filename)
                    if content:
                        ioc_data = parse_ioc_tab(content)
                else:
                    print(f"    No TAB file found in {len(rows)} documents")
            else:
                print(f"    No IOC documents found")
        except Exception as e:
            print(f"    IOC error: {e}")
    else:
        print("  Skipping IOC (not published for this pipeline)")

    print(f"    IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    # Build points
    by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points(loc_data, by_point, county_coords)

    geocoded_count = sum(1 for pt in points if 'lat' in pt)
    print(f"  {short}: {len(points)} pts ({geocoded_count} geocoded), {ioc_data['num_contracts']} IOC")

    return {
        'entry': {
            'name': pl['name'],
            'short': short,
            'updated': TODAY,
            'points': points,
            'unsub_points': [],
            'ioc_totals': {
                'firm_mdq': ioc_data['total_mdq'],
                'num_contracts': ioc_data['num_contracts'],
                'num_shippers': ioc_data['num_shippers'],
            },
        },
        'ioc_data': ioc_data,
        'loc_data': loc_data,
        'points': points,
        'new_counties': new_counties,
        'geocoded_count': geocoded_count,
    }


# ============================================================
# MERGE AND TRACKER
# ============================================================

def merge_into_gas_interconnects(results):
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            gi = json.load(f)
    else:
        gi = {'pipelines': []}

    shorts = {r['entry']['short'] for r in results}
    gi['pipelines'] = [p for p in gi['pipelines'] if p.get('short') not in shorts]
    for r in results:
        gi['pipelines'].append(r['entry'])

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(gi, f)
    print(f"\ngas_interconnects.json: {len(gi['pipelines'])} total pipelines")


def update_tracker(results):
    if not os.path.exists(TRACKER_FILE):
        return

    with open(TRACKER_FILE) as f:
        tracker = json.load(f)

    pipelines_list = tracker.get('gas_pipelines', [])

    for r in results:
        pl = r['_pipeline']
        tracker_name = pl['tracker_name']

        entry = None
        for e in pipelines_list:
            if e.get('pipeline_name') == tracker_name:
                entry = e
                break

        if not entry:
            entry = {
                'pipeline_name': tracker_name,
                'hifld_company': pl.get('hifld_company', ''),
                'operator': 'TC Energy',
                'regulation': 'interstate',
                'hifld_points': 0,
            }
            pipelines_list.append(entry)
            print(f"  Added new tracker entry: {tracker_name}")

        # IOC
        if pl.get('no_ioc'):
            entry['ioc'] = {
                'status': 'not_published',
                'access_method': 'weekly_auto',
                'platform': 'tce_infopost',
                'cloud_accessible': True,
                'notes': 'No IOC folder on TC Energy infopost for this pipeline',
            }
        elif r['ioc_data']['num_contracts'] > 0:
            entry['ioc'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'tce_infopost',
                'url': 'https://ebb.tceconnects.com/infopost/',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'contract_count': r['ioc_data']['num_contracts'],
                'shipper_count': r['ioc_data']['num_shippers'],
            }
        else:
            entry['ioc'] = {
                'status': 'attempted',
                'access_method': 'weekly_auto',
                'platform': 'tce_infopost',
                'url': 'https://ebb.tceconnects.com/infopost/',
                'cloud_accessible': True,
                'notes': 'IOC folder exists but TAB file empty or download failed',
            }

        # Unsub — behind login
        entry['unsub'] = {
            'status': 'exists_not_captured',
            'access_method': 'login_required',
            'platform': 'tce_econnects_app',
            'url': 'https://ebb.tceconnects.com/app/',
            'notes': 'Behind TC eConnects Angular app login',
        }

        # Locations
        if r['loc_data']:
            entry['locations'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'tce_infopost',
                'cracked_points': len(r['loc_data']),
                'geocoded_points': r['geocoded_count'],
                'geocode_method': 'county_centroid',
                'cloud_accessible': True,
            }

        print(f"  Tracker updated: {tracker_name}")

    with open(TRACKER_FILE, 'w') as f:
        json.dump(tracker, f, indent=2)


# ============================================================
# MAIN
# ============================================================

def main():
    print(f"=== TC Energy Pipeline Refresh: {TODAY} ===")

    county_coords = load_county_coords()
    results = []

    for pl in PIPELINES:
        try:
            r = process_pipeline(pl, county_coords)
            r['_pipeline'] = pl
            results.append(r)
        except Exception as e:
            print(f"  ERROR {pl['short']}: {e}")
            import traceback; traceback.print_exc()
        time.sleep(3)  # Rate limiting — 2s delay between pipelines

    # Geocode all new counties
    all_new = set()
    for r in results:
        all_new.update(r.get('new_counties', []))

    if all_new:
        print(f"\nGeocoding {len(all_new)} new counties...")
        geocoded = 0
        for key in all_new:
            county, state = key.split('|')
            lat, lng = geocode_county(county, state)
            if lat and lng:
                county_coords[key] = {'lat': lat, 'lng': lng}
                geocoded += 1
            time.sleep(0.5)
        save_county_coords(county_coords)
        print(f"  Geocoded {geocoded}/{len(all_new)}")

        for r in results:
            for pt in r['points']:
                county = pt.get('county', '').upper()
                state = pt.get('state', '')
                if county and state and 'lat' not in pt:
                    key = f"{county}|{state}"
                    if key in county_coords:
                        pt['lat'] = county_coords[key]['lat']
                        pt['lng'] = county_coords[key]['lng']
                        pt['loc_accuracy'] = 'county_centroid'

    if not results:
        print("\nNo pipelines processed!")
        return

    # Summary
    print("\n--- Summary ---")
    for r in results:
        pl = r['_pipeline']
        geocoded = sum(1 for pt in r['points'] if 'lat' in pt)
        print(f"  {pl['short']}: {len(r['points'])} pts ({geocoded} geocoded), {r['ioc_data']['num_contracts']} IOC")

    merge_into_gas_interconnects(results)
    update_tracker(results)

    print(f"\n=== Done — {len(results)} pipelines processed ===")


if __name__ == '__main__':
    main()
