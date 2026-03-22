"""
BBT/Quorum Pipeline Data Fetcher
4 pipelines on web-prd.myquorumcloud.com — no auth, no WAF.

- AlaTenn (tspno=3)
- Midla (tspno=6)
- Trans-Union Interstate Pipeline (tspno=12)
- Ozark Gas Transmission (tspno=16)

Locations: direct CSV download
IOC/OAC/Unsub: HTML table scrape

Runs weekly via GitHub Actions (bbt-refresh.yml).
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

BASE_URL = 'https://web-prd.myquorumcloud.com/BBTPA1IPWS'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
TIMEOUT = 60

PIPELINES = [
    {
        'tspno': 3,
        'name': 'BBT AlaTenn Pipeline',
        'short': 'AlaTenn',
        'tracker_name': 'American Midstream (AlaTenn)',
        'states': ['AL', 'TN'],
    },
    {
        'tspno': 6,
        'name': 'BBT Midla Pipeline',
        'short': 'Midla',
        'tracker_name': 'American Midstream (Midla)',
        'states': ['LA', 'MS'],
    },
    {
        'tspno': 12,
        'name': 'BBT Trans-Union Interstate Pipeline',
        'short': 'Trans-Union',
        'tracker_name': 'Trans-Union Interstate Pipeline',
        'states': ['LA'],
    },
    {
        'tspno': 16,
        'name': 'Ozark Gas Transmission, L.L.C.',
        'short': 'Ozark Gas',
        'tracker_name': 'Ozark Gas Transmission',
        'states': ['AR', 'OK'],
    },
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


def extract_table_rows(html):
    """Extract data rows from HTML <tbody> tables. Returns list of lists of cell text."""
    # Try to find tbody first
    tbody_matches = re.findall(r'<tbody[^>]*>(.*?)</tbody>', html, re.DOTALL | re.I)
    search_html = '\n'.join(tbody_matches) if tbody_matches else html

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', search_html, re.DOTALL | re.I)
    result = []
    for row_html in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.I)
        if not cells:
            continue
        cleaned = []
        for cell in cells:
            text = re.sub(r'<[^>]+>', '', cell).strip()
            text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
            text = text.replace('&nbsp;', ' ').replace('&#160;', ' ')
            text = re.sub(r'\s+', ' ', text).strip()
            cleaned.append(text)
        result.append(cleaned)
    return result


# ============================================================
# DATA FETCHERS
# ============================================================

def fetch_locations(tspno):
    """Download Locations CSV directly."""
    s = make_session()
    url = f'{BASE_URL}/IPWSFile/IPWSFileHandler?path=%5CTSP_{tspno}%5C&fileName=Locations%2FLOCATIONDATA.CSV&d=True'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Locations returned {r.status_code}")
        return None
    print(f"    Locations: {len(r.content):,} bytes")
    return r.content


def parse_locations_csv(content):
    """Parse Quorum Locations CSV into dict indexed by LOC."""
    text = content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(text))
    locs = {}
    for row in reader:
        loc_id = str(row.get('LOC', '')).strip()
        if loc_id:
            locs[loc_id] = row
    return locs


def fetch_ioc(tspno):
    """Fetch IOC HTML page."""
    s = make_session()
    url = f'{BASE_URL}/IndexOfCust?tspno={tspno}'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    IOC returned {r.status_code}")
        return None
    print(f"    IOC: {len(r.text):,} bytes")
    return r.text


def parse_ioc_html(html):
    """Parse IOC HTML table.

    Standard FERC IOC columns in HTML table rows.
    Returns dict with contracts, by_point aggregates, and totals.
    """
    cutoff = datetime.now() + timedelta(days=730)
    rows = extract_table_rows(html)

    contracts = {}
    by_point = defaultdict(lambda: {'firm_mdq': 0, 'expiring_2yr': 0, 'num_contracts': 0, 'shippers': set()})
    all_shippers = set()
    total_mdq = 0

    for cells in rows:
        if len(cells) < 10:
            continue

        # Try to identify IOC columns — typical order:
        # Shipper, Affiliate, Rate Sched, K#, Amend, Begin, End, Nego Rate, MDQ, MSQ,
        # Pt ID, Pt Name, Zone, Pt MDQ, ...
        shipper = cells[0].strip()
        if not shipper or shipper.lower() in ('shipper', 'shipper name', 'k holder name', ''):
            continue

        rate = cells[2].strip() if len(cells) > 2 else ''
        contract_id = cells[3].strip() if len(cells) > 3 else ''
        begin_date = cells[5].strip() if len(cells) > 5 else ''
        end_date = cells[6].strip() if len(cells) > 6 else ''

        # Find MDQ — typically around index 8-9
        mdq = 0
        for i in [8, 9, 7]:
            if i < len(cells):
                val = parse_int_safe(cells[i])
                if val > 0:
                    mdq = val
                    break

        if mdq == 0:
            continue

        # Find point ID — typically around index 10-11
        point_id = ''
        point_name = ''
        for i in [10, 11]:
            if i < len(cells) and cells[i].strip() and any(c.isdigit() for c in cells[i]):
                point_id = cells[i].strip()
                point_name = cells[i + 1].strip() if i + 1 < len(cells) else ''
                break

        all_shippers.add(shipper)
        total_mdq += mdq

        is_firm = 'FT' in rate.upper() or 'FIRM' in rate.upper()
        is_expiring = False
        if end_date:
            try:
                ed = datetime.strptime(end_date.strip()[:10], '%m/%d/%Y')
                if ed <= cutoff:
                    is_expiring = True
            except (ValueError, IndexError):
                pass

        if contract_id and contract_id not in contracts:
            contracts[contract_id] = {
                'shipper': shipper,
                'rate_schedule': rate,
                'contract_id': contract_id,
                'begin_date': begin_date,
                'end_date': end_date,
                'mdq_dth': mdq,
            }

        if point_id:
            by_point[point_id]['num_contracts'] += 1
            by_point[point_id]['shippers'].add(shipper)
            if is_firm:
                by_point[point_id]['firm_mdq'] += mdq
            if is_expiring:
                by_point[point_id]['expiring_2yr'] += mdq

    by_point_out = {}
    for loc_id, info in by_point.items():
        by_point_out[loc_id] = {
            'firm_mdq': info['firm_mdq'],
            'expiring_2yr': info['expiring_2yr'],
            'num_contracts': info['num_contracts'],
            'num_shippers': len(info['shippers']),
        }

    return {
        'contracts': list(contracts.values()),
        'by_point': by_point_out,
        'total_mdq': total_mdq,
        'num_contracts': len(contracts),
        'num_shippers': len(all_shippers),
    }


def fetch_oac(tspno):
    """Fetch OAC HTML page."""
    s = make_session()
    url = f'{BASE_URL}/OpAvailPosting?tspno={tspno}'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    OAC returned {r.status_code}")
        return None
    print(f"    OAC: {len(r.text):,} bytes")
    return r.text


def parse_oac_html(html):
    """Parse OAC HTML table. Returns dict indexed by Loc ID."""
    rows = extract_table_rows(html)
    oac = {}

    for cells in rows:
        if len(cells) < 6:
            continue

        # Find loc ID — look for a numeric cell in the first few positions
        loc_id = ''
        for i in range(min(4, len(cells))):
            c = cells[i].strip()
            if c and any(ch.isdigit() for ch in c) and len(c) < 15:
                loc_id = c
                break

        if not loc_id:
            continue

        # Skip header rows
        if loc_id.lower() in ('loc', 'location', 'loc prop'):
            continue

        # Find capacity values from numeric columns
        nums = []
        for c in cells[3:]:
            nums.append(parse_int_safe(c))

        # Standard OAC columns after location info: Design Cap, Operating Cap, Scheduled, Available
        design = 0
        scheduled = 0
        available = 0
        for i, n in enumerate(nums):
            if n > 0 and design == 0:
                design = n
            elif n >= 0 and design > 0 and scheduled == 0 and i > 0:
                scheduled = n
                # Available is typically right after scheduled
                if i + 1 < len(nums):
                    available = nums[i + 1]
                break

        if design > 0 or available > 0:
            oac[loc_id] = {
                'design': design,
                'scheduled': scheduled,
                'available': available,
            }

    return oac


def fetch_unsub(tspno):
    """Fetch Unsub HTML page."""
    s = make_session()
    url = f'{BASE_URL}/UnsubscribedCapacity?tspno={tspno}'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Unsub returned {r.status_code}")
        return None
    print(f"    Unsub: {len(r.text):,} bytes")
    return r.text


def parse_unsub_html(html):
    """Parse Unsub HTML table."""
    rows = extract_table_rows(html)
    result = []
    seen_locs = set()

    for cells in rows:
        if len(cells) < 4:
            continue

        # Find loc ID and unsub capacity
        loc_id = ''
        loc_name = ''
        for i in range(min(4, len(cells))):
            c = cells[i].strip()
            if c and any(ch.isdigit() for ch in c) and len(c) < 15:
                loc_id = c
                loc_name = cells[i + 1].strip() if i + 1 < len(cells) else ''
                break

        if not loc_id or loc_id.lower() in ('loc', 'location', 'loc prop'):
            continue

        # Find unsub capacity — last significant numeric value
        unsub_val = 0
        for i in range(len(cells) - 1, 2, -1):
            val = parse_int_safe(cells[i])
            if val > 0:
                unsub_val = val
                break

        if unsub_val > 0:
            if loc_id not in seen_locs:
                result.append({
                    'Loc': loc_id,
                    'Loc_Name': loc_name[:50],
                    'Loc_Purp_Desc': '',
                    'Unsubscribed_Capacity': unsub_val,
                })
                seen_locs.add(loc_id)
            else:
                for existing in result:
                    if existing['Loc'] == loc_id:
                        existing['Unsubscribed_Capacity'] += unsub_val
                        break

    return result


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

def build_points(loc_data, by_point, oac_data, county_coords):
    """Build points list from locations, IOC, and OAC data."""
    new_counties = []
    points = []
    point_ids_seen = set()

    for loc_id, loc in loc_data.items():
        county = str(loc.get('LOC COUNTY', loc.get('LOC CNTY ABBREV', ''))).strip()
        state = str(loc.get('LOC ST ABBREV', '')).strip()
        flow = str(loc.get('DIR FLO', '')).strip()
        loc_name = str(loc.get('LOC NAME', '')).strip()
        connected = str(loc.get('UP/DN NAME', '')).strip()[:50]
        loc_type = str(loc.get('LOC TYPE', loc.get('LOC TYPE IND', ''))).strip()

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
        oac_info = oac_data.get(loc_id, {})

        pt = {
            'id': loc_id,
            'name': loc_name[:50],
            'type': ptype,
            'county': county,
            'state': state,
            'design': oac_info.get('design', 0),
            'scheduled': oac_info.get('scheduled', 0),
            'available': oac_info.get('available', 0),
            'utilization': round(oac_info['scheduled'] / oac_info['design'] * 100) if oac_info.get('design', 0) > 0 else 0,
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
    tspno = pl['tspno']
    short = pl['short']

    print(f"\n--- {short} (tspno={tspno}) ---")

    # Locations CSV
    print("  Fetching Locations...")
    loc_content = fetch_locations(tspno)
    loc_data = parse_locations_csv(loc_content) if loc_content else {}
    print(f"    Locations: {len(loc_data)} points")

    # IOC
    print("  Fetching IOC...")
    ioc_html = fetch_ioc(tspno)
    ioc_data = parse_ioc_html(ioc_html) if ioc_html else {
        'contracts': [], 'by_point': {}, 'total_mdq': 0, 'num_contracts': 0, 'num_shippers': 0
    }
    print(f"    IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    # OAC
    print("  Fetching OAC...")
    oac_html = fetch_oac(tspno)
    oac_data = parse_oac_html(oac_html) if oac_html else {}
    print(f"    OAC: {len(oac_data)} points")

    # Unsub
    print("  Fetching Unsub...")
    unsub_html = fetch_unsub(tspno)
    unsub_data = parse_unsub_html(unsub_html) if unsub_html else []
    print(f"    Unsub: {len(unsub_data)} points")

    # Build points
    by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points(loc_data, by_point, oac_data, county_coords)

    geocoded_count = sum(1 for pt in points if 'lat' in pt)
    print(f"  {short}: {len(points)} pts ({geocoded_count} geocoded), {len(unsub_data)} unsub, {ioc_data['num_contracts']} IOC")

    return {
        'entry': {
            'name': pl['name'],
            'short': short,
            'updated': TODAY,
            'points': points,
            'unsub_points': unsub_data,
            'ioc_totals': {
                'firm_mdq': ioc_data['total_mdq'],
                'num_contracts': ioc_data['num_contracts'],
                'num_shippers': ioc_data['num_shippers'],
            },
        },
        'ioc_data': ioc_data,
        'unsub_data': unsub_data,
        'oac_data': oac_data,
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
        tspno = pl['tspno']

        entry = None
        for e in pipelines_list:
            if e.get('pipeline_name') == tracker_name:
                entry = e
                break

        if not entry:
            entry = {
                'pipeline_name': tracker_name,
                'operator': 'BBT / Third Coast Midstream',
                'regulation': 'interstate',
                'hifld_points': 0,
            }
            pipelines_list.append(entry)
            print(f"  Added new tracker entry: {tracker_name}")

        base = f'{BASE_URL}'
        entry['ioc'] = {
            'status': 'captured',
            'access_method': 'weekly_auto',
            'platform': 'quorum_cloud',
            'url': f'{base}/IndexOfCust?tspno={tspno}',
            'last_refreshed': TODAY,
            'refresh_frequency_days': 7,
            'cloud_accessible': True,
            'contract_count': r['ioc_data']['num_contracts'],
            'shipper_count': r['ioc_data']['num_shippers'],
        }
        if r['unsub_data']:
            entry['unsub'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'quorum_cloud',
                'url': f'{base}/UnsubscribedCapacity?tspno={tspno}',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'point_count': len(r['unsub_data']),
            }
        else:
            entry['unsub'] = {
                'status': 'attempted',
                'access_method': 'weekly_auto',
                'platform': 'quorum_cloud',
                'cloud_accessible': True,
            }
        if r['oac_data']:
            entry['capacity'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'quorum_cloud',
                'url': f'{base}/OpAvailPosting?tspno={tspno}',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'point_count': len(r['oac_data']),
            }
        if r['loc_data']:
            entry['locations'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'quorum_cloud',
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
    print(f"=== BBT/Quorum Pipeline Refresh: {TODAY} ===")

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
        time.sleep(2)

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

        # Re-apply coordinates
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

    merge_into_gas_interconnects(results)
    update_tracker(results)

    print(f"\n=== Done — {len(results)} pipelines processed ===")


if __name__ == '__main__':
    main()
