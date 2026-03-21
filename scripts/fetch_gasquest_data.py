"""
Gasquest/Boardwalk Pipeline Data Fetcher
Downloads IOC, Unsub, OAC, and Locations for Gulf South Pipeline and Texas Gas Transmission
via the Gasquest REST API at reporting.prod.bwpmlp.org — no auth required.

Two-step process per posting type:
  1. POST to /infopost/infopostdetails to get latest posting with document IDs
  2. GET /infopost/postings?postingsDocumentId={id} to download the actual file

Runs weekly via GitHub Actions (gasquest-refresh.yml).
"""

import requests
import csv
import json
import os
import io
import time
from datetime import datetime, timedelta
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
OUTPUT_FILE = os.path.join(DATA_DIR, 'gas_interconnects.json')
TRACKER_FILE = os.path.join(DATA_DIR, 'corridor_pipeline_tracker.json')
COUNTY_CACHE = os.path.join(DATA_DIR, 'gas_county_coords.json')
TODAY = datetime.now().strftime('%Y-%m-%d')

API_BASE = 'https://reporting.prod.bwpmlp.org/infopost'
API_HEADERS = {
    'Content-Type': 'application/json',
    'Origin': 'https://www.gasquest.com',
    'Referer': 'https://www.gasquest.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}
TIMEOUT = 60

PIPELINES = [
    {
        'tsp_id': 1,
        'name': 'Gulf South Pipeline Company, LP',
        'short': 'Gulf South',
        'tracker_name': 'Gulf South',
        'hifld_points': 836,
        'posting_types': {
            'ioc': 5,
            'unsub': 2,
            'oac': 1,
            'locations': 9,
            'unsub_ftsa': 46,
        },
    },
    {
        'tsp_id': 6,
        'name': 'Texas Gas Transmission, LLC',
        'short': 'Texas Gas',
        'tracker_name': 'Texas Gas Transmission',
        'hifld_points': 222,
        'posting_types': {
            'ioc': 5,
            'unsub': 2,
            'oac': 1,
            'locations': 9,
        },
    },
]


# ============================================================
# API HELPERS
# ============================================================

def get_latest_posting(tsp_id, info_post_id):
    """POST to get the latest posting list for a given pipeline and posting type."""
    url = f'{API_BASE}/infopostdetails'
    body = {
        'infoPostID': info_post_id,
        'tspId': tsp_id,
        'pageNumber': 1,
        'pageSize': 5,
        'sortBy': 'datetimePostingEffective',
        'sortDescending': True,
        'groupCode': 'INFOPOST',
    }
    r = requests.post(url, json=body, headers=API_HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Posting list returned {r.status_code}")
        return None
    data = r.json()
    postings = data.get('postings', [])
    if not postings:
        print(f"    No postings found")
        return None
    return postings[0]


def find_structured_file(report_files, prefer_ext='.tab'):
    """Pick the structured file (.TAB or .CSV) from reportFiles, not the .pdf."""
    if not report_files:
        return None

    # First pass: exact extension match
    for rf in report_files:
        fn = rf.get('fileName', '').lower()
        if fn.endswith(prefer_ext):
            return rf

    # Second pass: any CSV
    for rf in report_files:
        fn = rf.get('fileName', '').lower()
        if fn.endswith('.csv'):
            return rf

    # Third pass: any TAB
    for rf in report_files:
        fn = rf.get('fileName', '').lower()
        if fn.endswith('.tab'):
            return rf

    # Skip PDFs — return None if only PDFs available
    for rf in report_files:
        fn = rf.get('fileName', '').lower()
        if not fn.endswith('.pdf'):
            return rf

    return None


def download_document(tracker_id):
    """Download a document by its infoPostTrackerID."""
    url = f'{API_BASE}/postings?postingsDocumentId={tracker_id}'
    r = requests.get(url, headers=API_HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Download returned {r.status_code}")
        return None
    return r.content


def fetch_posting_file(tsp_id, info_post_id, label, prefer_ext='.tab'):
    """Full two-step fetch: get posting list, find structured file, download it."""
    posting = get_latest_posting(tsp_id, info_post_id)
    if not posting:
        return None

    report_files = posting.get('reportFiles', [])
    structured = find_structured_file(report_files, prefer_ext)
    if not structured:
        fnames = [rf.get('fileName', '') for rf in report_files]
        print(f"    No structured file found in: {fnames}")
        return None

    tracker_id = structured.get('infoPostTrackerID')
    fname = structured.get('fileName', '')
    print(f"    {label}: downloading {fname} (ID: {tracker_id})")

    content = download_document(tracker_id)
    if not content:
        return None

    print(f"    {label}: {len(content):,} bytes")
    return content


# ============================================================
# PARSERS
# ============================================================

def parse_int_safe(val):
    if val is None:
        return 0
    try:
        return int(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return 0


def decode_content(content):
    """Decode bytes to string, handling UTF-8 BOM."""
    if not content:
        return ''
    text = content.decode('utf-8-sig')  # Handles BOM automatically
    return text


def parse_ioc_tab(content):
    """Parse FERC H/D/P format IOC TAB file.

    H = header row (skip)
    D = contract detail: shipper, rate schedule, contract dates, MDQ
    P = point detail: point ID, point name, point MDQ

    Tab-delimited, same structure as other FERC IOC files.
    """
    text = decode_content(content)
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


def parse_unsub_csv(content):
    """Parse Gasquest unsubscribed capacity CSV."""
    text = decode_content(content)
    result = []

    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []

    has_loc = any('Loc' in h for h in headers)
    has_unsub = any('Unsub' in h or 'Cap' in h for h in headers)

    if has_loc and has_unsub:
        loc_key = next((h for h in headers if h.strip() in ('Loc', 'Location')), headers[0])
        name_key = next((h for h in headers if 'Name' in h), None)
        unsub_key = next((h for h in headers if 'Unsub' in h or 'Avail' in h), None)
        purp_key = next((h for h in headers if 'Purp' in h or 'Type' in h or 'Desc' in h), None)

        for row in reader:
            loc_id = str(row.get(loc_key, '')).strip()
            if not loc_id or 'Row Count' in loc_id or 'Comment' in loc_id:
                continue
            unsub_val = parse_int_safe(row.get(unsub_key, 0)) if unsub_key else 0
            if unsub_val > 0:
                result.append({
                    'Loc': loc_id,
                    'Loc_Name': str(row.get(name_key, '')).strip() if name_key else '',
                    'Loc_Purp_Desc': str(row.get(purp_key, '')).strip() if purp_key else '',
                    'Unsubscribed_Capacity': unsub_val,
                })
        return result

    # Fallback: H/D/P format
    reader2 = csv.reader(io.StringIO(text))
    for row in reader2:
        if not row or row[0].strip() != 'D':
            continue
        if len(row) < 5:
            continue
        loc_id = row[1].strip()
        loc_name = row[2].strip() if len(row) > 2 else ''
        unsub_val = parse_int_safe(row[-1])
        if loc_id and unsub_val > 0:
            result.append({
                'Loc': loc_id,
                'Loc_Name': loc_name,
                'Loc_Purp_Desc': '',
                'Unsubscribed_Capacity': unsub_val,
            })

    return result


def parse_oac_csv(content):
    """Parse Gasquest OAC CSV. Returns dict indexed by Loc ID."""
    text = decode_content(content)
    reader = csv.DictReader(io.StringIO(text))
    oac = {}

    for row in reader:
        loc_id = str(row.get('Loc', row.get('Location', ''))).strip()
        if not loc_id or 'Row Count' in loc_id:
            continue

        design = 0
        scheduled = 0
        available = 0
        for k, v in row.items():
            kl = k.lower()
            if 'design' in kl and 'cap' in kl:
                design = parse_int_safe(v)
            elif 'sched' in kl and ('total' in kl or 'qty' in kl or 'quant' in kl):
                scheduled = parse_int_safe(v)
            elif 'avail' in kl and 'oper' in kl:
                available = parse_int_safe(v)

        if design > 0 or available > 0:
            oac[loc_id] = {
                'design': design,
                'scheduled': scheduled,
                'available': available,
            }
    return oac


def parse_locations_csv(content):
    """Parse Gasquest Locations CSV into dict indexed by Loc ID."""
    text = decode_content(content)
    reader = csv.DictReader(io.StringIO(text))
    locs = {}
    for row in reader:
        loc_id = str(row.get('Loc', row.get('Location', ''))).strip()
        if loc_id:
            locs[loc_id] = row
    return locs


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
    """Geocode a county via Census.gov geocoder."""
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
        county = str(loc.get('Loc Cnty', loc.get('County', ''))).strip()
        state = str(loc.get('Loc St Abbrev', loc.get('State', ''))).strip()
        flow = str(loc.get('Dir Flo', loc.get('Flow Dir', ''))).strip()
        loc_type = str(loc.get('Loc Type Ind', '')).strip()
        connected = str(loc.get('Up/Dn Name', loc.get('Interconnect Name', ''))).strip()[:50]
        loc_name = str(loc.get('Loc Name', loc.get('Location Name', ''))).strip()

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
    """Fetch and process all data for a single pipeline."""
    tsp_id = pl['tsp_id']
    short = pl['short']
    posting_types = pl['posting_types']

    print(f"\n--- {short} (tspId={tsp_id}) ---")

    # IOC
    print("  Fetching IOC...")
    ioc_content = fetch_posting_file(tsp_id, posting_types['ioc'], 'IOC', prefer_ext='.tab')
    ioc_data = parse_ioc_tab(ioc_content) if ioc_content else {
        'contracts': [], 'by_point': {}, 'total_mdq': 0, 'num_contracts': 0, 'num_shippers': 0
    }
    print(f"    IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    # Unsub
    print("  Fetching Unsub...")
    unsub_content = fetch_posting_file(tsp_id, posting_types['unsub'], 'Unsub', prefer_ext='.csv')
    unsub_data = parse_unsub_csv(unsub_content) if unsub_content else []

    # FTS-A unsub (Gulf South only)
    if 'unsub_ftsa' in posting_types:
        print("  Fetching Unsub FTS-A...")
        ftsa_content = fetch_posting_file(tsp_id, posting_types['unsub_ftsa'], 'Unsub FTS-A', prefer_ext='.csv')
        ftsa_data = parse_unsub_csv(ftsa_content) if ftsa_content else []
        # Merge FTS-A into unsub, avoiding duplicates by Loc
        existing_locs = {u['Loc'] for u in unsub_data}
        for u in ftsa_data:
            if u['Loc'] not in existing_locs:
                unsub_data.append(u)
                existing_locs.add(u['Loc'])
            else:
                # Add capacity to existing
                for existing in unsub_data:
                    if existing['Loc'] == u['Loc']:
                        existing['Unsubscribed_Capacity'] += u['Unsubscribed_Capacity']
                        break

    print(f"    Unsub: {len(unsub_data)} points")

    # OAC
    print("  Fetching OAC...")
    oac_content = fetch_posting_file(tsp_id, posting_types['oac'], 'OAC', prefer_ext='.csv')
    oac_data = parse_oac_csv(oac_content) if oac_content else {}
    print(f"    OAC: {len(oac_data)} points")

    # Locations
    print("  Fetching Locations...")
    loc_content = fetch_posting_file(tsp_id, posting_types['locations'], 'Locations', prefer_ext='.csv')
    loc_data = parse_locations_csv(loc_content) if loc_content else {}
    print(f"    Locations: {len(loc_data)} points")

    # Build points
    by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points(loc_data, by_point, oac_data, county_coords)

    # Geocode new counties
    if new_counties:
        print(f"  Geocoding {len(new_counties)} new counties...")
        geocoded = 0
        for key in new_counties:
            county, state = key.split('|')
            lat, lng = geocode_county(county, state)
            if lat and lng:
                county_coords[key] = {'lat': lat, 'lng': lng}
                geocoded += 1
            time.sleep(0.5)
        save_county_coords(county_coords)
        print(f"    Geocoded {geocoded}/{len(new_counties)}")

        # Re-apply coordinates
        for pt in points:
            county = pt.get('county', '').upper()
            state = pt.get('state', '')
            if county and state and 'lat' not in pt:
                key = f"{county}|{state}"
                if key in county_coords:
                    pt['lat'] = county_coords[key]['lat']
                    pt['lng'] = county_coords[key]['lng']
                    pt['loc_accuracy'] = 'county_centroid'

    geocoded_count = sum(1 for pt in points if 'lat' in pt)
    print(f"  {short}: {len(points)} pts ({geocoded_count} geocoded), {len(unsub_data)} unsub, {ioc_data['num_contracts']} IOC")

    entry = {
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
    }

    return {
        'entry': entry,
        'ioc_data': ioc_data,
        'unsub_data': unsub_data,
        'oac_data': oac_data,
        'loc_data': loc_data,
        'points': points,
        'geocoded_count': geocoded_count,
    }


# ============================================================
# MERGE AND TRACKER UPDATE
# ============================================================

def merge_into_gas_interconnects(results):
    """Merge pipeline entries into gas_interconnects.json."""
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
    """Update corridor_pipeline_tracker.json for Gasquest pipelines."""
    if not os.path.exists(TRACKER_FILE):
        return

    with open(TRACKER_FILE) as f:
        tracker = json.load(f)

    for r in results:
        pl = r['_pipeline']
        tracker_name = pl['tracker_name']
        ioc_data = r['ioc_data']
        unsub_data = r['unsub_data']
        oac_data = r['oac_data']
        loc_data = r['loc_data']
        points = r['points']
        geocoded_count = r['geocoded_count']

        for e in tracker.get('gas_pipelines', []):
            if e.get('pipeline_name') == tracker_name:
                e['ioc'] = {
                    'status': 'captured',
                    'access_method': 'weekly_auto',
                    'platform': 'gasquest_rest_api',
                    'url': 'https://www.gasquest.com/informational-posting',
                    'last_refreshed': TODAY,
                    'refresh_frequency_days': 7,
                    'cloud_accessible': True,
                    'contract_count': ioc_data['num_contracts'],
                    'shipper_count': ioc_data['num_shippers'],
                }
                if unsub_data:
                    e['unsub'] = {
                        'status': 'captured',
                        'access_method': 'weekly_auto',
                        'platform': 'gasquest_rest_api',
                        'last_refreshed': TODAY,
                        'refresh_frequency_days': 7,
                        'cloud_accessible': True,
                        'point_count': len(unsub_data),
                    }
                else:
                    e['unsub'] = {
                        'status': 'attempted',
                        'access_method': 'weekly_auto',
                        'platform': 'gasquest_rest_api',
                        'cloud_accessible': True,
                        'notes': 'No unsub data returned',
                    }
                if oac_data:
                    e['capacity'] = {
                        'status': 'captured',
                        'access_method': 'weekly_auto',
                        'platform': 'gasquest_rest_api',
                        'last_refreshed': TODAY,
                        'refresh_frequency_days': 7,
                        'cloud_accessible': True,
                        'point_count': len(oac_data),
                    }
                e['locations'] = {
                    'status': 'captured',
                    'access_method': 'weekly_auto',
                    'platform': 'gasquest_rest_api',
                    'cracked_points': len(loc_data),
                    'geocoded_points': geocoded_count,
                    'geocode_method': 'county_centroid',
                    'cloud_accessible': True,
                }
                print(f"  Tracker updated for {tracker_name}")
                break

    with open(TRACKER_FILE, 'w') as f:
        json.dump(tracker, f, indent=2)


# ============================================================
# MAIN
# ============================================================

def main():
    print(f"=== Gasquest Pipeline Refresh: {TODAY} ===")

    county_coords = load_county_coords()
    results = []

    for pl in PIPELINES:
        try:
            r = process_pipeline(pl, county_coords)
            r['_pipeline'] = pl
            results.append(r)
        except Exception as e:
            print(f"  ERROR processing {pl['short']}: {e}")
            import traceback
            traceback.print_exc()

    if not results:
        print("\nNo pipelines processed successfully!")
        return

    merge_into_gas_interconnects(results)
    update_tracker(results)

    print(f"\n=== Done — {len(results)} pipelines processed ===")


if __name__ == '__main__':
    main()
