"""
Northern Natural Gas (NNG) Data Fetcher
Downloads IOC, Locations, Unsub, and OAC via direct CSV endpoints.
No postback, no login — just ?download=true query parameter.

Runs weekly via GitHub Actions (nng-refresh.yml).
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

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
TIMEOUT = 90  # NNG endpoints can be slow

PIPELINE_NAME = 'Northern Natural Gas Company'
PIPELINE_SHORT = 'Northern Natural'
TRACKER_NAME = 'Northern Natural Gas (NNG)'


# ============================================================
# DATA FETCHERS
# ============================================================

def fetch_ioc():
    """Download IOC CSV via ?download=true. Returns H/D/P format CSV text."""
    url = 'https://www.northernnaturalgas.com/infopostings/Pages/IndexOfCustomers.aspx?download=true'
    r = requests.get(url, headers={'User-Agent': UA}, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"  IOC returned {r.status_code}")
        return None
    ct = r.headers.get('Content-Type', '')
    disp = r.headers.get('Content-Disposition', '')
    print(f"  IOC: {len(r.text):,} bytes, Content-Type: {ct}, Disposition: {disp}")
    return r.text


def fetch_locations():
    """Download Locations CSV."""
    url = 'https://www.northernnaturalgas.com/infopostings/Pages/Locations.aspx?download=true'
    r = requests.get(url, headers={'User-Agent': UA}, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"  Locations returned {r.status_code}")
        return None
    print(f"  Locations: {len(r.text):,} bytes")
    return r.text


def fetch_unsub():
    """Download Unsubscribed Capacity CSV (90s timeout — this endpoint is slow)."""
    url = 'https://www.northernnaturalgas.com/InfoPostings/Capacity/Pages/Unsubscribed.aspx?ac=Download&rpType=UCBP'
    r = requests.get(url, headers={'User-Agent': UA}, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"  Unsub returned {r.status_code}")
        return None
    print(f"  Unsub: {len(r.text):,} bytes")
    return r.text


def fetch_oac():
    """Download OAC CSV."""
    url = 'https://www.northernnaturalgas.com/InfoPostings/Capacity/Pages/OperationallyAvailable.aspx?retMode=rptDownload'
    r = requests.get(url, headers={'User-Agent': UA}, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"  OAC returned {r.status_code}")
        return None
    print(f"  OAC: {len(r.text):,} bytes")
    return r.text


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


def parse_ioc_csv(text):
    """Parse FERC H/D/P format IOC CSV.

    H = header row (skip)
    D = contract detail: shipper, rate schedule, contract dates, MDQ
    P = point detail: point ID, point name, point MDQ

    Returns dict with contracts list, per-point aggregates, and totals.
    """
    reader = csv.reader(io.StringIO(text))
    cutoff = datetime.now() + timedelta(days=730)

    contracts = {}  # contract_id -> dict
    by_point = defaultdict(lambda: {'firm_mdq': 0, 'expiring_2yr': 0, 'num_contracts': 0, 'shippers': set()})
    all_shippers = set()
    total_mdq = 0

    current_contract = None

    for row in reader:
        if not row:
            continue
        row_type = row[0].strip()

        if row_type == 'D':
            # D row: 0=type, 1=shipper, 2=DUNS, 3=affiliate, 4=rate_schedule,
            # 5=contract_id, 6=begin_date, 7=end_date, 8=amendment,
            # 9=nego_rate, 10=MDQ, 11=storage_qty, 12=footnote
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
            # P row: 0=type, 1=point_id, 2=point_name, 3=point_id_qualifier,
            # 4=point_id_2, 5=zone, 6=pt_mdq, 7=pt_msq
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

    # Convert sets to counts
    by_point_out = {}
    for loc_id, info in by_point.items():
        by_point_out[loc_id] = {
            'firm_mdq': info['firm_mdq'],
            'expiring_2yr': info['expiring_2yr'],
            'num_contracts': info['num_contracts'],
            'num_shippers': len(info['shippers']),
        }

    contract_list = list(contracts.values())
    # Clean internal fields
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


def parse_locations_csv(text):
    """Parse NNG Locations CSV into dict indexed by Loc ID."""
    reader = csv.DictReader(io.StringIO(text))
    locs = {}
    for row in reader:
        loc_id = str(row.get('Loc', '')).strip()
        if loc_id:
            locs[loc_id] = row
    return locs


def parse_unsub_csv(text):
    """Parse NNG Unsubscribed Capacity CSV.

    May use H/D row format or may be a flat CSV. Handle both.
    """
    result = []

    # Try as flat CSV first
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []

    # Check if it's a standard CSV with known headers
    has_loc = any('Loc' in h for h in headers)
    has_unsub = any('Unsub' in h for h in headers)

    if has_loc and has_unsub:
        loc_key = next((h for h in headers if h.strip() == 'Loc'), headers[0])
        name_key = next((h for h in headers if 'Name' in h), None)
        unsub_key = next((h for h in headers if 'Unsub' in h), None)
        purp_key = next((h for h in headers if 'Purp' in h or 'Type' in h), None)

        for row in reader:
            loc_id = str(row.get(loc_key, '')).strip()
            if not loc_id or 'Row Count' in loc_id or 'Comment' in loc_id:
                continue
            unsub_val = parse_int_safe(row.get(unsub_key, 0))
            if unsub_val > 0:
                result.append({
                    'Loc': loc_id,
                    'Loc_Name': str(row.get(name_key, '')).strip() if name_key else '',
                    'Loc_Purp_Desc': str(row.get(purp_key, '')).strip() if purp_key else '',
                    'Unsubscribed_Capacity': unsub_val,
                })
        return result

    # Fallback: try H/D/P format
    reader2 = csv.reader(io.StringIO(text))
    for row in reader2:
        if not row or row[0].strip() != 'D':
            continue
        if len(row) < 5:
            continue
        loc_id = row[1].strip()
        loc_name = row[2].strip() if len(row) > 2 else ''
        unsub_val = parse_int_safe(row[-1])  # Last column is usually unsub capacity
        if loc_id and unsub_val > 0:
            result.append({
                'Loc': loc_id,
                'Loc_Name': loc_name,
                'Loc_Purp_Desc': '',
                'Unsubscribed_Capacity': unsub_val,
            })

    return result


def parse_oac_csv(text):
    """Parse NNG OAC CSV. Returns dict indexed by Loc ID."""
    reader = csv.DictReader(io.StringIO(text))
    oac = {}
    for row in reader:
        loc_id = str(row.get('Loc', '')).strip()
        if not loc_id or 'Row Count' in loc_id:
            continue

        # Find capacity columns (names vary slightly)
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
# MAIN PROCESSING
# ============================================================

def main():
    print(f"=== NNG Pipeline Refresh: {TODAY} ===\n")

    # Fetch all endpoints
    print("Fetching IOC...")
    ioc_text = fetch_ioc()

    print("Fetching Locations...")
    loc_text = fetch_locations()

    print("Fetching Unsub...")
    try:
        unsub_text = fetch_unsub()
    except Exception as e:
        print(f"  Unsub failed: {e}")
        unsub_text = None

    print("Fetching OAC...")
    try:
        oac_text = fetch_oac()
    except Exception as e:
        print(f"  OAC failed: {e}")
        oac_text = None

    # Parse
    print("\nParsing...")

    ioc_data = parse_ioc_csv(ioc_text) if ioc_text else {'contracts': [], 'by_point': {}, 'total_mdq': 0, 'num_contracts': 0, 'num_shippers': 0}
    print(f"  IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ, {len(ioc_data['by_point'])} points")

    loc_data = parse_locations_csv(loc_text) if loc_text else {}
    print(f"  Locations: {len(loc_data)} points")

    unsub_data = parse_unsub_csv(unsub_text) if unsub_text else []
    print(f"  Unsub: {len(unsub_data)} points")

    oac_data = parse_oac_csv(oac_text) if oac_text else {}
    print(f"  OAC: {len(oac_data)} points")

    # Build points from locations
    county_coords = load_county_coords()
    by_point = ioc_data.get('by_point', {})
    new_counties = []
    points = []
    point_ids_seen = set()

    for loc_id, loc in loc_data.items():
        county = str(loc.get('Loc Cnty', '')).strip()
        state = str(loc.get('Loc St Abbrev', '')).strip()
        flow = str(loc.get('Dir Flo', '')).strip()
        loc_type = str(loc.get('Loc Type Ind', '')).strip()
        connected = str(loc.get('Up/Dn Name', '')).strip()[:50]
        loc_name = str(loc.get('Loc Name', '')).strip()
        zone = str(loc.get('Loc Zone', '')).strip()

        ptype = 'delivery'
        if flow == 'R':
            ptype = 'receipt'
        elif flow == 'B':
            ptype = 'bidirectional'

        # Geocode
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

    # Geocode new counties
    new_counties = list(set(new_counties))
    if new_counties:
        print(f"\nGeocoding {len(new_counties)} new counties...")
        geocoded = 0
        for key in new_counties:
            county, state = key.split('|')
            lat, lng = geocode_county(county, state)
            if lat and lng:
                county_coords[key] = {'lat': lat, 'lng': lng}
                geocoded += 1
            time.sleep(0.5)
        save_county_coords(county_coords)
        print(f"  Geocoded {geocoded}/{len(new_counties)}")

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

    # Build gas_interconnects entry
    entry = {
        'name': PIPELINE_NAME,
        'short': PIPELINE_SHORT,
        'updated': TODAY,
        'points': points,
        'unsub_points': unsub_data,
        'ioc_totals': {
            'firm_mdq': ioc_data['total_mdq'],
            'num_contracts': ioc_data['num_contracts'],
            'num_shippers': ioc_data['num_shippers'],
        },
    }

    print(f"\nNNG: {len(points)} pts, {len(unsub_data)} unsub, {ioc_data['num_contracts']} IOC, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    # Merge into gas_interconnects.json
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            gi = json.load(f)
    else:
        gi = {'pipelines': []}

    # Remove existing NNG entry
    gi['pipelines'] = [p for p in gi['pipelines'] if p.get('short') != PIPELINE_SHORT]
    gi['pipelines'].append(entry)

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(gi, f)
    print(f"gas_interconnects.json: {len(gi['pipelines'])} total pipelines")

    # Update tracker
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE) as f:
            tracker = json.load(f)

        for e in tracker.get('gas_pipelines', []):
            if e.get('pipeline_name') == TRACKER_NAME:
                # Update IOC
                e['ioc'] = {
                    'status': 'captured',
                    'access_method': 'weekly_auto',
                    'platform': 'nng_direct_csv',
                    'url': 'https://www.northernnaturalgas.com/infopostings/Pages/IndexOfCustomers.aspx?download=true',
                    'last_refreshed': TODAY,
                    'refresh_frequency_days': 7,
                    'contract_count': ioc_data['num_contracts'],
                    'shipper_count': ioc_data['num_shippers'],
                }
                # Update unsub
                if unsub_data:
                    e['unsub'] = {
                        'status': 'captured',
                        'access_method': 'weekly_auto',
                        'platform': 'nng_direct_csv',
                        'last_refreshed': TODAY,
                        'point_count': len(unsub_data),
                    }
                else:
                    e['unsub'] = {
                        'status': 'attempted',
                        'access_method': 'weekly_auto',
                        'platform': 'nng_direct_csv',
                        'notes': 'Endpoint timed out or returned empty',
                    }
                # Update OAC
                if oac_data:
                    e['capacity'] = {
                        'status': 'captured',
                        'access_method': 'weekly_auto',
                        'platform': 'nng_direct_csv',
                        'last_refreshed': TODAY,
                        'point_count': len(oac_data),
                    }
                # Update locations
                e['locations'] = {
                    'status': 'captured',
                    'access_method': 'weekly_auto',
                    'cracked_points': len(loc_data),
                    'geocoded_points': sum(1 for pt in points if 'lat' in pt),
                    'geocode_method': 'county_centroid',
                }
                print(f"Tracker updated for {TRACKER_NAME}")
                break

        with open(TRACKER_FILE, 'w') as f:
            json.dump(tracker, f, indent=2)

    print(f"\n=== Done ===")


if __name__ == '__main__':
    main()
