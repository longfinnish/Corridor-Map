"""
National Fuel Gas Supply Corporation Data Fetcher
PeopleSoft portal at sbsprd2.natfuel.com — all public, no login needed.

Data types: IOC (TSV), Locations (CSV), Unsub (HTML/Word), OAC (CSV)
PeopleSoft pattern: GET page → extract hidden fields → POST with ICAction → follow window.open URL

CRITICAL: PeopleSoft uses SINGLE QUOTES in HTML attributes.

Runs weekly via GitHub Actions (nfg-refresh.yml).
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

BASE_URL = 'https://sbsprd2.natfuel.com/psc/sbsprd/NFSBS/SBSPRD/c'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

# Page names and button names for each data type
PAGES = {
    'ioc': {
        'page': 'NFOM_INFORMATIONAL_POSTINGS.NFOC_IOC_INTRO',
        'button': 'NF_FILE_ATT_WRK_NF_DOWNLOAD_BTN',
    },
    'locations': {
        'page': 'NFOM_INFORMATIONAL_POSTINGS.NFOC_LOCATION_DATA',
        'button': 'NF_FILE_ATT_WRK_NF_CSV_DWN_BTN',
    },
    'unsub': {
        'page': 'NFOM_INFORMATIONAL_POSTINGS.NFOC_UNSUBSCRIBED',
        'button': 'NF_FILE_ATT_WRK_NF_SN_FILE_DWN_BTN$0',
    },
    'oac': {
        'page': 'NFOM_INFORMATIONAL_POSTINGS.NFOC_OPER_AVAIL_1',
        'button': 'NF_FILE_ATT_WRK_NF_CSV_DWN_BTN$0',
    },
}


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
    s.headers.update(HEADERS)
    return s


# ============================================================
# PEOPLESOFT DOWNLOAD HELPER
# ============================================================

def extract_hidden_fields(html):
    """Extract hidden form fields — PeopleSoft uses SINGLE QUOTES."""
    fields = {}
    for m in re.finditer(
        r"<input\s+type='hidden'\s+name='([^']*)'\s+id='[^']*'\s+value='([^']*)'\s*/>",
        html
    ):
        fields[m.group(1)] = m.group(2)
    return fields


def peoplesoft_download(session, page_name, button_name, timeout=90):
    """
    PeopleSoft form-postback download pattern:
    1. GET the page
    2. Extract hidden fields (single-quote regex)
    3. POST with ICAction = button name
    4. Extract window.open URL from response
    5. GET the download URL
    """
    url = f'{BASE_URL}/{page_name}.GBL'

    # Step 1: GET the page
    print(f"    GET {page_name}...")
    r = session.get(url, timeout=timeout)
    if r.status_code != 200:
        print(f"    GET returned {r.status_code}")
        return None, None

    # Step 2: Extract hidden fields
    fields = extract_hidden_fields(r.text)
    if not fields:
        print(f"    No hidden fields found (got {len(r.text):,} bytes)")
        return None, None
    print(f"    Found {len(fields)} hidden fields")

    # Step 3: POST with ICAction
    fields['ICAction'] = button_name
    print(f"    POST ICAction={button_name}...")
    r2 = session.post(url, data=fields, timeout=timeout)
    if r2.status_code != 200:
        print(f"    POST returned {r2.status_code}")
        return None, None

    # Step 4: Extract window.open URL
    js_opens = re.findall(r'window\.open\(["\']([^"\']+)', r2.text)
    if not js_opens:
        print(f"    No window.open URL found in POST response ({len(r2.text):,} bytes)")
        return None, None
    download_url = js_opens[0]
    # Make absolute if relative
    if download_url.startswith('/'):
        download_url = f'https://sbsprd2.natfuel.com{download_url}'
    print(f"    Download URL found")

    # Step 5: GET the file
    r3 = session.get(download_url, timeout=timeout)
    if r3.status_code != 200:
        print(f"    Download returned {r3.status_code}")
        return None, None

    content_type = r3.headers.get('Content-Type', '')
    print(f"    Downloaded {len(r3.content):,} bytes (Content-Type: {content_type})")
    return r3.content, content_type


# ============================================================
# IOC PARSER (FERC H/D/A/P tab-delimited)
# ============================================================

def parse_ioc_tsv(content):
    """Parse FERC H/D/A/P tab-delimited IOC file."""
    # Handle possible \r\r\n line endings
    text = content.decode('utf-8-sig')
    text = text.replace('\r\r\n', '\r\n')

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
                '_is_firm': 'FT' in rate.upper() or 'EFT' in rate.upper() or 'FIRM' in rate.upper(),
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
# LOCATIONS CSV PARSER
# ============================================================

def parse_locations_csv(content):
    """Parse National Fuel Gas Locations CSV."""
    text = content.decode('utf-8-sig')
    text = text.replace('\r\r\n', '\r\n')
    reader = csv.DictReader(io.StringIO(text))
    locs = {}
    for row in reader:
        loc_id = str(row.get('Loc', '')).strip()
        if not loc_id:
            continue
        locs[loc_id] = {
            'Loc': loc_id,
            'Loc Name': str(row.get('Location Name', row.get('Loc Name', ''))).strip(),
            'Loc Type Ind': str(row.get('Loc Type Ind', '')).strip(),
            'Dir Flo': str(row.get('Dir Flo', '')).strip(),
            'Loc Zone': str(row.get('Loc Zone', '')).strip(),
            'Loc Cnty': str(row.get('Loc Cnty', '')).strip(),
            'Loc St Abbrev': str(row.get('Loc St Abbrev', '')).strip(),
            'Up/Dn Name': str(row.get('Up/Dn Name', '')).strip(),
        }
    return locs


# ============================================================
# UNSUB HTML PARSER
# ============================================================

def parse_unsub_html(content):
    """Parse unsubscribed capacity from HTML/Word format file.
    Extract point names and capacity values from <td> cells."""
    text = content.decode('utf-8-sig', errors='replace')

    unsub_points = []

    # Find table rows — look for patterns with point name and capacity data
    # NFG unsub HTML uses <tr> rows with <td> cells
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', text, re.DOTALL | re.IGNORECASE)

    for row_html in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.IGNORECASE)
        if len(cells) < 3:
            continue

        # Clean cell content (strip HTML tags and whitespace)
        cleaned = []
        for cell in cells:
            val = re.sub(r'<[^>]+>', '', cell).strip()
            val = val.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
            cleaned.append(val)

        # Skip header rows and empty rows
        if not cleaned[0] or cleaned[0].upper() in ('LOCATION', 'LOC', 'POINT', 'NAME', ''):
            continue

        # Try to find a numeric capacity value in the cells
        point_name = ''
        design_cap = 0
        unsub_cap = 0

        for i, val in enumerate(cleaned):
            num = parse_int_safe(val)
            if num > 0 and not point_name:
                # Previous non-empty cell is likely the point name
                for j in range(i - 1, -1, -1):
                    if cleaned[j] and not cleaned[j].replace(',', '').replace('.', '').isdigit():
                        point_name = cleaned[j][:50]
                        break
                design_cap = num
            elif num > 0 and point_name and design_cap > 0:
                unsub_cap = num
                break

        if point_name and (design_cap > 0 or unsub_cap > 0):
            unsub_points.append({
                'name': point_name,
                'design': design_cap,
                'unsubscribed': unsub_cap,
            })

    return unsub_points


# ============================================================
# OAC CSV PARSER
# ============================================================

def parse_oac_csv(content):
    """Parse OAC CSV — extract point-level DC/OPC/TSQ/OAC values."""
    text = content.decode('utf-8-sig', errors='replace')
    text = text.replace('\r\r\n', '\r\n')

    # Check if actually CSV
    if '<html' in text[:500].lower():
        print("    OAC: got HTML instead of CSV")
        return {}

    reader = csv.DictReader(io.StringIO(text))
    oac_by_point = {}

    for row in reader:
        loc_id = str(row.get('Loc', row.get('LOC', ''))).strip()
        if not loc_id:
            continue

        oac_by_point[loc_id] = {
            'design': parse_int_safe(row.get('Design Capacity', row.get('DC', 0))),
            'opc': parse_int_safe(row.get('Operating Capacity', row.get('OPC', 0))),
            'tsq': parse_int_safe(row.get('Total Sched Qty', row.get('TSQ', 0))),
            'oac': parse_int_safe(row.get('OAC', row.get('Oper Avail Cap', 0))),
        }

    return oac_by_point


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
    """Build points from Locations CSV, enriched with IOC and OAC data."""
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
        oac_info = oac_data.get(loc_id, {})

        pt = {
            'id': loc_id,
            'name': loc_name[:50],
            'type': ptype,
            'county': county,
            'state': state,
            'design': oac_info.get('design', 0),
            'scheduled': oac_info.get('tsq', 0),
            'available': oac_info.get('oac', 0),
            'utilization': 0,
            'connected': connected,
            'firm_contracted': ioc_info.get('firm_mdq', 0),
            'num_contracts': ioc_info.get('num_contracts', 0),
            'num_shippers': ioc_info.get('num_shippers', 0),
            'expiring_2yr': ioc_info.get('expiring_2yr', 0),
        }

        # Compute utilization
        if pt['design'] > 0 and pt['scheduled'] > 0:
            pt['utilization'] = round(pt['scheduled'] / pt['design'] * 100, 1)

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
# MAIN FETCH + PROCESS
# ============================================================

def main():
    print(f"=== National Fuel Gas Refresh: {TODAY} ===")

    session = make_session()
    county_coords = load_county_coords()

    # --- IOC ---
    print("\n--- IOC ---")
    ioc_data = EMPTY_IOC.copy()
    try:
        content, ctype = peoplesoft_download(
            session, PAGES['ioc']['page'], PAGES['ioc']['button'], timeout=90
        )
        if content:
            ioc_data = parse_ioc_tsv(content)
    except Exception as e:
        print(f"  IOC error: {e}")
    print(f"  IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    time.sleep(3)

    # --- Locations ---
    print("\n--- Locations ---")
    loc_data = {}
    try:
        content, ctype = peoplesoft_download(
            session, PAGES['locations']['page'], PAGES['locations']['button'], timeout=90
        )
        if content:
            loc_data = parse_locations_csv(content)
    except Exception as e:
        print(f"  Locations error: {e}")
    print(f"  Locations: {len(loc_data)} points")

    time.sleep(3)

    # --- Unsub ---
    print("\n--- Unsub ---")
    unsub_points = []
    try:
        content, ctype = peoplesoft_download(
            session, PAGES['unsub']['page'], PAGES['unsub']['button'], timeout=90
        )
        if content:
            unsub_points = parse_unsub_html(content)
    except Exception as e:
        print(f"  Unsub error: {e}")
    print(f"  Unsub: {len(unsub_points)} points")

    time.sleep(3)

    # --- OAC ---
    print("\n--- OAC ---")
    oac_data = {}
    try:
        content, ctype = peoplesoft_download(
            session, PAGES['oac']['page'], PAGES['oac']['button'], timeout=90
        )
        if content:
            oac_data = parse_oac_csv(content)
            if not oac_data:
                # Try button $1 if $0 didn't work
                print("  Trying OAC button $1...")
                time.sleep(2)
                content2, ctype2 = peoplesoft_download(
                    session, PAGES['oac']['page'], 'NF_FILE_ATT_WRK_NF_CSV_DWN_BTN$1', timeout=90
                )
                if content2:
                    oac_data = parse_oac_csv(content2)
    except Exception as e:
        print(f"  OAC error: {e}")
    print(f"  OAC: {len(oac_data)} points")

    # Build points
    by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points(loc_data, by_point, oac_data, county_coords)

    # Geocode new counties
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

        # Re-apply geocoded coords to points
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

    print(f"\n--- Summary ---")
    print(f"  National Fuel Gas: {len(points)} pts ({geocoded_count} geocoded)")
    print(f"  IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers")
    print(f"  Unsub: {len(unsub_points)} points")
    print(f"  OAC: {len(oac_data)} points")

    # Build gas_interconnects entry
    entry = {
        'name': 'National Fuel Gas Supply Corporation',
        'short': 'National Fuel Gas',
        'updated': TODAY,
        'points': points,
        'unsub_points': unsub_points,
        'ioc_totals': {
            'firm_mdq': ioc_data['total_mdq'],
            'num_contracts': ioc_data['num_contracts'],
            'num_shippers': ioc_data['num_shippers'],
        },
    }

    # Merge into gas_interconnects.json
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            gi = json.load(f)
    else:
        gi = {'pipelines': []}

    gi['pipelines'] = [p for p in gi['pipelines'] if p.get('short') != 'National Fuel Gas']
    gi['pipelines'].append(entry)

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(gi, f)
    print(f"\ngas_interconnects.json: {len(gi['pipelines'])} total pipelines")

    # Update tracker
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE) as f:
            tracker = json.load(f)

        pipelines_list = tracker.get('gas_pipelines', [])

        tracker_entry = None
        for e in pipelines_list:
            if e.get('pipeline_name') == 'National Fuel Gas Supply':
                tracker_entry = e
                break

        if not tracker_entry:
            tracker_entry = {
                'pipeline_name': 'National Fuel Gas Supply',
                'hifld_company': 'NATIONAL FUEL GAS SUPPLY CORPORATION',
                'operator': 'National Fuel Gas',
                'regulation': 'interstate',
                'hifld_points': 47,
            }
            pipelines_list.append(tracker_entry)
            print("  Added new tracker entry: National Fuel Gas Supply")

        # IOC
        if ioc_data['num_contracts'] > 0:
            tracker_entry['ioc'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'nfg_peoplesoft',
                'url': f'{BASE_URL}/{PAGES["ioc"]["page"]}.GBL',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'contract_count': ioc_data['num_contracts'],
                'shipper_count': ioc_data['num_shippers'],
            }
        else:
            tracker_entry['ioc'] = {
                'status': 'attempted',
                'access_method': 'weekly_auto',
                'platform': 'nfg_peoplesoft',
                'url': f'{BASE_URL}/{PAGES["ioc"]["page"]}.GBL',
                'cloud_accessible': True,
                'notes': 'PeopleSoft download attempted but no IOC data parsed',
            }

        # Unsub
        if unsub_points:
            tracker_entry['unsub'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'nfg_peoplesoft',
                'url': f'{BASE_URL}/{PAGES["unsub"]["page"]}.GBL',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
            }
        else:
            tracker_entry['unsub'] = {
                'status': 'attempted',
                'access_method': 'weekly_auto',
                'platform': 'nfg_peoplesoft',
                'url': f'{BASE_URL}/{PAGES["unsub"]["page"]}.GBL',
                'cloud_accessible': True,
                'notes': 'HTML/Word format — parsing may have missed data',
            }

        # OAC
        if oac_data:
            tracker_entry['capacity'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'nfg_peoplesoft',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
            }

        # Locations
        if loc_data:
            tracker_entry['locations'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'nfg_peoplesoft',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'cracked_points': len(loc_data),
                'geocoded_points': geocoded_count,
                'geocode_method': 'county_centroid',
            }

        tracker_entry['notes'] = f'PeopleSoft portal. {ioc_data["num_contracts"]} IOC, {len(loc_data)} locations, {len(unsub_points)} unsub pts.'

        print(f"  Tracker updated: National Fuel Gas Supply")

        with open(TRACKER_FILE, 'w') as f:
            json.dump(tracker, f, indent=2)

    print(f"\n=== Done ===")


if __name__ == '__main__':
    main()
