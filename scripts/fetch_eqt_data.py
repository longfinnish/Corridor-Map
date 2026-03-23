"""
EQT/Equitrans Pipeline Data Fetcher
Salesforce Experience Cloud (LWR) portal at infopost.eqt.com.
No login needed — just a cookie consent cookie.

Pipelines:
  - Equitrans, L.P. (TSP 189569585, code EQU) — 64 IOC, 322 OAC pts
  - Mountain Valley Pipeline, LLC (TSP 062498393, code MVP) — 10 IOC, 36 OAC pts

Runs weekly via GitHub Actions (eqt-refresh.yml).
"""

import requests
import json
import os
import time
from datetime import datetime, timedelta
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
OUTPUT_FILE = os.path.join(DATA_DIR, 'gas_interconnects.json')
TRACKER_FILE = os.path.join(DATA_DIR, 'corridor_pipeline_tracker.json')
COUNTY_CACHE = os.path.join(DATA_DIR, 'gas_county_coords.json')
TODAY = datetime.now().strftime('%Y-%m-%d')

BASE_URL = 'https://infopost.eqt.com/CustomerPortal/webruntime/api/apex/execute'
API_PARAMS = '?language=en-US&asGuest=true&htmlEncode=false'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': '*/*',
    'Origin': 'https://infopost.eqt.com',
    'Referer': 'https://infopost.eqt.com/CustomerPortal/informational-postings',
    'Content-Type': 'application/json; charset=utf-8',
}
COOKIES = {'CookieConsentPolicy': '0:1'}
TIMEOUT = 60

# Apex class IDs
CLS_IOC_DATA = '@udd/01pHs00000bhQdQ'
CLS_IOC_CSV = '@udd/01pHs00000bhQdG'
CLS_TABLE = '@udd/01pHs00000bhQdd'

PIPELINES = [
    {
        'tsp': '189569585',
        'code': 'EQU',
        'name': 'Equitrans, L.P.',
        'short': 'Equitrans',
        'tracker_name': 'Equitrans',
    },
    {
        'tsp': '062498393',
        'code': 'MVP',
        'name': 'Mountain Valley Pipeline, LLC',
        'short': 'Mountain Valley',
        'tracker_name': 'Mountain Valley Pipeline',
    },
]


# ============================================================
# SALESFORCE APEX API
# ============================================================

def apex_call(classname, method, params):
    """Make a Salesforce Apex API call. Returns the returnValue."""
    body = {
        'namespace': '',
        'classname': classname,
        'method': method,
        'isContinuation': False,
        'params': params,
        'cacheable': False,
    }
    r = requests.post(
        BASE_URL + API_PARAMS,
        headers=HEADERS,
        cookies=COOKIES,
        json=body,
        timeout=TIMEOUT,
    )
    if r.status_code != 200:
        print(f"    Apex call {method} returned {r.status_code}")
        return None

    data = r.json()
    return data.get('returnValue')


def apex_call_json_string(classname, method, params):
    """Apex call where returnValue is a JSON string that needs parsing."""
    rv = apex_call(classname, method, params)
    if rv is None:
        return None
    if isinstance(rv, str):
        return json.loads(rv)
    return rv


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


def parse_float_safe(val):
    if val is None:
        return 0.0
    try:
        return float(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return 0.0


# ============================================================
# IOC
# ============================================================

def fetch_ioc(pipeline_code):
    """Fetch IOC via structured JSON endpoint."""
    print(f"  Fetching IOC...")
    data = apex_call_json_string(CLS_IOC_DATA, 'getIndexOfCustomersData', {
        'pipelineCode': pipeline_code,
    })
    if not data:
        print(f"    IOC: no data returned")
        return None
    return data


def parse_ioc(data):
    """Parse IOC detailWrappers into contracts, by_point aggregates, and totals."""
    cutoff = datetime.now() + timedelta(days=730)

    details = data.get('detailWrappers', [])
    if not details:
        return {
            'contracts': [], 'by_point': {}, 'total_mdq': 0,
            'num_contracts': 0, 'num_shippers': 0,
        }

    contracts = {}
    by_point = defaultdict(lambda: {'firm_mdq': 0, 'expiring_2yr': 0, 'num_contracts': 0, 'shippers': set()})
    all_shippers = set()
    total_mdq = 0

    for d in details:
        shipper = (d.get('shipperName') or '').strip()
        rate = (d.get('rateShedule') or d.get('rateSchedule') or '').strip()
        contract_id = (d.get('contractNumber') or '').strip()
        begin_date = (d.get('contractEffDate') or '').strip()
        end_date = (d.get('contractKExpDate') or '').strip()
        mdq = parse_int_safe(d.get('transMDQ'))

        if not shipper or mdq == 0:
            continue

        all_shippers.add(shipper)
        total_mdq += mdq

        is_firm = 'FT' in rate.upper() or 'FTS' in rate.upper() or 'FIRM' in rate.upper()
        is_expiring = False
        if end_date:
            try:
                # Try multiple date formats
                for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y'):
                    try:
                        ed = datetime.strptime(end_date.strip()[:10], fmt)
                        if ed <= cutoff:
                            is_expiring = True
                        break
                    except ValueError:
                        continue
            except Exception:
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

        # Process point assignments
        point_wrappers = d.get('pointWrappers') or []
        for pw in point_wrappers:
            pt_name = (pw.get('pointName') or '').strip()
            pt_mdq = parse_int_safe(pw.get('transpPtMDQ')) or mdq

            # Use point name as ID since EQT doesn't provide numeric IDs in IOC
            if pt_name:
                by_point[pt_name]['num_contracts'] += 1
                by_point[pt_name]['shippers'].add(shipper)
                if is_firm:
                    by_point[pt_name]['firm_mdq'] += pt_mdq
                if is_expiring:
                    by_point[pt_name]['expiring_2yr'] += pt_mdq

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


EMPTY_IOC = {'contracts': [], 'by_point': {}, 'total_mdq': 0, 'num_contracts': 0, 'num_shippers': 0}


# ============================================================
# OAC (three-step: metadata → latest date → data)
# ============================================================

def fetch_oac(tsp):
    """Fetch OAC data via three-step Salesforce API."""
    print(f"  Fetching OAC...")

    # Step 1: metadata
    meta_name = f'Operationally_Available_{tsp}'
    metadata = apex_call_json_string(CLS_TABLE, 'getMetadataInfo', {
        'metadataName': meta_name,
    })
    if not metadata:
        print(f"    OAC: no metadata for {meta_name}")
        return []

    # Step 2: latest date
    latest_date = apex_call(CLS_TABLE, 'getLatestDateForTable', {
        'prepopulatedFieldName': 'EFF_GAS_DAY_DATE__c',
        'linkedFieldName': 'EFF_GAS_DAY__c',
        'objectApiName': 'IPWS_OpAvailL__c',
        'tsp': tsp,
    })
    if not latest_date:
        print(f"    OAC: no latest date")
        return []
    print(f"    OAC date: {latest_date}")

    time.sleep(1)

    # Step 3: data
    data = apex_call(CLS_TABLE, 'getTableDataForDate', {
        'objectConfigJSON': json.dumps(metadata),
        'selectedDate': latest_date,
    })
    if not data or not isinstance(data, list):
        print(f"    OAC: no data returned")
        return []

    print(f"    OAC: {len(data)} rows")
    return data


def parse_oac(rows):
    """Parse OAC rows into dict indexed by LOC."""
    oac = {}
    for row in rows:
        loc_id = str(row.get('LOC__c', '')).strip()
        if not loc_id:
            continue

        design = parse_int_safe(row.get('DC__c'))
        opc = parse_int_safe(row.get('OPC__c'))
        scheduled = parse_int_safe(row.get('TSQ__c'))
        available = parse_int_safe(row.get('OAC__c'))
        loc_name = str(row.get('LOC_NAME__c', '')).strip()
        flow = str(row.get('FLOW_IND__c', '')).strip()
        loc_purp = str(row.get('LOC_PURP__c', '')).strip()

        if design > 0 or available > 0:
            oac[loc_id] = {
                'design': design,
                'opc': opc,
                'scheduled': scheduled,
                'available': available,
                'loc_name': loc_name,
                'flow': flow,
                'loc_purp': loc_purp,
            }
    return oac


# ============================================================
# UNSUB (three-step: metadata → latest date → data)
# ============================================================

def fetch_unsub(tsp):
    """Fetch unsub data via three-step Salesforce API."""
    print(f"  Fetching Unsub...")

    # Step 1: metadata — note: "Unsubscribe" not "Unsubscribed"
    meta_name = f'Unsubscribe_{tsp}'
    metadata = apex_call_json_string(CLS_TABLE, 'getMetadataInfo', {
        'metadataName': meta_name,
    })
    if not metadata:
        print(f"    Unsub: no metadata for {meta_name}")
        return []

    # Step 2: latest date
    latest_date = apex_call(CLS_TABLE, 'getLatestDateForTable', {
        'prepopulatedFieldName': 'EFF_GAS_DAY_DATE__c',
        'linkedFieldName': 'EFF_GAS_DAY__c',
        'objectApiName': 'IPWS_UNSUB__c',
        'tsp': tsp,
    })
    if not latest_date:
        print(f"    Unsub: no latest date")
        return []
    print(f"    Unsub date: {latest_date}")

    time.sleep(1)

    # Step 3: data
    data = apex_call(CLS_TABLE, 'getTableDataForDate', {
        'objectConfigJSON': json.dumps(metadata),
        'selectedDate': latest_date,
    })
    if not data or not isinstance(data, list):
        print(f"    Unsub: no data returned")
        return []

    print(f"    Unsub: {len(data)} rows")
    return data


def parse_unsub(rows):
    """Parse unsub rows into list."""
    result = []
    for row in rows:
        loc_id = str(row.get('LOC__c', '')).strip()
        if not loc_id:
            continue

        unsub_val = parse_int_safe(row.get('Unsub_Cap__c'))
        if unsub_val <= 0:
            continue

        result.append({
            'Loc': loc_id,
            'Loc_Name': str(row.get('LOC_NAME__c', '')).strip()[:50],
            'Loc_Purp_Desc': str(row.get('LOC_PURP__c', '')).strip(),
            'Unsubscribed_Capacity': unsub_val,
            'Design_Capacity': parse_int_safe(row.get('DESIGNCAP__c')),
        })
    return result


# ============================================================
# LOCATIONS (two-step: metadata → data, no date)
# ============================================================

def fetch_locations(tsp):
    """Fetch locations via two-step Salesforce API."""
    print(f"  Fetching Locations...")

    # Step 1: metadata
    meta_name = f'Locations_{tsp}'
    metadata = apex_call_json_string(CLS_TABLE, 'getMetadataInfo', {
        'metadataName': meta_name,
    })
    if not metadata:
        print(f"    Locations: no metadata for {meta_name}")
        return []

    # Step 2: data (getTableData, NOT getTableDataForDate)
    data = apex_call(CLS_TABLE, 'getTableData', {
        'objectConfigJSON': json.dumps(metadata),
    })
    if not data or not isinstance(data, list):
        print(f"    Locations: no data returned")
        return []

    print(f"    Locations: {len(data)} rows")
    return data


def parse_locations(rows):
    """Parse locations into dict indexed by meter number."""
    locs = {}
    for row in rows:
        meter_no = str(row.get('MeterNo__c', '')).strip()
        if not meter_no:
            continue
        locs[meter_no] = {
            'meter_no': meter_no,
            'name': str(row.get('MeterDesc__c', '')).strip(),
            'county': str(row.get('COUNTY__c', '')).strip(),
            'state': str(row.get('STATE__c', '')).strip(),
            'flow': str(row.get('FlowDir__c', '')).strip(),
            'loc_type': str(row.get('LocTypeInd__c', '')).strip(),
            'connected': str(row.get('UpDownName__c', '')).strip(),
            'zone': str(row.get('LocZone__c', '')).strip(),
        }
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

def build_points(oac_data, loc_data, ioc_by_point, county_coords):
    """Build points from OAC data, enriched with locations and IOC."""
    new_counties = []
    points = []
    point_ids_seen = set()

    # Build a lookup from loc name → loc info for IOC matching
    loc_by_name = {}
    for meter_no, loc in loc_data.items():
        name = loc.get('name', '').strip().upper()
        if name:
            loc_by_name[name] = loc

    for loc_id, oac in oac_data.items():
        loc_name = oac.get('loc_name', '')
        flow = oac.get('flow', '')

        ptype = 'delivery'
        if 'Receipt' in flow or flow == 'R':
            ptype = 'receipt'
        elif 'Both' in flow or flow == 'B':
            ptype = 'bidirectional'

        # Try to find location info by matching LOC ID or name
        loc_info = loc_data.get(loc_id, {})
        if not loc_info:
            # Try matching by name
            loc_info = loc_by_name.get(loc_name.upper(), {})

        county = loc_info.get('county', '')
        state = loc_info.get('state', '')
        connected = loc_info.get('connected', '')[:50]

        lat, lng = None, None
        if county and state:
            key = f"{county.upper()}|{state}"
            if key in county_coords:
                lat, lng = county_coords[key].get('lat'), county_coords[key].get('lng')
            else:
                new_counties.append(key)

        # IOC matching by point name
        ioc_info = ioc_by_point.get(loc_name, ioc_by_point.get(loc_id, {}))

        pt = {
            'id': loc_id,
            'name': loc_name[:50],
            'type': ptype,
            'county': county,
            'state': state,
            'design': oac.get('design', 0),
            'scheduled': oac.get('scheduled', 0),
            'available': oac.get('available', 0),
            'utilization': round(oac['scheduled'] / oac['design'] * 100) if oac.get('design', 0) > 0 else 0,
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
        point_ids_seen.add(loc_name)

    # Add IOC-only points not in OAC
    for pt_name, info in ioc_by_point.items():
        if pt_name not in point_ids_seen:
            points.append({
                'id': pt_name, 'name': pt_name[:50], 'type': 'other',
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
    tsp = pl['tsp']
    code = pl['code']
    short = pl['short']

    print(f"\n--- {short} (TSP {tsp}, code {code}) ---")

    # IOC
    try:
        ioc_raw = fetch_ioc(code)
        ioc_data = parse_ioc(ioc_raw) if ioc_raw else EMPTY_IOC.copy()
    except Exception as e:
        print(f"    IOC error: {e}")
        ioc_data = EMPTY_IOC.copy()
    print(f"    IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")
    time.sleep(2)

    # OAC
    try:
        oac_rows = fetch_oac(tsp)
        oac_data = parse_oac(oac_rows)
    except Exception as e:
        print(f"    OAC error: {e}")
        oac_data = {}
    print(f"    OAC: {len(oac_data)} points")
    time.sleep(2)

    # Unsub
    try:
        unsub_rows = fetch_unsub(tsp)
        unsub_data = parse_unsub(unsub_rows)
    except Exception as e:
        print(f"    Unsub error: {e}")
        unsub_data = []
    print(f"    Unsub: {len(unsub_data)} points")
    time.sleep(2)

    # Locations
    try:
        loc_rows = fetch_locations(tsp)
        loc_data = parse_locations(loc_rows)
    except Exception as e:
        print(f"    Locations error: {e}")
        loc_data = {}
    print(f"    Locations: {len(loc_data)} meters")

    # Build points
    ioc_by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points(oac_data, loc_data, ioc_by_point, county_coords)

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

        entry = None
        for e in pipelines_list:
            if e.get('pipeline_name') == tracker_name:
                entry = e
                break

        if not entry:
            entry = {
                'pipeline_name': tracker_name,
                'operator': 'EQT Corporation',
                'regulation': 'interstate',
                'hifld_points': 0,
            }
            pipelines_list.append(entry)
            print(f"  Added new tracker entry: {tracker_name}")

        entry['ioc'] = {
            'status': 'captured',
            'access_method': 'weekly_auto',
            'platform': 'eqt_salesforce',
            'url': 'https://infopost.eqt.com/CustomerPortal/informational-postings',
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
                'platform': 'eqt_salesforce',
                'url': 'https://infopost.eqt.com/CustomerPortal/informational-postings',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'point_count': len(r['unsub_data']),
            }
        else:
            entry['unsub'] = {
                'status': 'attempted',
                'access_method': 'weekly_auto',
                'platform': 'eqt_salesforce',
                'cloud_accessible': True,
            }
        if r['oac_data']:
            entry['capacity'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'eqt_salesforce',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'point_count': len(r['oac_data']),
            }
        if r['loc_data']:
            entry['locations'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'eqt_salesforce',
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
    print(f"=== EQT Pipeline Refresh: {TODAY} ===")

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
