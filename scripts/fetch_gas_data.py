"""
Gas Interconnect Data Fetcher
Pulls EBB capacity and IOC contract data from pipeline operator portals.
Runs daily via GitHub Actions to build 30-day rolling averages.

Platforms:
  - Kinder Morgan pipeline2 (El Paso, Tennessee Gas, NGPL, MEP, Southern Natural)
  - Williams 1line (Transco)
  - Enbridge rtba (Texas Eastern)
"""

import requests
import re
import csv
import json
import os
import io
import sys
import time
import openpyxl
from datetime import datetime, timedelta
from collections import defaultdict

GEOCODIO_KEY = os.environ.get('GEOCODIO_KEY', '')
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
HISTORY_FILE = os.path.join(DATA_DIR, 'gas_history.json')
COUNTY_CACHE = os.path.join(DATA_DIR, 'gas_county_coords.json')
HIFLD_CACHE = os.path.join(DATA_DIR, 'gas_hifld_points.json')
OUTPUT_FILE = os.path.join(DATA_DIR, 'gas_interconnects.json')

TODAY = datetime.now().strftime('%Y-%m-%d')
TODAY_MMDDYYYY = datetime.now().strftime('%m/%d/%Y')

# ============================================================
# KINDER MORGAN (ASP.NET ViewState POST)
# ============================================================

KM_PIPELINES = [
    {'code': 'EPNG', 'name': 'El Paso Natural Gas Company', 'short': 'El Paso'},
    {'code': 'TGP', 'name': 'Tennessee Gas Pipeline Company', 'short': 'Tennessee Gas'},
    {'code': 'NGPL', 'name': 'Natural Gas Pipeline Company of America', 'short': 'NGPL'},
    {'code': 'MEP', 'name': 'Midcontinent Express Pipeline', 'short': 'Midcontinent Express'},
    {'code': 'SNG', 'name': 'Southern Natural Gas Company, LLC', 'short': 'Southern Natural'},
    {'code': 'FGT', 'name': 'Florida Gas Transmission Company, LLC', 'short': 'Florida Gas'},
    {'code': 'CIG', 'name': 'Colorado Interstate Gas Company, LLC', 'short': 'Colorado Interstate'},
]

def fetch_km_capacity(code):
    """Fetch Operationally Available Capacity from KM portal."""
    s = requests.Session()
    s.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    
    url = f'https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code={code}'
    r = s.get(url)
    
    hidden = extract_hidden_fields(r.text)
    form_data = dict(hidden)
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL'] = 'EXCEL'
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.x'] = '10'
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.y'] = '10'
    
    r2 = s.post(url, data=form_data)
    
    if 'excel' in r2.headers.get('Content-Type', '').lower() or r2.content[:2] == b'PK':
        return parse_km_xlsx(r2.content)
    return []


def fetch_km_locations(code):
    """Fetch Location Data Download from KM portal."""
    s = requests.Session()
    s.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    
    url = f'https://pipeline2.kindermorgan.com/LocationDataDownload/LocDataDwnld.aspx?code={code}'
    r = s.get(url)
    
    hidden = extract_hidden_fields(r.text)
    form_data = dict(hidden)
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL'] = 'CSV'
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.x'] = '10'
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.y'] = '10'
    
    r2 = s.post(url, data=form_data)
    
    if 'text' in r2.headers.get('Content-Type', ''):
        return {str(r['Loc']).strip(): r for r in csv.DictReader(io.StringIO(r2.text))}
    return {}


def fetch_km_ioc(code):
    """Fetch Index of Customers from KM portal."""
    s = requests.Session()
    s.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    
    url = f'https://pipeline2.kindermorgan.com/IndexOfCust/IOC.aspx?code={code}'
    r = s.get(url)
    
    hidden = extract_hidden_fields(r.text)
    form_data = dict(hidden)
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL'] = 'EXCEL'
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.x'] = '10'
    form_data['ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.y'] = '10'
    
    r2 = s.post(url, data=form_data)
    
    if 'excel' in r2.headers.get('Content-Type', '').lower() or 'octet' in r2.headers.get('Content-Type', ''):
        return parse_ioc_xlsx(r2.content)
    return {}


# ============================================================
# WILLIAMS (JSP Form POST + HTML Parse)
# ============================================================

WILLIAMS_PIPELINES = [
    {'buid': 80, 'name': 'Transcontinental Gas Pipe Line Company (Transco)', 'short': 'Transco'},
]

def fetch_williams_capacity(buid):
    """Fetch OAC from Williams 1line JSP portal."""
    s = requests.Session()
    s.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    
    s.get(f'https://www.1line.williams.com/ebbCode/OACQueryRequest.jsp?BUID={buid}')
    s.post(f'https://www.1line.williams.com/ebbCode/OACQueryRequest.jsp?BUID={buid}', data={
        'tbGasFlowBeginDate': TODAY_MMDDYYYY,
        'tbGasFlowEndDate': TODAY_MMDDYYYY,
        'cycle': '2',
        'locationIDs': '',
        'reportType': 'OAC',
        'submitflag': 'true',
        'MapID': '0',
    })
    
    r = s.get('https://www.1line.williams.com/ebbCode/OACreport.jsp')
    return parse_williams_html(r.text)


# ============================================================
# ENBRIDGE (ASP.NET ViewState POST → CSV)
# ============================================================

ENBRIDGE_PIPELINES = [
    {'bu': 'TE', 'name': 'Texas Eastern Transmission, LP', 'short': 'Texas Eastern'},
    {'bu': 'AG', 'name': 'Algonquin Gas Transmission, LLC', 'short': 'Algonquin'},
]

def fetch_enbridge_capacity(bu_code):
    """Fetch OAC CSV from Enbridge rtba portal."""
    s = requests.Session()
    s.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    
    r = s.get(f'https://rtba.enbridge.com/InformationalPosting/Default.aspx?Type=OA&bu={bu_code}')
    
    hidden = extract_hidden_fields(r.text)
    cycles = re.findall(r'<option[^>]*value="([^"]*)"', r.text)
    if not cycles:
        return []
    
    form_data = dict(hidden)
    form_data[f'ctl00$MainContent$ctl01$oaDefault$ddlSelector'] = cycles[0]
    form_data['__EVENTTARGET'] = 'ctl00$MainContent$ctl01$oaDefault$hlDown$LinkButton1'
    form_data['__EVENTARGUMENT'] = ''
    
    r2 = s.post(f'https://rtba.enbridge.com/InformationalPosting/Default.aspx?Type=OA&bu={bu_code}', data=form_data)
    
    if 'text/plain' in r2.headers.get('Content-Type', ''):
        return list(csv.DictReader(io.StringIO(r2.text)))
    return []


# ============================================================
# PARSING HELPERS
# ============================================================

def extract_hidden_fields(html):
    """Extract ASP.NET hidden form fields."""
    fields = {}
    for m in re.finditer(r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"', html):
        fields[m.group(1)] = m.group(2)
    return fields


def parse_km_xlsx(content):
    """Parse KM multi-row header XLSX capacity file."""
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    
    header_idx = None
    for i, row in enumerate(rows):
        if row and str(row[0]).strip() == 'Loc':
            header_idx = i
            break
    
    if header_idx is None:
        return []
    
    headers = [str(h).strip() if h else f'col{j}' for j, h in enumerate(rows[header_idx])]
    data = []
    for row in rows[header_idx + 1:]:
        if row and row[0] is not None:
            data.append(dict(zip(headers, row)))
    return data


def parse_williams_html(html):
    """Parse Williams OAC HTML report into structured data."""
    rows = re.findall(r'<TR[^>]*>(.*?)</TR>', html, re.DOTALL | re.I)
    data = []
    for row in rows:
        cells = re.findall(r'<TD[^>]*>(.*?)</TD>', row, re.DOTALL | re.I)
        clean = [re.sub(r'<[^>]+>', '', c).strip().replace('&nbsp;', '').replace('\n', ' ').strip() for c in cells]
        if len(clean) >= 10 and any(c.replace(',', '').replace('-', '').isdigit() for c in clean[6:10]):
            data.append({
                'Loc': clean[0],
                'Loc Purp Desc': clean[1],
                'Flow Ind': clean[2],
                'QTI': clean[3],
                'Loc Name': clean[4],
                'Loc Zn': clean[5],
                'Design Capacity': clean[6],
                'Operating Capacity': clean[7],
                'Total Scheduled Quantity': clean[8],
                'Operationally Available Capacity': clean[9],
            })
    return data


def parse_ioc_xlsx(content):
    """Parse KM IOC Excel into per-point contract aggregates."""
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    
    header_idx = None
    for i, row in enumerate(rows):
        if row and any('Shipper' in str(c) for c in row if c):
            header_idx = i
            break
    if header_idx is None:
        return {}
    
    headers = [str(h).strip() if h else f'col{j}' for j, h in enumerate(rows[header_idx])]
    data = [dict(zip(headers, row)) for row in rows[header_idx + 1:] if row and row[0] is not None]
    
    loc_caps = defaultdict(lambda: {'firm_mdq': 0, 'expiring_2yr': 0, 'num_contracts': 0, 'shippers': set()})
    
    cutoff = datetime.now() + timedelta(days=730)
    
    for d in data:
        point_id = str(d.get('Point ID', '')).strip()
        mdq = d.get('MDQ') or d.get('PT MDQ') or 0
        rate = str(d.get('Rate Sched', '')).strip()
        shipper = str(d.get('Shipper Name', '')).strip()
        exp_date = d.get('Contract Expiration Date', '')
        
        try:
            mdq = int(str(mdq).replace(',', '').strip())
        except:
            mdq = 0
        
        if not point_id or mdq == 0:
            continue
        
        loc_caps[point_id]['num_contracts'] += 1
        loc_caps[point_id]['shippers'].add(shipper)
        
        if 'FT' in rate.upper():
            loc_caps[point_id]['firm_mdq'] += mdq
        
        if exp_date:
            try:
                if isinstance(exp_date, datetime):
                    ed = exp_date
                else:
                    ed = datetime.strptime(str(exp_date).strip()[:10], '%m/%d/%Y')
                if ed <= cutoff:
                    loc_caps[point_id]['expiring_2yr'] += mdq
            except:
                pass
    
    result = {}
    for loc_id, info in loc_caps.items():
        result[loc_id] = {
            'firm_mdq': info['firm_mdq'],
            'expiring_2yr': info['expiring_2yr'],
            'num_contracts': info['num_contracts'],
            'num_shippers': len(info['shippers']),
        }
    return result


def parse_int_safe(val):
    """Parse integer from possibly comma-formatted string."""
    if val is None:
        return 0
    s = str(val).replace(',', '').strip()
    try:
        return int(float(s))
    except:
        return 0


# ============================================================
# MAIN PIPELINE
# ============================================================

def fetch_all_capacity():
    """Fetch capacity data from all platforms. Returns list of pipeline dicts."""
    pipelines = []
    
    # Kinder Morgan
    for pl in KM_PIPELINES:
        print(f"Fetching KM {pl['short']}...")
        try:
            caps = fetch_km_capacity(pl['code'])
            locs = fetch_km_locations(pl['code'])
            ioc = fetch_km_ioc(pl['code'])
            
            points = []
            for c in caps:
                loc_id = str(c.get('Loc', '')).strip()
                if not loc_id:
                    continue
                loc = locs.get(loc_id, {})
                purp = str(c.get('Loc Purp Desc', '')).strip()
                
                dc = parse_int_safe(c.get('Design Capacity'))
                sched = parse_int_safe(c.get('Total Scheduled Quantity'))
                avail_key = next((k for k in c.keys() if 'Operationally' in str(k)), None)
                avail = parse_int_safe(c.get(avail_key)) if avail_key else 0
                
                if dc == 0:
                    continue
                
                ptype = 'delivery' if 'Delivery' in purp else ('receipt' if 'Receipt' in purp else 'other')
                
                pt = {
                    'id': loc_id,
                    'name': str(c.get('Loc Name', '')).strip()[:50],
                    'type': ptype,
                    'county': str(loc.get('Loc Cnty', '')).strip(),
                    'state': str(loc.get('Loc St Abbrev', '')).strip(),
                    'design': dc,
                    'scheduled': sched,
                    'available': avail,
                    'utilization': round(sched / dc * 100) if dc > 0 else 0,
                    'connected': str(loc.get('Up/Dn Name', '')).strip()[:50] if loc.get('Up/Dn Name') else '',
                }
                
                # Add IOC data if available
                if loc_id in ioc:
                    pt['firm_contracted'] = ioc[loc_id]['firm_mdq']
                    pt['expiring_2yr'] = ioc[loc_id]['expiring_2yr']
                    pt['num_shippers'] = ioc[loc_id]['num_shippers']
                    pt['num_contracts'] = ioc[loc_id]['num_contracts']
                
                points.append(pt)
            
            pipelines.append({
                'name': pl['name'],
                'short': pl['short'],
                'updated': TODAY,
                'points': points,
            })
            print(f"  {pl['short']}: {len(points)} points, {len(ioc)} IOC locations")
        except Exception as e:
            print(f"  ERROR {pl['short']}: {e}")
        time.sleep(2)
    
    # Williams
    for pl in WILLIAMS_PIPELINES:
        print(f"Fetching Williams {pl['short']}...")
        try:
            caps = fetch_williams_capacity(pl['buid'])
            
            points = []
            for c in caps:
                if 'Segment' in c.get('Loc Purp Desc', ''):
                    continue
                
                dc = parse_int_safe(c.get('Design Capacity'))
                sched = parse_int_safe(c.get('Total Scheduled Quantity'))
                avail = parse_int_safe(c.get('Operationally Available Capacity'))
                
                if dc == 0:
                    continue
                
                flow = c.get('Flow Ind', '')
                ptype = 'delivery' if 'D' in flow else ('receipt' if 'R' in flow else 'other')
                
                points.append({
                    'id': c.get('Loc', ''),
                    'name': c.get('Loc Name', '').strip()[:50],
                    'type': ptype,
                    'county': '',
                    'state': '',
                    'zone': c.get('Loc Zn', ''),
                    'design': dc,
                    'scheduled': sched,
                    'available': avail,
                    'utilization': round(sched / dc * 100) if dc > 0 else 0,
                    'connected': '',
                })
            
            pipelines.append({
                'name': pl['name'],
                'short': pl['short'],
                'updated': TODAY,
                'points': points,
            })
            print(f"  {pl['short']}: {len(points)} points")
        except Exception as e:
            print(f"  ERROR {pl['short']}: {e}")
        time.sleep(2)
    
    # Enbridge
    for pl in ENBRIDGE_PIPELINES:
        print(f"Fetching Enbridge {pl['short']}...")
        try:
            caps = fetch_enbridge_capacity(pl['bu'])
            
            points = []
            for c in caps:
                if 'Segment' in c.get('Loc_Purp_Desc', ''):
                    continue
                
                dc = parse_int_safe(c.get('Total_Design_Capacity'))
                sched = parse_int_safe(c.get('Total_Scheduled_Quantity'))
                avail = parse_int_safe(c.get('Operationally_Available_Capacity'))
                
                if dc == 0:
                    continue
                
                flow = c.get('Flow_Ind_Desc', '')
                ptype = 'delivery' if 'Delivery' in flow else ('receipt' if 'Receipt' in flow else 'other')
                
                points.append({
                    'id': c.get('Loc', ''),
                    'name': c.get('Loc_Name', '').strip()[:50],
                    'type': ptype,
                    'county': '',
                    'state': '',
                    'zone': c.get('Loc_Zn', ''),
                    'design': dc,
                    'scheduled': sched,
                    'available': avail,
                    'utilization': round(sched / dc * 100) if dc > 0 else 0,
                    'connected': '',
                })
            
            pipelines.append({
                'name': pl['name'],
                'short': pl['short'],
                'updated': TODAY,
                'points': points,
            })
            print(f"  {pl['short']}: {len(points)} points")
        except Exception as e:
            print(f"  ERROR {pl['short']}: {e}")
        time.sleep(2)
    
    return pipelines


def update_history(pipelines):
    """Append today's utilization snapshot to history file for rolling averages."""
    history = {}
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE) as f:
            history = json.load(f)
    
    for pl in pipelines:
        for pt in pl['points']:
            key = f"{pl['short']}|{pt['id']}"
            if key not in history:
                history[key] = {'snapshots': []}
            
            history[key]['snapshots'].append({
                'date': TODAY,
                'scheduled': pt['scheduled'],
                'available': pt['available'],
                'utilization': pt['utilization'],
            })
            
            # Keep only last 45 days
            cutoff = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
            history[key]['snapshots'] = [
                s for s in history[key]['snapshots'] if s['date'] >= cutoff
            ]
    
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f)
    
    print(f"History updated: {len(history)} tracked points")
    return history


def compute_rolling_stats(pipelines, history):
    """Add 30-day rolling average and peak to each point."""
    for pl in pipelines:
        for pt in pl['points']:
            key = f"{pl['short']}|{pt['id']}"
            snaps = history.get(key, {}).get('snapshots', [])
            
            # Filter to last 30 days
            cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            recent = [s for s in snaps if s['date'] >= cutoff]
            
            if len(recent) >= 2:
                utils = [s['utilization'] for s in recent]
                scheds = [s['scheduled'] for s in recent]
                pt['avg_utilization_30d'] = round(sum(utils) / len(utils), 1)
                pt['peak_utilization_30d'] = max(utils)
                pt['avg_scheduled_30d'] = round(sum(scheds) / len(scheds))
                pt['peak_scheduled_30d'] = max(scheds)
                pt['days_of_data'] = len(recent)


# ============================================================
# ENTRY POINT
# ============================================================

# ============================================================
# GEOCODING + HIFLD MATCHING
# ============================================================

PIPELINE_HIFLD_MAP = {
    'El Paso Natural Gas Company': ['EL PASO NATURAL GAS COMPANY'],
    'Tennessee Gas Pipeline Company': ['TENNESSEE GAS PIPELINE'],
    'Natural Gas Pipeline Company of America': ['NATURAL GAS PIPELINE (KINDER MORGAN)'],
    'Midcontinent Express Pipeline': ['MIDCONTINENT EXPRESS PIPELINE'],
    'Southern Natural Gas Company, LLC': ['SOUTHERN NATURAL GAS COMPANY LLC'],
    'Transcontinental Gas Pipe Line Company (Transco)': ['TRANSCONTINENTAL GAS PIPE LINE COMPANY, LLC'],
    'Texas Eastern Transmission, LP': ['TEXAS EASTERN TRANSMISSION'],
    'Florida Gas Transmission Company, LLC': ['FLORIDA GAS TRANSMISSION COMPANY'],
    'Colorado Interstate Gas Company, LLC': ['COLORADO INTERSTATE GAS COMPANY'],
    'Algonquin Gas Transmission, LLC': ['ALGONQUIN GAS TRANSMISSION'],
}

ZONE_COORDS = {
    # Transco zones
    'Transco': {
        '1': {'lat': 29.5, 'lng': -96.5, 'state': 'TX'},
        '2': {'lat': 30.5, 'lng': -91.0, 'state': 'LA'},
        '3': {'lat': 32.0, 'lng': -88.5, 'state': 'MS/AL'},
        '4': {'lat': 33.5, 'lng': -84.5, 'state': 'GA'},
        '5': {'lat': 36.5, 'lng': -79.0, 'state': 'NC/VA'},
        '6': {'lat': 40.5, 'lng': -74.5, 'state': 'NJ/NY'},
    },
    # Texas Eastern zones
    'Texas Eastern': {
        '1': {'lat': 29.8, 'lng': -94.5, 'state': 'TX'},
        'STX': {'lat': 28.5, 'lng': -97.5, 'state': 'TX'},
        '2': {'lat': 31.0, 'lng': -91.5, 'state': 'LA'},
        '3': {'lat': 35.5, 'lng': -86.0, 'state': 'TN/KY'},
        'ELA': {'lat': 30.5, 'lng': -90.0, 'state': 'LA'},
        'WLA': {'lat': 30.0, 'lng': -93.0, 'state': 'LA'},
        'ETX': {'lat': 31.5, 'lng': -94.5, 'state': 'TX'},
        'SLA': {'lat': 29.5, 'lng': -91.0, 'state': 'LA'},
        'M1': {'lat': 40.3, 'lng': -75.0, 'state': 'PA/NJ'},
        'M2': {'lat': 40.8, 'lng': -74.0, 'state': 'NJ'},
        'M3': {'lat': 41.2, 'lng': -73.0, 'state': 'CT'},
    },
    # Algonquin zones
    'Algonquin': {
        'SE': {'lat': 41.0, 'lng': -73.5, 'state': 'CT'},
        'NE': {'lat': 42.0, 'lng': -72.0, 'state': 'MA'},
        'AG': {'lat': 41.5, 'lng': -73.0, 'state': 'CT/NY'},
    },
}


def load_hifld_points():
    """Load or fetch HIFLD gas receipt/delivery points."""
    if os.path.exists(HIFLD_CACHE):
        with open(HIFLD_CACHE) as f:
            return json.load(f)
    
    print("Fetching HIFLD gas points (first run)...")
    base_url = "https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Natural_Gas_Receipt_Delivery_Points/FeatureServer/0/query"
    all_pts = []
    offset = 0
    while True:
        r = requests.get(base_url, params={
            'where': "COUNTRY='USA'",
            'outFields': 'NAME,STATE,COUNTY,TYPE,COMPNAME,LATITUDE,LONGITUDE',
            'resultOffset': offset, 'resultRecordCount': 2000, 'f': 'json'
        }, headers={'user-agent': 'Mozilla/5.0'})
        features = r.json().get('features', [])
        for f in features:
            a = f['attributes']
            all_pts.append({
                'name': a.get('NAME', ''),
                'state': a.get('STATE', ''),
                'county': a.get('COUNTY', ''),
                'company': a.get('COMPNAME', ''),
                'lat': a.get('LATITUDE', 0),
                'lng': a.get('LONGITUDE', 0),
            })
        if len(features) < 2000:
            break
        offset += 2000
        time.sleep(1)
    
    with open(HIFLD_CACHE, 'w') as f:
        json.dump(all_pts, f)
    print(f"  Cached {len(all_pts)} HIFLD points")
    return all_pts


def normalize_name(name):
    """Normalize point name for fuzzy matching."""
    n = name.upper().strip()
    for prefix in ['EPNG/', 'SNG/', 'TGP/', 'NGPL/', 'TETCO/', 'GS-', 'GS ', 'KMTP/', 'MEP/']:
        if n.startswith(prefix):
            n = n[len(prefix):]
    n = re.sub(r'\([^)]*\)', '', n)
    for word in [' DEL', ' REC', ' DELIVERY', ' RECEIPT', ' METER', ' STATION', ' STA',
                 ' SHIPPER', ' DEDUCT', ' DED', ' PLANT', ' POWER',
                 ' LLC', ' INC', ' CORP', ' CO', ' COMPANY']:
        n = n.replace(word, '')
    return re.sub(r'\s+', ' ', n).strip()


def name_match_score(n1, n2):
    """Score similarity between two point names."""
    from difflib import SequenceMatcher
    a, b = normalize_name(n1), normalize_name(n2)
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.9
    wa, wb = a.split(), b.split()
    if wa and wb and wa[0] == wb[0] and len(wa[0]) > 3:
        return max(0.6, SequenceMatcher(None, a, b).ratio())
    return SequenceMatcher(None, a, b).ratio()


def geocode_counties(counties_needing_coords):
    """Batch geocode county names via Geocodio."""
    if not counties_needing_coords or not GEOCODIO_KEY:
        return {}
    
    queries = [f"{nc.split('|')[0]} County, {nc.split('|')[1]}" for nc in counties_needing_coords]
    results = {}
    
    # Geocodio batch limit is 10,000
    for i in range(0, len(queries), 500):
        batch = queries[i:i+500]
        batch_keys = counties_needing_coords[i:i+500]
        try:
            r = requests.post(f"https://api.geocod.io/v1.7/geocode?api_key={GEOCODIO_KEY}", json=batch)
            if r.status_code == 200:
                for j, result in enumerate(r.json().get('results', [])):
                    locs = result.get('response', {}).get('results', [])
                    if locs:
                        results[batch_keys[j]] = {
                            'lat': locs[0]['location']['lat'],
                            'lng': locs[0]['location']['lng'],
                        }
        except Exception as e:
            print(f"  Geocodio error: {e}")
        time.sleep(1)
    
    return results


def geocode_and_locate(pipelines):
    """Add lat/lng to all points using HIFLD matching, county geocoding, and zone fallback."""
    import random
    random.seed(42)
    
    # Load caches
    county_coords = {}
    if os.path.exists(COUNTY_CACHE):
        with open(COUNTY_CACHE) as f:
            county_coords = json.load(f)
    
    hifld = load_hifld_points()
    
    # Build HIFLD lookups
    hifld_by_company = defaultdict(list)
    hifld_by_company_county = defaultdict(list)
    for h in hifld:
        comp = h['company'].upper()
        hifld_by_company[comp].append(h)
        county = h.get('county', '').upper()
        if county and county != 'NOT AVAILABLE':
            hifld_by_company_county[f"{comp}|{county}|{h['state']}"].append(h)
    
    # Find new counties that need geocoding
    new_counties = []
    for pl in pipelines:
        for pt in pl['points']:
            county, state = pt.get('county', ''), pt.get('state', '')
            if county and state:
                key = f"{county}|{state}"
                if key not in county_coords:
                    new_counties.append(key)
    
    new_counties = list(set(new_counties))
    if new_counties:
        print(f"\nGeocoding {len(new_counties)} new counties...")
        new_coords = geocode_counties(new_counties)
        county_coords.update(new_coords)
        with open(COUNTY_CACHE, 'w') as f:
            json.dump(county_coords, f)
        print(f"  Geocoded {len(new_coords)}/{len(new_counties)}")
    
    # Match each point
    stats = {'hifld': 0, 'county': 0, 'zone': 0, 'total': 0}
    
    for pl in pipelines:
        hifld_companies = PIPELINE_HIFLD_MAP.get(pl['name'], [])
        zone_map = ZONE_COORDS.get(pl['short'], {})
        
        for pt in pl['points']:
            stats['total'] += 1
            county = pt.get('county', '').upper()
            state = pt.get('state', '')
            matched = False
            
            # Pass 1: HIFLD name match (within state if available, global if not)
            best_score = 0
            best_match = None
            
            for hc in hifld_companies:
                if state and '/' not in state:
                    candidates = [h for h in hifld_by_company.get(hc, []) if h['state'] == state]
                elif state and '/' in state:
                    states = state.split('/')
                    candidates = [h for h in hifld_by_company.get(hc, []) if h['state'] in states]
                else:
                    candidates = hifld_by_company.get(hc, [])
                
                for c in candidates:
                    score = name_match_score(pt['name'], c['name'])
                    if county and c.get('county', '').upper() not in ('', 'NOT AVAILABLE'):
                        if county in c['county'].upper() or c['county'].upper() in county:
                            score += 0.1
                    if score > best_score:
                        best_score = score
                        best_match = c
            
            # Pass 2: County-only match (single point of same type in county)
            if best_score < 0.4 and county and state:
                ptype_map = {'delivery': 'DELIVERY', 'receipt': 'RECEIPT'}
                type_kw = ptype_map.get(pt['type'], '')
                for hc in hifld_companies:
                    key = f"{hc}|{county}|{state}"
                    cands = hifld_by_company_county.get(key, [])
                    typed = [c for c in cands if type_kw in c.get('type', '').upper()] if type_kw else cands
                    if len(typed) == 1:
                        best_score = 0.7
                        best_match = typed[0]
                        break
            
            # Apply HIFLD match
            if best_score >= 0.4 and best_match:
                pt['lat'] = round(best_match['lat'], 5)
                pt['lng'] = round(best_match['lng'], 5)
                pt['loc_accuracy'] = 'hifld'
                if best_match.get('county', 'NOT AVAILABLE') != 'NOT AVAILABLE' and not pt.get('county'):
                    pt['county'] = best_match['county'].title()
                if best_match.get('state') and not pt.get('state'):
                    pt['state'] = best_match['state']
                stats['hifld'] += 1
                matched = True
            
            # Fallback: county centroid
            if not matched and county and state:
                key = f"{pt['county']}|{state}"
                if key in county_coords:
                    cc = county_coords[key]
                    pt['lat'] = round(cc['lat'] + random.uniform(-0.05, 0.05), 5)
                    pt['lng'] = round(cc['lng'] + random.uniform(-0.05, 0.05), 5)
                    pt['loc_accuracy'] = 'county'
                    stats['county'] += 1
                    matched = True
            
            # Fallback: zone centroid
            if not matched:
                zone = pt.get('zone', '')
                zc = zone_map.get(zone)
                if zc:
                    pt['lat'] = round(zc['lat'] + random.uniform(-0.8, 0.8), 5)
                    pt['lng'] = round(zc['lng'] + random.uniform(-0.8, 0.8), 5)
                    pt['state'] = pt.get('state') or zc.get('state', '')
                    pt['loc_accuracy'] = 'zone'
                    stats['zone'] += 1
                    matched = True
            
            if not matched:
                pt['loc_accuracy'] = 'zone'
                stats['zone'] += 1
        
    # Remove points with no coordinates
    for pl in pipelines:
        pl['points'] = [p for p in pl['points'] if 'lat' in p]
    
    total = stats['total']
    print(f"\nGeocoding results:")
    print(f"  HIFLD precise: {stats['hifld']} ({stats['hifld']*100//max(total,1)}%)")
    print(f"  County centroid: {stats['county']} ({stats['county']*100//max(total,1)}%)")
    print(f"  Zone estimate: {stats['zone']} ({stats['zone']*100//max(total,1)}%)")

if __name__ == '__main__':
    print(f"=== Gas Interconnect Refresh: {TODAY} ===\n")
    
    # Fetch fresh data
    pipelines = fetch_all_capacity()
    
    total_pts = sum(len(p['points']) for p in pipelines)
    print(f"\nTotal: {len(pipelines)} pipelines, {total_pts} points")
    
    # Update history for rolling averages
    history = update_history(pipelines)
    
    # Compute rolling stats
    compute_rolling_stats(pipelines, history)
    
    # Geocode + HIFLD coordinate matching
    geocode_and_locate(pipelines)
    
    output = {'pipelines': pipelines}
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f)
    
    print(f"\nOutput: {os.path.getsize(OUTPUT_FILE) / 1024:.0f} KB")
    print("Done.")


