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
    
    # TODO: geocoding + HIFLD matching (reuse existing cached data)
    # For now, output the raw data and let a separate script handle geocoding
    
    output = {'pipelines': pipelines}
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f)
    
    print(f"\nOutput: {os.path.getsize(OUTPUT_FILE) / 1024:.0f} KB")
    print("Done.")
