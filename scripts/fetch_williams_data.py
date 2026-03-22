"""
Williams Pipeline Data Fetcher
5 pipelines across 3 portals — all confirmed accessible, no auth required.

Portal 1: 1Line (1line.williams.com) — Transco + Gulfstream
  Transco: IOC (TAB download), Locations (HTML scrape), Unsub (RTF)
  Gulfstream: Unsub (RTF), Locations (RTF)

Portal 2: NW Pipeline (northwest.williams.com)
  IOC (direct TAB), Unsub (HTML scrape), OAC (HTML scrape)

Portal 3: MountainWest (mwpipe.com)
  MWP + OTP: IOC (HTML scrape), Unsub (HTML scrape), OAC (HTML scrape)

Runs weekly via GitHub Actions (williams-refresh.yml).
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
TIMEOUT = 60

# Try importing striprtf for RTF parsing (Transco/Gulfstream)
try:
    from striprtf.striprtf import rtf_to_text
    HAS_STRIPRTF = True
except ImportError:
    HAS_STRIPRTF = False
    print("WARNING: striprtf not installed — RTF parsing (Transco unsub, Gulfstream) will be skipped")


PIPELINES = [
    {
        'portal': '1line',
        'name': 'Transcontinental Gas Pipe Line Company (Transco)',
        'short': 'Transco',
        'tracker_name': 'Transco',
        'hifld_points': 125,
        'platform': 'williams_1line',
    },
    {
        'portal': '1line',
        'name': 'Gulfstream Natural Gas System, L.L.C.',
        'short': 'Gulfstream',
        'tracker_name': 'Gulfstream Natural Gas',
        'hifld_points': 18,
        'platform': 'williams_1line',
    },
    {
        'portal': 'nwp',
        'name': 'Northwest Pipeline LLC',
        'short': 'Northwest',
        'tracker_name': 'Northwest Pipeline',
        'hifld_points': 370,
        'platform': 'williams_nwp',
    },
    {
        'portal': 'mwpipe',
        'name': 'MountainWest Pipeline',
        'short': 'MountainWest',
        'tracker_name': 'Questar/MountainWest Pipeline',
        'mw_code': 'MWP',
        'hifld_points': 77,
        'platform': 'williams_mwpipe',
    },
    {
        'portal': 'mwpipe',
        'name': 'MountainWest Overthrust Pipeline',
        'short': 'MountainWest Overthrust',
        'tracker_name': 'MountainWest Overthrust Pipeline',
        'mw_code': 'OTP',
        'hifld_points': 0,
        'platform': 'williams_mwpipe',
    },
]


# ============================================================
# COMMON HELPERS
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


def extract_table_rows(html, row_pattern=None):
    """Extract data rows from HTML table. Returns list of lists of cell text."""
    if row_pattern:
        rows = re.findall(row_pattern, html, re.DOTALL | re.I)
    else:
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.I)

    result = []
    for row_html in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.I)
        if not cells:
            continue
        # Strip HTML tags and whitespace from each cell
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
# FERC H/D/P TAB PARSER (shared by Transco IOC + NWP IOC)
# ============================================================

def parse_ioc_tab(content, delimiter='\t'):
    """Parse FERC H/D/P format IOC file (TAB or CSV delimited)."""
    if isinstance(content, bytes):
        text = content.decode('utf-8-sig')
    else:
        text = content

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
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
# PORTAL 1: 1LINE (Transco + Gulfstream)
# ============================================================

def fetch_1line_ioc_transco():
    """Fetch Transco IOC TAB from 1line — scrape document list for latest TAB link."""
    s = make_session()
    url = 'https://www.1line.williams.com/xhtml/document_list.jsf?category=IndexOfCus&pipe_id=Transco'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Transco doc list returned {r.status_code}")
        return None

    # Find DocumentDownload links with .TAB or Text+Format
    links = re.findall(r'DocumentDownload\.jsp\?delvid=(\d+)&hfFileName=([^"&]+)', r.text)
    if not links:
        print(f"    No download links found on Transco IOC page")
        return None

    # Pick the TAB file (Text Format)
    tab_link = None
    for delvid, fname in links:
        if fname.lower().endswith('.tab') or 'text' in fname.lower() or 'Text' in fname:
            tab_link = (delvid, fname)
            break

    if not tab_link:
        # Fall back to first non-PDF
        for delvid, fname in links:
            if not fname.lower().endswith('.pdf'):
                tab_link = (delvid, fname)
                break

    if not tab_link:
        print(f"    No TAB file found, only PDFs: {[f for _, f in links]}")
        return None

    delvid, fname = tab_link
    print(f"    IOC: downloading {fname} (delvid={delvid})")
    dl_url = f'https://www.1line.williams.com/ebbCode/DocumentDownload.jsp?delvid={delvid}&hfFileName={fname}'
    r2 = s.get(dl_url, timeout=TIMEOUT)
    if r2.status_code != 200:
        print(f"    IOC download returned {r2.status_code}")
        return None

    print(f"    IOC: {len(r2.content):,} bytes")
    return r2.content


def fetch_1line_locations_transco():
    """Fetch Transco locations from 1line HTML — ICEfaces datatable with ~935 rows."""
    s = make_session()
    url = 'https://www.1line.williams.com/xhtml/location_data_download.jsf?buid=80'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Transco locations returned {r.status_code}")
        return None
    print(f"    Locations: {len(r.text):,} bytes")
    return r.text


def parse_1line_locations_html(html):
    """Parse Transco locations HTML table into dict indexed by Loc ID.

    Columns: TSP Name, TSP, TSP FERC CID, Loc Name, Loc, Loc Zone, State, County,
    Dir Flo, Status, Type, Eff Date, Inact Date, Up/Dn Ind, Up/Dn Name, Up/Dn ID Prop,
    Up/Dn ID, Up/Dn Loc Name, Up/Dn Loc, Up/Dn FERC CID Ind, Up/Dn FERC CID, Update D/T
    """
    rows = extract_table_rows(html)
    locs = {}
    col_names = ['TSP Name', 'TSP', 'TSP FERC CID', 'Loc Name', 'Loc', 'Loc Zone',
                 'State', 'County', 'Dir Flo', 'Status', 'Type', 'Eff Date', 'Inact Date',
                 'Up/Dn Ind', 'Up/Dn Name', 'Up/Dn ID Prop', 'Up/Dn ID',
                 'Up/Dn Loc Name', 'Up/Dn Loc', 'Up/Dn FERC CID Ind', 'Up/Dn FERC CID', 'Update D/T']

    for cells in rows:
        if len(cells) < 8:
            continue
        # Map cells to column names
        row_dict = {}
        for i, name in enumerate(col_names):
            if i < len(cells):
                row_dict[name] = cells[i]

        loc_id = row_dict.get('Loc', '').strip()
        if not loc_id or not loc_id[0].isdigit():
            continue

        # Use standard field names for downstream compatibility
        locs[loc_id] = {
            'Loc': loc_id,
            'Loc Name': row_dict.get('Loc Name', ''),
            'Loc Cnty': row_dict.get('County', ''),
            'Loc St Abbrev': row_dict.get('State', ''),
            'Dir Flo': row_dict.get('Dir Flo', ''),
            'Loc Type Ind': row_dict.get('Type', ''),
            'Up/Dn Name': row_dict.get('Up/Dn Name', ''),
            'Loc Zone': row_dict.get('Loc Zone', ''),
        }
    return locs


def fetch_1line_unsub(pipe_id):
    """Fetch unsub RTF from 1line document list for a given pipe_id."""
    if not HAS_STRIPRTF:
        print(f"    Skipping unsub — striprtf not installed")
        return None

    s = make_session()
    url = f'https://www.1line.williams.com/xhtml/document_list.jsf?category=UnsubsCap&pipe_id={pipe_id}'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Unsub doc list returned {r.status_code}")
        return None

    # Find download links
    links = re.findall(r'DocumentDownload\.jsp\?delvid=(\d+)&hfFileName=([^"&]+)', r.text)
    if not links:
        print(f"    No download links found on unsub page")
        return None

    # Pick first non-PDF (RTF)
    rtf_link = None
    for delvid, fname in links:
        fl = fname.lower()
        if fl.endswith('.pdf') or 'no updated data' in fl.lower():
            continue
        rtf_link = (delvid, fname)
        break

    # If all seem like "No Updated Data", try the first link anyway
    if not rtf_link:
        for delvid, fname in links:
            if not fname.lower().endswith('.pdf'):
                rtf_link = (delvid, fname)
                break

    if not rtf_link:
        print(f"    No RTF file found: {[f for _, f in links]}")
        return None

    delvid, fname = rtf_link
    print(f"    Unsub: downloading {fname} (delvid={delvid})")
    dl_url = f'https://www.1line.williams.com/ebbCode/DocumentDownload.jsp?delvid={delvid}&hfFileName={fname}'
    r2 = s.get(dl_url, timeout=TIMEOUT)
    if r2.status_code != 200:
        print(f"    Unsub download returned {r2.status_code}")
        return None

    print(f"    Unsub: {len(r2.content):,} bytes")
    return r2.content


def parse_unsub_rtf(content):
    """Parse unsub RTF file using striprtf. Extract Loc, Loc Name, Unsub Cap."""
    if not HAS_STRIPRTF or not content:
        return []

    try:
        text = rtf_to_text(content.decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"    RTF parse error: {e}")
        return []

    result = []
    lines = text.split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Look for lines with numeric location IDs and capacity values
        # Format varies but typically: loc_id  loc_name  ... unsub_capacity
        parts = re.split(r'\t+|\s{2,}', line)
        if len(parts) < 3:
            continue

        # First part should be a numeric loc ID
        loc_id = parts[0].strip()
        if not loc_id or not any(c.isdigit() for c in loc_id):
            continue

        # Try to find capacity value (last numeric part)
        unsub_val = 0
        loc_name = ''
        for i in range(len(parts) - 1, 0, -1):
            val = parse_int_safe(parts[i])
            if val > 0:
                unsub_val = val
                loc_name = ' '.join(parts[1:i]).strip()
                break

        if unsub_val > 0:
            result.append({
                'Loc': loc_id,
                'Loc_Name': loc_name[:50],
                'Loc_Purp_Desc': '',
                'Unsubscribed_Capacity': unsub_val,
            })

    return result


def fetch_1line_locations_gulfstream():
    """Fetch Gulfstream locations RTF from 1line."""
    if not HAS_STRIPRTF:
        print(f"    Skipping locations — striprtf not installed")
        return None

    s = make_session()
    url = 'https://www.1line.williams.com/xhtml/document_list.jsf?category=MastrLocList&pipe_id=Gulfstream'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    Gulfstream loc doc list returned {r.status_code}")
        return None

    links = re.findall(r'DocumentDownload\.jsp\?delvid=(\d+)&hfFileName=([^"&]+)', r.text)
    if not links:
        print(f"    No download links on Gulfstream locations page")
        return None

    # Pick first non-PDF
    rtf_link = None
    for delvid, fname in links:
        if not fname.lower().endswith('.pdf'):
            rtf_link = (delvid, fname)
            break

    if not rtf_link:
        print(f"    No RTF found: {[f for _, f in links]}")
        return None

    delvid, fname = rtf_link
    print(f"    Locations: downloading {fname} (delvid={delvid})")
    dl_url = f'https://www.1line.williams.com/ebbCode/DocumentDownload.jsp?delvid={delvid}&hfFileName={fname}'
    r2 = s.get(dl_url, timeout=TIMEOUT)
    if r2.status_code != 200:
        return None

    print(f"    Locations: {len(r2.content):,} bytes")
    return r2.content


def parse_locations_rtf(content):
    """Parse Gulfstream locations RTF into dict indexed by Loc ID."""
    if not HAS_STRIPRTF or not content:
        return {}

    try:
        text = rtf_to_text(content.decode('utf-8', errors='replace'))
    except Exception as e:
        print(f"    RTF locations parse error: {e}")
        return {}

    locs = {}
    lines = text.split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        parts = re.split(r'\t+|\s{2,}', line)
        if len(parts) < 4:
            continue

        # Look for rows starting with a numeric loc ID
        loc_id = parts[0].strip()
        if not loc_id or not any(c.isdigit() for c in loc_id):
            continue

        # Try to extract loc name, state, county from remaining parts
        loc_name = parts[1].strip() if len(parts) > 1 else ''

        # Look for state abbreviation (2 letters)
        state = ''
        county = ''
        for p in parts[2:]:
            p = p.strip()
            if len(p) == 2 and p.isalpha() and p.isupper():
                state = p
            elif county == '' and p and not p[0].isdigit():
                county = p

        locs[loc_id] = {
            'Loc': loc_id,
            'Loc Name': loc_name[:50],
            'Loc Cnty': county,
            'Loc St Abbrev': state,
            'Dir Flo': '',
            'Loc Type Ind': '',
            'Up/Dn Name': '',
        }

    return locs


# ============================================================
# PORTAL 2: NW PIPELINE (northwest.williams.com)
# ============================================================

def fetch_nwp_ioc():
    """Fetch NW Pipeline IOC TAB directly — no scraping needed."""
    s = make_session()
    url = 'https://northwest.williams.com/NWP_Portal/file_download?hfFileURL=Files/Northwest/Downloads/Shipper.tab'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    NWP IOC returned {r.status_code}")
        return None
    print(f"    IOC: {len(r.content):,} bytes")
    return r.content


def fetch_nwp_unsub():
    """Fetch NW Pipeline unsub HTML table."""
    s = make_session()
    url = 'https://northwest.williams.com/NWP_Portal/UnsubscribedCapForm.action'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    NWP unsub returned {r.status_code}")
        return None
    print(f"    Unsub: {len(r.text):,} bytes")
    return r.text


def parse_nwp_unsub_html(html):
    """Parse NW Pipeline unsub HTML table.

    Rows inside <tbody class="fontNorm">.
    Columns: Loc Prop, Loc Name, Loc, Loc Purp Desc, Loc/QTI, Phys Design,
    K Design, Subscribed, Unsub Cap (multiple cols for capacity types).
    """
    # Extract tbody with fontNorm class
    tbody_match = re.search(r'<tbody[^>]*class="fontNorm"[^>]*>(.*?)</tbody>', html, re.DOTALL | re.I)
    if not tbody_match:
        # Fall back to all rows
        search_html = html
    else:
        search_html = tbody_match.group(1)

    rows = extract_table_rows(search_html)
    result = []

    for cells in rows:
        if len(cells) < 6:
            continue

        loc_id = cells[2].strip() if len(cells) > 2 else ''
        loc_name = cells[1].strip() if len(cells) > 1 else ''
        loc_purp = cells[3].strip() if len(cells) > 3 else ''

        if not loc_id or not any(c.isdigit() for c in loc_id):
            continue

        # Find unsub capacity — typically one of the later columns
        unsub_val = 0
        for i in range(len(cells) - 1, 4, -1):
            val = parse_int_safe(cells[i])
            if val > 0:
                unsub_val = val
                break

        if unsub_val > 0:
            result.append({
                'Loc': loc_id,
                'Loc_Name': loc_name[:50],
                'Loc_Purp_Desc': loc_purp,
                'Unsubscribed_Capacity': unsub_val,
            })

    return result


def fetch_nwp_oac():
    """Fetch NW Pipeline OAC HTML table."""
    s = make_session()
    url = 'https://northwest.williams.com/NWP_Portal/CapacityResultsScrollable.action'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    NWP OAC returned {r.status_code}")
        return None
    print(f"    OAC: {len(r.text):,} bytes")
    return r.text


def parse_nwp_oac_html(html):
    """Parse NW Pipeline OAC HTML.

    Columns: Loc Prop, Loc Name, Loc, Loc Purp Desc, Flow Ind Desc, Loc/QTI,
    Design Cap, Operating Capacity, Total Scheduled Qty, Operationally Available Capacity, IT.
    """
    tbody_match = re.search(r'<tbody[^>]*class="fontNorm"[^>]*>(.*?)</tbody>', html, re.DOTALL | re.I)
    search_html = tbody_match.group(1) if tbody_match else html

    rows = extract_table_rows(search_html)
    oac = {}

    for cells in rows:
        if len(cells) < 10:
            continue

        loc_id = cells[2].strip() if len(cells) > 2 else ''
        if not loc_id or not any(c.isdigit() for c in loc_id):
            continue

        design = parse_int_safe(cells[6]) if len(cells) > 6 else 0
        scheduled = parse_int_safe(cells[8]) if len(cells) > 8 else 0
        available = parse_int_safe(cells[9]) if len(cells) > 9 else 0

        if design > 0 or available > 0:
            oac[loc_id] = {
                'design': design,
                'scheduled': scheduled,
                'available': available,
            }

    return oac


# ============================================================
# PORTAL 3: MOUNTAINWEST (mwpipe.com)
# ============================================================

def fetch_mw_ioc(code):
    """Fetch MountainWest IOC HTML table."""
    s = make_session()
    url = f'https://www.mwpipe.com/IndexOfCustomers/{code}'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    MW {code} IOC returned {r.status_code}")
        return None
    print(f"    IOC: {len(r.text):,} bytes")
    return r.text


def parse_mw_ioc_html(html):
    """Parse MountainWest IOC HTML table.

    Row class: highlighttablerow greenbarBackgroundON or greenbarBackgroundOFF.
    Standard IOC columns in table cells.
    """
    cutoff = datetime.now() + timedelta(days=730)

    # Find rows with the specific class pattern
    row_pattern = r'<tr[^>]*(?:greenbarBackground(?:ON|OFF)|highlighttablerow)[^>]*>(.*?)</tr>'
    row_htmls = re.findall(row_pattern, html, re.DOTALL | re.I)

    contracts = {}
    by_point = defaultdict(lambda: {'firm_mdq': 0, 'expiring_2yr': 0, 'num_contracts': 0, 'shippers': set()})
    all_shippers = set()
    total_mdq = 0

    for row_html in row_htmls:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.I)
        if not cells or len(cells) < 10:
            continue

        cleaned = []
        for cell in cells:
            text = re.sub(r'<[^>]+>', '', cell).strip()
            text = text.replace('&amp;', '&').replace('&nbsp;', ' ').replace('&#160;', ' ')
            text = re.sub(r'\s+', ' ', text).strip()
            cleaned.append(text)

        # IOC table columns vary — try to identify shipper, rate, contract, MDQ, point
        # Typical: Shipper, Affil, Rate Sched, K#, Begin, End, Neg Rate, MDQ, MSQ, Pt ID, Pt Name, Zone, Pt MDQ
        shipper = cleaned[0] if len(cleaned) > 0 else ''
        rate = cleaned[2] if len(cleaned) > 2 else ''
        contract_id = cleaned[3] if len(cleaned) > 3 else ''
        begin_date = cleaned[4] if len(cleaned) > 4 else ''
        end_date = cleaned[5] if len(cleaned) > 5 else ''

        # Find MDQ — look for a large numeric value
        mdq = 0
        mdq_idx = 7 if len(cleaned) > 7 else -1
        if mdq_idx >= 0:
            mdq = parse_int_safe(cleaned[mdq_idx])

        point_id = cleaned[9] if len(cleaned) > 9 else ''
        point_name = cleaned[10] if len(cleaned) > 10 else ''

        if not shipper or shipper.lower() in ('shipper', 'shipper name', ''):
            continue

        if mdq > 0:
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


def fetch_mw_unsub(code):
    """Fetch MountainWest unsub HTML table."""
    s = make_session()
    url = f'https://www.mwpipe.com/UnsubscribedCapacity/{code}'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    MW {code} unsub returned {r.status_code}")
        return None
    print(f"    Unsub: {len(r.text):,} bytes")
    return r.text


def parse_mw_unsub_html(html):
    """Parse MountainWest unsub HTML.

    Row class: greenbarBackgroundON/OFF.
    Columns: Loc/QTI, Loc Purp Desc, Loc, Loc Name, RecLocTP, Loc Purp Desc,
    Loc, Loc Name, Route Cd, DelLocTP, Unsub Cap, Eff Gas Day, End Eff Gas Day, Comments.
    Values have commas.
    """
    row_pattern = r'<tr[^>]*(?:greenbarBackground(?:ON|OFF)|highlighttablerow)[^>]*>(.*?)</tr>'
    row_htmls = re.findall(row_pattern, html, re.DOTALL | re.I)

    result = []
    seen_locs = set()

    for row_html in row_htmls:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.I)
        if not cells or len(cells) < 10:
            continue

        cleaned = []
        for cell in cells:
            text = re.sub(r'<[^>]+>', '', cell).strip()
            text = text.replace('&amp;', '&').replace('&nbsp;', ' ').replace('&#160;', ' ')
            text = re.sub(r'\s+', ' ', text).strip()
            cleaned.append(text)

        # Loc is typically at index 2, Loc Name at 3, Unsub Cap at 10
        loc_id = cleaned[2].strip() if len(cleaned) > 2 else ''
        loc_name = cleaned[3].strip() if len(cleaned) > 3 else ''
        loc_purp = cleaned[1].strip() if len(cleaned) > 1 else ''

        if not loc_id or not any(c.isdigit() for c in loc_id):
            continue

        unsub_val = parse_int_safe(cleaned[10]) if len(cleaned) > 10 else 0

        if unsub_val > 0:
            key = f"{loc_id}"
            if key not in seen_locs:
                result.append({
                    'Loc': loc_id,
                    'Loc_Name': loc_name[:50],
                    'Loc_Purp_Desc': loc_purp,
                    'Unsubscribed_Capacity': unsub_val,
                })
                seen_locs.add(key)
            else:
                # Aggregate capacity for same location
                for existing in result:
                    if existing['Loc'] == loc_id:
                        existing['Unsubscribed_Capacity'] += unsub_val
                        break

    return result


def fetch_mw_oac(code):
    """Fetch MountainWest OAC HTML table."""
    s = make_session()
    url = f'https://www.mwpipe.com/OperationalCapacity/{code}/TIM/CURRENT'
    r = s.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        print(f"    MW {code} OAC returned {r.status_code}")
        return None
    print(f"    OAC: {len(r.text):,} bytes")
    return r.text


def parse_mw_oac_html(html):
    """Parse MountainWest OAC HTML. Standard OAC columns."""
    row_pattern = r'<tr[^>]*(?:greenbarBackground(?:ON|OFF)|highlighttablerow)[^>]*>(.*?)</tr>'
    row_htmls = re.findall(row_pattern, html, re.DOTALL | re.I)

    oac = {}

    for row_html in row_htmls:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL | re.I)
        if not cells or len(cells) < 8:
            continue

        cleaned = []
        for cell in cells:
            text = re.sub(r'<[^>]+>', '', cell).strip()
            text = text.replace('&amp;', '&').replace('&nbsp;', ' ')
            text = re.sub(r'\s+', ' ', text).strip()
            cleaned.append(text)

        # Find loc_id — typically a numeric field
        loc_id = ''
        for c in cleaned[:4]:
            if c and any(ch.isdigit() for ch in c) and len(c) < 15:
                loc_id = c.strip()
                break

        if not loc_id:
            continue

        # Find design, scheduled, available from numeric columns
        nums = []
        for c in cleaned[4:]:
            nums.append(parse_int_safe(c))

        design = nums[0] if len(nums) > 0 else 0
        scheduled = nums[2] if len(nums) > 2 else 0
        available = nums[3] if len(nums) > 3 else 0

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
        flow = str(loc.get('Dir Flo', '')).strip()
        loc_name = str(loc.get('Loc Name', '')).strip()
        connected = str(loc.get('Up/Dn Name', '')).strip()[:50]

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
# PIPELINE PROCESSORS
# ============================================================

def process_transco(county_coords):
    """Process Transco: IOC (TAB), Locations (HTML), Unsub (RTF)."""
    print("\n--- Transco (1line) ---")

    print("  Fetching IOC...")
    ioc_content = fetch_1line_ioc_transco()
    ioc_data = parse_ioc_tab(ioc_content) if ioc_content else EMPTY_IOC.copy()
    print(f"    IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    print("  Fetching Locations...")
    loc_html = fetch_1line_locations_transco()
    loc_data = parse_1line_locations_html(loc_html) if loc_html else {}
    print(f"    Locations: {len(loc_data)} points")

    print("  Fetching Unsub...")
    unsub_content = fetch_1line_unsub('Transco')
    unsub_data = parse_unsub_rtf(unsub_content) if unsub_content else []
    print(f"    Unsub: {len(unsub_data)} points")

    by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points(loc_data, by_point, {}, county_coords)

    return {
        'ioc_data': ioc_data,
        'unsub_data': unsub_data,
        'oac_data': {},
        'loc_data': loc_data,
        'points': points,
        'new_counties': new_counties,
    }


def process_gulfstream(county_coords):
    """Process Gulfstream: Unsub (RTF), Locations (RTF). No IOC on portal."""
    print("\n--- Gulfstream (1line) ---")

    print("  Fetching Unsub...")
    unsub_content = fetch_1line_unsub('Gulfstream')
    unsub_data = parse_unsub_rtf(unsub_content) if unsub_content else []
    print(f"    Unsub: {len(unsub_data)} points")

    print("  Fetching Locations...")
    loc_content = fetch_1line_locations_gulfstream()
    loc_data = parse_locations_rtf(loc_content) if loc_content else {}
    print(f"    Locations: {len(loc_data)} points")

    points, new_counties = build_points(loc_data, {}, {}, county_coords)

    return {
        'ioc_data': EMPTY_IOC.copy(),
        'unsub_data': unsub_data,
        'oac_data': {},
        'loc_data': loc_data,
        'points': points,
        'new_counties': new_counties,
    }


def process_nwp(county_coords):
    """Process NW Pipeline: IOC (TAB), Unsub (HTML), OAC (HTML)."""
    print("\n--- NW Pipeline (northwest.williams.com) ---")

    print("  Fetching IOC...")
    ioc_content = fetch_nwp_ioc()
    ioc_data = parse_ioc_tab(ioc_content) if ioc_content else EMPTY_IOC.copy()
    print(f"    IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    print("  Fetching Unsub...")
    unsub_html = fetch_nwp_unsub()
    unsub_data = parse_nwp_unsub_html(unsub_html) if unsub_html else []
    print(f"    Unsub: {len(unsub_data)} points")

    print("  Fetching OAC...")
    oac_html = fetch_nwp_oac()
    oac_data = parse_nwp_oac_html(oac_html) if oac_html else {}
    print(f"    OAC: {len(oac_data)} points")

    # NWP has no locations endpoint — points come from IOC + OAC only
    by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points({}, by_point, oac_data, county_coords)

    return {
        'ioc_data': ioc_data,
        'unsub_data': unsub_data,
        'oac_data': oac_data,
        'loc_data': {},
        'points': points,
        'new_counties': new_counties,
    }


def process_mountainwest(code, county_coords):
    """Process MountainWest pipeline (MWP or OTP)."""
    print(f"\n--- MountainWest {code} (mwpipe.com) ---")

    print("  Fetching IOC...")
    ioc_html = fetch_mw_ioc(code)
    ioc_data = parse_mw_ioc_html(ioc_html) if ioc_html else EMPTY_IOC.copy()
    print(f"    IOC: {ioc_data['num_contracts']} contracts, {ioc_data['num_shippers']} shippers, {ioc_data['total_mdq']:,} MDQ")

    print("  Fetching Unsub...")
    unsub_html = fetch_mw_unsub(code)
    unsub_data = parse_mw_unsub_html(unsub_html) if unsub_html else []
    print(f"    Unsub: {len(unsub_data)} points")

    print("  Fetching OAC...")
    oac_html = fetch_mw_oac(code)
    oac_data = parse_mw_oac_html(oac_html) if oac_html else {}
    print(f"    OAC: {len(oac_data)} points")

    # MountainWest has no locations endpoint — points from IOC + OAC
    by_point = ioc_data.get('by_point', {})
    points, new_counties = build_points({}, by_point, oac_data, county_coords)

    return {
        'ioc_data': ioc_data,
        'unsub_data': unsub_data,
        'oac_data': oac_data,
        'loc_data': {},
        'points': points,
        'new_counties': new_counties,
    }


# ============================================================
# MERGE AND TRACKER
# ============================================================

def merge_into_gas_interconnects(all_results):
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            gi = json.load(f)
    else:
        gi = {'pipelines': []}

    shorts = {pl['short'] for pl, _ in all_results}
    gi['pipelines'] = [p for p in gi['pipelines'] if p.get('short') not in shorts]

    for pl, r in all_results:
        entry = {
            'name': pl['name'],
            'short': pl['short'],
            'updated': TODAY,
            'points': r['points'],
            'unsub_points': r['unsub_data'],
            'ioc_totals': {
                'firm_mdq': r['ioc_data']['total_mdq'],
                'num_contracts': r['ioc_data']['num_contracts'],
                'num_shippers': r['ioc_data']['num_shippers'],
            },
        }
        gi['pipelines'].append(entry)

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(gi, f)
    print(f"\ngas_interconnects.json: {len(gi['pipelines'])} total pipelines")


def update_tracker(all_results):
    if not os.path.exists(TRACKER_FILE):
        return

    with open(TRACKER_FILE) as f:
        tracker = json.load(f)

    pipelines_list = tracker.get('gas_pipelines', [])

    for pl, r in all_results:
        tracker_name = pl['tracker_name']
        platform = pl['platform']
        ioc_data = r['ioc_data']
        unsub_data = r['unsub_data']
        oac_data = r['oac_data']
        loc_data = r['loc_data']
        geocoded_count = sum(1 for pt in r['points'] if 'lat' in pt)

        # Find existing entry or create new one
        entry = None
        for e in pipelines_list:
            if e.get('pipeline_name') == tracker_name:
                entry = e
                break

        if not entry:
            # Add new entry (e.g. MountainWest Overthrust)
            entry = {
                'pipeline_name': tracker_name,
                'operator': 'Williams',
                'regulation': 'interstate',
                'hifld_points': pl.get('hifld_points', 0),
            }
            pipelines_list.append(entry)
            print(f"  Added new tracker entry: {tracker_name}")

        # Update IOC
        if ioc_data['num_contracts'] > 0:
            entry['ioc'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': platform,
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'contract_count': ioc_data['num_contracts'],
                'shipper_count': ioc_data['num_shippers'],
            }
        elif pl['short'] == 'Gulfstream':
            entry['ioc'] = {
                'status': 'not_available',
                'access_method': 'weekly_auto',
                'platform': platform,
                'cloud_accessible': True,
                'notes': 'No IOC found on 1line portal for Gulfstream',
            }

        # Update Unsub
        if unsub_data:
            entry['unsub'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': platform,
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'point_count': len(unsub_data),
            }
        else:
            entry['unsub'] = {
                'status': 'attempted',
                'access_method': 'weekly_auto',
                'platform': platform,
                'cloud_accessible': True,
            }

        # Update OAC
        if oac_data:
            entry['capacity'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': platform,
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
                'point_count': len(oac_data),
            }

        # Update Locations
        if loc_data:
            entry['locations'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': platform,
                'cracked_points': len(loc_data),
                'geocoded_points': geocoded_count,
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
    print(f"=== Williams Pipeline Refresh: {TODAY} ===")
    print(f"  striprtf available: {HAS_STRIPRTF}")

    county_coords = load_county_coords()
    all_results = []  # list of (pipeline_dict, result_dict)

    # Portal 1: 1Line — Transco
    try:
        pl = PIPELINES[0]  # Transco
        r = process_transco(county_coords)
        all_results.append((pl, r))
    except Exception as e:
        print(f"  ERROR Transco: {e}")
        import traceback; traceback.print_exc()

    time.sleep(2)

    # Portal 1: 1Line — Gulfstream
    try:
        pl = PIPELINES[1]  # Gulfstream
        r = process_gulfstream(county_coords)
        all_results.append((pl, r))
    except Exception as e:
        print(f"  ERROR Gulfstream: {e}")
        import traceback; traceback.print_exc()

    time.sleep(2)

    # Portal 2: NW Pipeline
    try:
        pl = PIPELINES[2]  # Northwest
        r = process_nwp(county_coords)
        all_results.append((pl, r))
    except Exception as e:
        print(f"  ERROR NW Pipeline: {e}")
        import traceback; traceback.print_exc()

    time.sleep(2)

    # Portal 3: MountainWest — MWP
    try:
        pl = PIPELINES[3]  # MountainWest
        r = process_mountainwest('MWP', county_coords)
        all_results.append((pl, r))
    except Exception as e:
        print(f"  ERROR MountainWest MWP: {e}")
        import traceback; traceback.print_exc()

    time.sleep(2)

    # Portal 3: MountainWest — OTP
    try:
        pl = PIPELINES[4]  # Overthrust
        r = process_mountainwest('OTP', county_coords)
        all_results.append((pl, r))
    except Exception as e:
        print(f"  ERROR MountainWest OTP: {e}")
        import traceback; traceback.print_exc()

    # Geocode all new counties
    all_new_counties = set()
    for _, r in all_results:
        all_new_counties.update(r.get('new_counties', []))

    if all_new_counties:
        print(f"\nGeocoding {len(all_new_counties)} new counties...")
        geocoded = 0
        for key in all_new_counties:
            county, state = key.split('|')
            lat, lng = geocode_county(county, state)
            if lat and lng:
                county_coords[key] = {'lat': lat, 'lng': lng}
                geocoded += 1
            time.sleep(0.5)
        save_county_coords(county_coords)
        print(f"  Geocoded {geocoded}/{len(all_new_counties)}")

        # Re-apply coordinates to all points
        for _, r in all_results:
            for pt in r['points']:
                county = pt.get('county', '').upper()
                state = pt.get('state', '')
                if county and state and 'lat' not in pt:
                    key = f"{county}|{state}"
                    if key in county_coords:
                        pt['lat'] = county_coords[key]['lat']
                        pt['lng'] = county_coords[key]['lng']
                        pt['loc_accuracy'] = 'county_centroid'

    if not all_results:
        print("\nNo pipelines processed!")
        return

    # Summary
    print("\n--- Summary ---")
    for pl, r in all_results:
        geocoded = sum(1 for pt in r['points'] if 'lat' in pt)
        print(f"  {pl['short']}: {len(r['points'])} pts ({geocoded} geocoded), "
              f"{len(r['unsub_data'])} unsub, {r['ioc_data']['num_contracts']} IOC")

    merge_into_gas_interconnects(all_results)
    update_tracker(all_results)

    print(f"\n=== Done — {len(all_results)} pipelines processed ===")


if __name__ == '__main__':
    main()
