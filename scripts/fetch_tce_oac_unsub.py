"""
TC Energy eConnects — OAC and Unsub Downloader
12 pipelines at ebb.tceconnects.com/infopost/ — all public, no login needed.

Unsub: Direct SSRS URL with AssetNbr parameter → XLS
OAC: SSRS session postback (no URL parameters) → XLSX

Downloads raw report files to data/gas_interconnects/.
Runs weekly via GitHub Actions (tce-refresh.yml), after fetch_tce_data.py.
"""

import requests
import json
import os
import re
import time
from datetime import datetime
from urllib.parse import quote

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
OUTPUT_DIR = os.path.join(DATA_DIR, 'gas_interconnects')
TRACKER_FILE = os.path.join(DATA_DIR, 'corridor_pipeline_tracker.json')
TODAY = datetime.now().strftime('%Y-%m-%d')

SSRS_BASE = 'https://ebb.tceconnects.com/infopost'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

UNSUB_REPORT = '/InfoPost/UnsubscribedCapacity'
OAC_REPORT = '/InfoPost/OperationallyAvailableCapacity'

ASSETS = [
    {'name': 'ANR Pipeline',              'asset_id': 3005, 'folder': 'anr',   'tracker_name': 'ANR Pipeline'},
    {'name': 'ANR Storage',               'asset_id': 3009, 'folder': 'anrsc', 'tracker_name': 'ANR Storage'},
    {'name': 'Bison Pipeline',            'asset_id': 3031, 'folder': 'bison', 'tracker_name': 'Bison Pipeline'},
    {'name': 'Blue Lake Gas Storage',     'asset_id': 3014, 'folder': 'blgsc', 'tracker_name': 'Blue Lake Gas Storage'},
    {'name': 'Columbia Gas Transmission', 'asset_id': 51,   'folder': 'tco',   'tracker_name': 'Columbia Gas Transmission'},
    {'name': 'Columbia Gulf Transmission','asset_id': 14,   'folder': 'cgt',   'tracker_name': 'Columbia Gulf Transmission'},
    {'name': 'Crossroads Pipeline',       'asset_id': 44,   'folder': 'xrd',   'tracker_name': 'Crossroads Pipeline'},
    {'name': 'Eaton Rapids Gas Storage',  'asset_id': 3012, 'folder': 'ergss', 'tracker_name': 'Eaton Rapids Gas Storage System'},
    {'name': 'Hardy Storage',             'asset_id': 465,  'folder': 'hrd',   'tracker_name': 'Hardy Storage'},
    {'name': 'Millennium Pipeline',       'asset_id': 26,   'folder': 'mpl',   'tracker_name': 'Millennium Pipeline'},
    {'name': 'Northern Border Pipeline',  'asset_id': 3029, 'folder': 'nbpl',  'tracker_name': 'Northern Border Pipeline'},
    {'name': 'TC Louisiana Intrastate',   'asset_id': 3119, 'folder': 'tcli',  'tracker_name': 'TC Louisiana Intrastate'},
]

ASSET_IDS = {a['asset_id'] for a in ASSETS}


# ============================================================
# HELPERS
# ============================================================

def make_session():
    s = requests.Session()
    s.headers['User-Agent'] = UA
    return s


def extract_aspnet_fields(html):
    """Extract ASP.NET hidden form fields (__VIEWSTATE, etc.)."""
    fields = {}
    for name in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION',
                 '__EVENTTARGET', '__EVENTARGUMENT', '__PREVIOUSPAGE']:
        m = re.search(rf'id="{name}"[^>]*value="([^"]*)"', html)
        if not m:
            m = re.search(rf'name="{name}"[^>]*value="([^"]*)"', html)
        if m:
            fields[name] = m.group(1)
    return fields


# ============================================================
# UNSUB — direct SSRS URL with AssetNbr parameter
# ============================================================

def fetch_unsub(session, asset_id):
    """Download Unsub report via direct SSRS URL access. Returns XLS bytes."""
    url = (
        f'{SSRS_BASE}/ReportViewer.aspx?{UNSUB_REPORT}'
        f'&AssetNbr={asset_id}'
        f'&rs:Format=EXCEL'
    )
    try:
        r = session.get(url, timeout=90)
    except requests.Timeout:
        print(f"    Unsub: timeout")
        return None

    if r.status_code != 200:
        print(f"    Unsub: HTTP {r.status_code}")
        return None

    ct = r.headers.get('Content-Type', '').lower()
    # Expect binary Excel content, not HTML
    if '<html' in r.text[:500].lower() and 'excel' not in ct:
        print(f"    Unsub: got HTML error page ({len(r.content):,} bytes)")
        return None

    if len(r.content) < 100:
        print(f"    Unsub: response too small ({len(r.content)} bytes)")
        return None

    print(f"    Unsub: {len(r.content):,} bytes ({ct})")
    return r.content


# ============================================================
# OAC — SSRS session postback (no URL parameters)
# ============================================================

def find_asset_dropdown(html):
    """Find the SSRS parameter dropdown containing asset IDs."""
    # Match <select name="...">..options..</select> blocks
    for m in re.finditer(r'<select[^>]+name="([^"]+)"[^>]*>(.*?)</select>', html, re.DOTALL):
        name = m.group(1)
        options_html = m.group(2)
        option_values = re.findall(r'value="(\d+)"', options_html)
        # Check if any option value matches a known asset ID
        for v in option_values:
            if v.isdigit() and int(v) in ASSET_IDS:
                return name
    return None


def find_view_report_button(html):
    """Find the SSRS 'View Report' submit button name."""
    # Standard SSRS button patterns
    m = re.search(r'name="([^"]*)"[^>]*value="View Report"', html, re.IGNORECASE)
    if m:
        return m.group(1)
    # Alternative: button with id containing ViewReport
    m = re.search(r'id="([^"]*ViewReport[^"]*)"[^>]*name="([^"]*)"', html, re.IGNORECASE)
    if m:
        return m.group(2)
    return None


def find_export_url(html, fmt='EXCELOPENXML'):
    """Extract SSRS export handler URL from rendered report page."""
    # Look for Reserved.ReportViewerWebControl.axd export URLs
    pattern = r'Reserved\.ReportViewerWebControl\.axd\?[^"\'>\s]+'
    matches = re.findall(pattern, html)

    for url_fragment in matches:
        if 'OpType=Export' in url_fragment or 'Export' in url_fragment:
            return f'{SSRS_BASE}/{url_fragment}'

    # Construct from execution context if export URL not found directly
    exec_match = re.search(r'ExecutionID=([^&"\'>\s]+)', html)
    ctrl_match = re.search(r'ControlID=([^&"\'>\s]+)', html)

    if exec_match and ctrl_match:
        return (
            f'{SSRS_BASE}/Reserved.ReportViewerWebControl.axd'
            f'?ExecutionID={exec_match.group(1)}'
            f'&Culture=1033&CultureOverrides=True'
            f'&UICulture=1033&UICultureOverrides=True'
            f'&ReportStack=1'
            f'&ControlID={ctrl_match.group(1)}'
            f'&OpType=Export'
            f'&FileName=OAC'
            f'&ContentDisposition=AlwaysAttachment'
            f'&Format={fmt}'
        )

    return None


def fetch_oac(session, asset_id):
    """Download OAC report via SSRS session postback. Returns XLSX bytes."""
    report_url = f'{SSRS_BASE}/ReportViewer.aspx?{OAC_REPORT}'

    # Step 1: GET the report page to establish SSRS session
    try:
        r = session.get(report_url, timeout=90)
    except requests.Timeout:
        print(f"    OAC: timeout on initial GET")
        return None

    if r.status_code != 200:
        print(f"    OAC: initial GET returned {r.status_code}")
        return None

    html = r.text

    # Step 2: Extract ASP.NET form fields
    fields = extract_aspnet_fields(html)
    if '__VIEWSTATE' not in fields:
        print(f"    OAC: no __VIEWSTATE found")
        return None

    # Step 3: Find the asset parameter dropdown
    dropdown_name = find_asset_dropdown(html)
    if not dropdown_name:
        # Fallback: look for text input in SSRS parameter panel
        m = re.search(r'name="([^"]*(?:txtValue|ddValue)[^"]*)"', html)
        if m:
            dropdown_name = m.group(1)
            print(f"    OAC: using fallback parameter control: {dropdown_name}")
        else:
            print(f"    OAC: could not find asset parameter control")
            return None

    # Step 4: POST to render report for this asset
    fields[dropdown_name] = str(asset_id)

    btn_name = find_view_report_button(html)
    if btn_name:
        fields[btn_name] = 'View Report'

    try:
        r2 = session.post(report_url, data=fields, timeout=90)
    except requests.Timeout:
        print(f"    OAC: timeout on POST")
        return None

    if r2.status_code != 200:
        print(f"    OAC: POST returned {r2.status_code}")
        return None

    # Check if the response is already the file
    ct = r2.headers.get('Content-Type', '').lower()
    if 'excel' in ct or 'spreadsheet' in ct or 'octet-stream' in ct:
        if len(r2.content) > 100:
            print(f"    OAC: {len(r2.content):,} bytes (direct from POST)")
            return r2.content

    # Step 5: Extract export URL from rendered page
    export_url = find_export_url(r2.text)
    if not export_url:
        print(f"    OAC: could not find export URL in rendered page")
        return None

    # Step 6: Download the exported file
    try:
        r3 = session.get(export_url, timeout=90)
    except requests.Timeout:
        print(f"    OAC: timeout on export download")
        return None

    if r3.status_code != 200:
        print(f"    OAC: export download returned {r3.status_code}")
        return None

    ct = r3.headers.get('Content-Type', '').lower()
    if len(r3.content) < 100:
        print(f"    OAC: export too small ({len(r3.content)} bytes)")
        return None

    print(f"    OAC: {len(r3.content):,} bytes ({ct})")
    return r3.content


# ============================================================
# TRACKER UPDATE
# ============================================================

def update_tracker(results):
    """Update tracker — set unsub/oac to captured for successful downloads."""
    if not os.path.exists(TRACKER_FILE):
        return

    with open(TRACKER_FILE) as f:
        tracker = json.load(f)

    pipelines_list = tracker.get('gas_pipelines', [])
    tracker_map = {e['pipeline_name']: e for e in pipelines_list}

    for asset, unsub_ok, oac_ok in results:
        tracker_name = asset['tracker_name']
        entry = tracker_map.get(tracker_name)
        if not entry:
            continue

        if unsub_ok:
            entry['unsub'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'tc_energy_ssrs',
                'url': f'{SSRS_BASE}/ReportViewer.aspx?{UNSUB_REPORT}&AssetNbr={asset["asset_id"]}',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
            }

        if oac_ok:
            entry['capacity'] = {
                'status': 'captured',
                'access_method': 'weekly_auto',
                'platform': 'tc_energy_ssrs',
                'url': f'{SSRS_BASE}/ReportViewer.aspx?{OAC_REPORT}',
                'last_refreshed': TODAY,
                'refresh_frequency_days': 7,
                'cloud_accessible': True,
            }

        print(f"  Tracker updated: {tracker_name} (unsub={'OK' if unsub_ok else 'SKIP'}, oac={'OK' if oac_ok else 'SKIP'})")

    with open(TRACKER_FILE, 'w') as f:
        json.dump(tracker, f, indent=2)


# ============================================================
# MAIN
# ============================================================

def main():
    print(f"=== TC Energy OAC + Unsub Refresh: {TODAY} ===")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    results = []
    unsub_ok_count = 0
    oac_ok_count = 0

    for asset in ASSETS:
        name = asset['name']
        asset_id = asset['asset_id']
        folder = asset['folder']

        print(f"\n--- {name} (asset={asset_id}) ---")

        session = make_session()
        unsub_ok = False
        oac_ok = False

        # Unsub
        print("  Fetching Unsub...")
        try:
            content = fetch_unsub(session, asset_id)
            if content:
                path = os.path.join(OUTPUT_DIR, f'tce_{folder}_unsub.xls')
                with open(path, 'wb') as f:
                    f.write(content)
                unsub_ok = True
                unsub_ok_count += 1
        except Exception as e:
            print(f"    Unsub error: {e}")

        time.sleep(2)

        # OAC — needs fresh session for clean SSRS state
        print("  Fetching OAC...")
        oac_session = make_session()
        try:
            content = fetch_oac(oac_session, asset_id)
            if content:
                path = os.path.join(OUTPUT_DIR, f'tce_{folder}_oac.xlsx')
                with open(path, 'wb') as f:
                    f.write(content)
                oac_ok = True
                oac_ok_count += 1
        except Exception as e:
            print(f"    OAC error: {e}")

        results.append((asset, unsub_ok, oac_ok))
        time.sleep(3)

    # Summary
    print(f"\n--- Summary ---")
    print(f"  Unsub: {unsub_ok_count}/{len(ASSETS)} downloaded")
    print(f"  OAC:   {oac_ok_count}/{len(ASSETS)} downloaded")
    for asset, unsub_ok, oac_ok in results:
        status = f"{'U' if unsub_ok else '-'}{'O' if oac_ok else '-'}"
        print(f"  [{status}] {asset['name']}")

    # Update tracker
    update_tracker(results)

    print(f"\n=== Done — {unsub_ok_count} unsub, {oac_ok_count} oac ===")


if __name__ == '__main__':
    main()
