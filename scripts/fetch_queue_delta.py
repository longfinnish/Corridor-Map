#!/usr/bin/env python3
"""Queue Delta Tracker — weekly snapshot, diff, AI analysis, email."""
import json, os, sys, hashlib
from datetime import datetime, timedelta

import requests
import gridstatus
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'data')
SNAPSHOT_DIR = os.path.join(DATA_DIR, 'queue_snapshots')

def ensure_dirs():
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)

def pull_ercot():
    print("Pulling ERCOT queue...")
    ercot = gridstatus.Ercot()
    df = ercot.get_interconnection_queue()
    records = []
    for _, r in df.iterrows():
        records.append({
            'id': r.get('Queue ID', ''),
            'name': r.get('Project Name', ''),
            'developer': r.get('Interconnecting Entity', ''),
            'county': r.get('County', ''),
            'state': r.get('State', 'TX'),
            'sub': r.get('Interconnection Location', ''),
            'tx_owner': r.get('Transmission Owner', ''),
            'mw': float(r.get('Capacity (MW)', 0) or 0),
            'fuel': r.get('Fuel', '') or r.get('Generation Type', ''),
            'status': r.get('Status', ''),
            'queue_date': str(r.get('Queue Date', ''))[:10],
            'proposed_completion': str(r.get('Proposed Completion Date', ''))[:10],
            'withdrawn_date': str(r.get('Withdrawn Date', ''))[:10] if pd.notna(r.get('Withdrawn Date')) else '',
            'iso': 'ERCOT'
        })
    print(f"  ERCOT: {len(records)} projects")
    return records

def pull_spp():
    print("Pulling SPP queue...")
    spp = gridstatus.SPP()
    df = spp.get_interconnection_queue()
    records = []
    for _, r in df.iterrows():
        records.append({
            'id': r.get('Queue ID', ''),
            'name': r.get('Project Name', ''),
            'developer': r.get('Interconnecting Entity', ''),
            'county': r.get('County', ''),
            'state': r.get('State', ''),
            'sub': r.get('Interconnection Location', ''),
            'tx_owner': r.get('Transmission Owner', ''),
            'mw': float(r.get('Capacity (MW)', 0) or 0),
            'fuel': r.get('Generation Type', ''),
            'status': r.get('Status', ''),
            'queue_date': str(r.get('Queue Date', ''))[:10],
            'proposed_completion': str(r.get('Proposed Completion Date', ''))[:10],
            'withdrawn_date': str(r.get('Withdrawn Date', ''))[:10] if pd.notna(r.get('Withdrawn Date')) else '',
            'iso': 'SPP'
        })
    print(f"  SPP: {len(records)} projects")
    return records

def pull_miso():
    print("Pulling MISO queue...")
    try:
        miso = gridstatus.MISO()
        df = miso.get_interconnection_queue()
        records = []
        for _, r in df.iterrows():
            records.append({
                'id': str(r.get('Queue ID', '')),
                'name': r.get('Project Name', ''),
                'developer': r.get('Interconnecting Entity', '') or r.get('Developer', ''),
                'county': r.get('County', ''),
                'state': r.get('State', ''),
                'sub': r.get('POI Location', '') or r.get('Interconnection Location', ''),
                'tx_owner': r.get('Transmission Owner', ''),
                'mw': float(r.get('Capacity (MW)', 0) or 0),
                'fuel': r.get('Fuel', '') or r.get('Generation Type', ''),
                'status': r.get('Status', ''),
                'queue_date': str(r.get('Queue Date', ''))[:10],
                'proposed_completion': str(r.get('Proposed Completion Date', ''))[:10] if pd.notna(r.get('Proposed Completion Date')) else '',
                'withdrawn_date': '',
                'iso': 'MISO'
            })
        print(f"  MISO: {len(records)} projects")
        return records
    except Exception as e:
        print(f"  MISO failed: {e}")
        return []

def save_snapshot(records, date_str):
    """Save this week's snapshot."""
    path = os.path.join(SNAPSHOT_DIR, f'queue_{date_str}.json')
    with open(path, 'w') as f:
        json.dump(records, f, separators=(',', ':'))
    print(f"Saved snapshot: {len(records)} projects -> {path}")
    return path

def load_previous_snapshot():
    """Load the most recent previous snapshot."""
    if not os.path.exists(SNAPSHOT_DIR):
        return None, None
    files = sorted([f for f in os.listdir(SNAPSHOT_DIR) if f.startswith('queue_') and f.endswith('.json')])
    if len(files) < 2:
        return None, None
    prev_file = files[-2]  # second to last
    with open(os.path.join(SNAPSHOT_DIR, prev_file)) as f:
        data = json.load(f)
    date = prev_file.replace('queue_', '').replace('.json', '')
    return data, date

def compute_delta(current, previous):
    """Compute additions, withdrawals, and status changes."""
    curr_ids = {r['id']: r for r in current}
    prev_ids = {r['id']: r for r in previous}

    additions = []
    withdrawals = []
    status_changes = []

    # New projects (in current but not previous)
    for pid, proj in curr_ids.items():
        if pid not in prev_ids:
            additions.append(proj)

    # Removed projects (in previous but not current)
    for pid, proj in prev_ids.items():
        if pid not in curr_ids:
            withdrawals.append(proj)

    # Status changes
    for pid in curr_ids:
        if pid in prev_ids:
            if curr_ids[pid]['status'] != prev_ids[pid]['status']:
                status_changes.append({
                    'project': curr_ids[pid],
                    'old_status': prev_ids[pid]['status'],
                    'new_status': curr_ids[pid]['status']
                })

    return additions, withdrawals, status_changes

def build_delta_report(additions, withdrawals, status_changes, current_date, prev_date):
    """Build the delta report JSON for the dashboard."""
    # Developer activity summary
    dev_adds = {}
    for p in additions:
        dev = p.get('developer', 'Unknown')
        if dev not in dev_adds:
            dev_adds[dev] = {'count': 0, 'mw': 0, 'projects': []}
        dev_adds[dev]['count'] += 1
        dev_adds[dev]['mw'] += p.get('mw', 0)
        dev_adds[dev]['projects'].append(p.get('name', ''))

    dev_drops = {}
    for p in withdrawals:
        dev = p.get('developer', 'Unknown')
        if dev not in dev_drops:
            dev_drops[dev] = {'count': 0, 'mw': 0, 'projects': []}
        dev_drops[dev]['count'] += 1
        dev_drops[dev]['mw'] += p.get('mw', 0)
        dev_drops[dev]['projects'].append(p.get('name', ''))

    # Substation activity
    sub_adds = {}
    for p in additions:
        sub = p.get('sub', 'Unknown')
        if sub not in sub_adds:
            sub_adds[sub] = {'count': 0, 'mw': 0}
        sub_adds[sub]['count'] += 1
        sub_adds[sub]['mw'] += p.get('mw', 0)

    sub_drops = {}
    for p in withdrawals:
        sub = p.get('sub', 'Unknown')
        if sub not in sub_drops:
            sub_drops[sub] = {'count': 0, 'mw': 0}
        sub_drops[sub]['count'] += 1
        sub_drops[sub]['mw'] += p.get('mw', 0)

    # ISO breakdown
    iso_summary = {}
    for p in additions:
        iso = p.get('iso', '?')
        if iso not in iso_summary:
            iso_summary[iso] = {'added': 0, 'added_mw': 0, 'dropped': 0, 'dropped_mw': 0}
        iso_summary[iso]['added'] += 1
        iso_summary[iso]['added_mw'] += p.get('mw', 0)
    for p in withdrawals:
        iso = p.get('iso', '?')
        if iso not in iso_summary:
            iso_summary[iso] = {'added': 0, 'added_mw': 0, 'dropped': 0, 'dropped_mw': 0}
        iso_summary[iso]['dropped'] += 1
        iso_summary[iso]['dropped_mw'] += p.get('mw', 0)

    report = {
        'current_date': current_date,
        'previous_date': prev_date,
        'summary': {
            'total_added': len(additions),
            'total_added_mw': round(sum(p.get('mw', 0) for p in additions), 1),
            'total_dropped': len(withdrawals),
            'total_dropped_mw': round(sum(p.get('mw', 0) for p in withdrawals), 1),
            'total_status_changes': len(status_changes),
        },
        'iso_breakdown': {k: {kk: round(vv, 1) if isinstance(vv, float) else vv for kk, vv in v.items()} for k, v in iso_summary.items()},
        'additions': sorted(additions, key=lambda x: x.get('mw', 0), reverse=True)[:50],
        'withdrawals': sorted(withdrawals, key=lambda x: x.get('mw', 0), reverse=True)[:50],
        'status_changes': status_changes[:30],
        'developer_activity': {
            'most_active_adders': sorted(dev_adds.items(), key=lambda x: x[1]['mw'], reverse=True)[:15],
            'most_active_droppers': sorted(dev_drops.items(), key=lambda x: x[1]['mw'], reverse=True)[:15],
        },
        'substation_activity': {
            'hottest_subs': sorted(sub_adds.items(), key=lambda x: x[1]['mw'], reverse=True)[:15],
            'clearing_subs': sorted(sub_drops.items(), key=lambda x: x[1]['mw'], reverse=True)[:15],
        }
    }
    return report

def generate_ai_narrative(report):
    """Use Claude API to generate narrative analysis."""
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        print("  No ANTHROPIC_API_KEY — skipping AI narrative")
        return None

    prompt = f"""Analyze this weekly interconnection queue delta report and write a concise intelligence briefing (3-4 paragraphs) for a data center site selection professional. Focus on:
1. Key takeaways — what matters most this week
2. Developer movements — who's adding/dropping and what it signals
3. Substation/geographic trends — where is competition increasing or decreasing
4. Implications for data center site selection

Report data:
- Period: {report['previous_date']} to {report['current_date']}
- Added: {report['summary']['total_added']} projects ({report['summary']['total_added_mw']} MW)
- Dropped: {report['summary']['total_dropped']} projects ({report['summary']['total_dropped_mw']} MW)
- ISO breakdown: {json.dumps(report['iso_breakdown'])}
- Top developers adding: {json.dumps(report['developer_activity']['most_active_adders'][:5])}
- Top developers dropping: {json.dumps(report['developer_activity']['most_active_droppers'][:5])}
- Hottest substations: {json.dumps(report['substation_activity']['hottest_subs'][:5])}
- Clearing substations: {json.dumps(report['substation_activity']['clearing_subs'][:5])}

Write in a direct, analytical tone. No fluff. Be specific about developer names and MW figures."""

    try:
        r = requests.post('https://api.anthropic.com/v1/messages', json={
            'model': 'claude-sonnet-4-20250514',
            'max_tokens': 1000,
            'messages': [{'role': 'user', 'content': prompt}]
        }, headers={
            'Content-Type': 'application/json',
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01'
        }, timeout=30)
        data = r.json()
        narrative = data.get('content', [{}])[0].get('text', '')
        print(f"  AI narrative: {len(narrative)} chars")
        return narrative
    except Exception as e:
        print(f"  AI narrative failed: {e}")
        return None

def main():
    ensure_dirs()
    today = datetime.now().strftime('%Y-%m-%d')

    # Pull from all ISOs
    records = []
    records.extend(pull_ercot())
    records.extend(pull_spp())
    records.extend(pull_miso())

    print(f"\nTotal: {len(records)} projects across all ISOs")

    # Save snapshot
    save_snapshot(records, today)

    # Load previous snapshot
    prev_data, prev_date = load_previous_snapshot()
    if not prev_data:
        print("No previous snapshot — first run. Delta report will be available next week.")
        # Still output empty delta
        report = {
            'current_date': today,
            'previous_date': None,
            'summary': {'total_added': 0, 'total_added_mw': 0, 'total_dropped': 0, 'total_dropped_mw': 0, 'total_status_changes': 0},
            'iso_breakdown': {},
            'additions': [],
            'withdrawals': [],
            'status_changes': [],
            'developer_activity': {'most_active_adders': [], 'most_active_droppers': []},
            'substation_activity': {'hottest_subs': [], 'clearing_subs': []},
            'narrative': 'First snapshot captured. Delta analysis will begin next week.',
            'total_projects': len(records)
        }
    else:
        # Compute delta
        additions, withdrawals, status_changes = compute_delta(records, prev_data)
        print(f"\nDelta: +{len(additions)} added, -{len(withdrawals)} dropped, {len(status_changes)} status changes")

        report = build_delta_report(additions, withdrawals, status_changes, today, prev_date)

        # AI narrative
        narrative = generate_ai_narrative(report)
        report['narrative'] = narrative or ''
        report['total_projects'] = len(records)

    # Save delta report
    delta_path = os.path.join(DATA_DIR, 'queue_delta.json')
    with open(delta_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\nDelta report saved: {delta_path}")

if __name__ == '__main__':
    main()
