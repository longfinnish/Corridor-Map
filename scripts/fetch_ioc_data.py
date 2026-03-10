#!/usr/bin/env python3
"""
Corridor IOC (Index of Customers) Refresh Script
Pulls contract data from pipeline EBB portals, normalizes, outputs ioc_contracts.json

Sources:
  - Energy Transfer iPost: PEPL, TGC, FGT, TW, TGR, SPC
  - Enbridge Link: TE (Texas Eastern), SESH
  
Not automated (browser required):
  - Kinder Morgan: EPNG, TGP, NGPL, SNG, CIG (ASP.NET tree, no API)
  - TC Energy: ANR, Columbia Gas, Columbia Gulf (complex EBB)
  - Northern Border, Rover (separate portals)

Usage:
  python scripts/fetch_ioc_data.py
  
Output:
  data/ioc_contracts.json
"""

import requests
import csv
import io
import json
import os
import sys
import time
from datetime import datetime

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})


def fetch_et_ioc(asset, base_url, name):
    """Fetch IOC from Energy Transfer iPost portal."""
    url = f"{base_url}/index-of-customers/index?asset={asset}&f=csv&extension=csv"
    try:
        r = SESSION.get(url, timeout=30)
        if r.status_code != 200 or len(r.text) < 100:
            print(f"  WARN: {name} returned {r.status_code}")
            return []
        
        reader = csv.DictReader(io.StringIO(r.text))
        contracts = []
        
        for row in reader:
            shipper = (row.get('Shipper Name', '') or '').strip()
            end_date = (row.get('Contract Expiration Date', '') or '').strip()
            begin_date = (row.get('Contract Effective Date', '') or '').strip()
            mdq_raw = (row.get('Max Daily Quantity', '') or '').strip().replace(',', '')
            mdq = int(mdq_raw) if mdq_raw and mdq_raw.isdigit() else 0
            
            contracts.append({
                'shipper': shipper,
                'contract': (row.get('Contract Number', '') or '').strip(),
                'rate_schedule': (row.get('Rate Schedule', '') or '').strip(),
                'begin': begin_date,
                'end': end_date,
                'mdq': mdq,
                'negotiated': (row.get('Neg Rate Ind', '') or '').strip(),
            })
        
        print(f"  {name}: {len(contracts)} contracts")
        return contracts
    
    except Exception as e:
        print(f"  ERROR: {name}: {e}")
        return []


def fetch_enbridge_ioc(pipe_code, name):
    """Fetch IOC from Enbridge Link CSV download."""
    url = f"https://infopost.enbridge.com/Downloads/IOC/{pipe_code}_IOC.csv"
    try:
        r = SESSION.get(url, timeout=30)
        if r.status_code != 200 or len(r.text) < 200:
            print(f"  WARN: {name} returned {r.status_code}")
            return []
        
        lines = r.text.strip().split('\n')
        contracts = []
        current = None
        
        for line in lines:
            parts = [p.strip().strip('"') for p in line.split('","')]
            row_type = parts[0].strip('"')
            
            if row_type == 'D' and len(parts) >= 8:
                transport_mdq = 0
                if len(parts) > 10:
                    raw = parts[10].replace(',', '').strip()
                    transport_mdq = int(raw) if raw and raw.isdigit() else 0
                
                current = {
                    'shipper': parts[1],
                    'contract': parts[5],
                    'rate_schedule': parts[4],
                    'begin': parts[6],
                    'end': parts[7],
                    'mdq': transport_mdq,
                    'negotiated': parts[3] if len(parts) > 3 else '',
                    'points': []
                }
                contracts.append(current)
            
            elif row_type == 'P' and current and len(parts) >= 5:
                current['points'].append({
                    'name': parts[2],
                    'id': parts[4],
                    'type': parts[1],
                })
        
        print(f"  {name}: {len(contracts)} contracts")
        return contracts
    
    except Exception as e:
        print(f"  ERROR: {name}: {e}")
        return []


def classify_contract(end_date):
    """Classify contract status based on expiration date."""
    if not end_date:
        return 'unknown'
    try:
        parts = end_date.split('/')
        if len(parts) == 3:
            year = int(parts[2])
            if year < 2026:
                return 'expired'
            elif year <= 2027:
                return 'expiring_soon'
            else:
                return 'active'
    except:
        pass
    return 'unknown'


def build_output(all_data):
    """Normalize all IOC data into deployment-ready format."""
    output = {
        'generated': datetime.now().strftime('%Y-%m-%d'),
        'sources': {},
        'pipelines': {},
        'summary': {}
    }
    
    total_contracts = 0
    total_expiring = 0
    total_expiring_mdq = 0
    
    for pipe_name, (source, contracts) in all_data.items():
        output['sources'][pipe_name] = source
        
        pipe_contracts = []
        for c in contracts:
            status = classify_contract(c.get('end', ''))
            entry = {
                'shipper': c.get('shipper', '').strip(),
                'contract': c.get('contract', '').strip(),
                'rate_schedule': c.get('rate_schedule', '').strip(),
                'begin': c.get('begin', ''),
                'end': c.get('end', ''),
                'mdq': c.get('mdq', 0),
                'negotiated': c.get('negotiated', ''),
                'status': status,
            }
            if 'points' in c and c['points']:
                entry['points'] = c['points'][:5]
            pipe_contracts.append(entry)
        
        pipe_contracts.sort(key=lambda c: c.get('end', '9999'))
        
        expiring = [c for c in pipe_contracts if c['status'] == 'expiring_soon']
        
        output['pipelines'][pipe_name] = {
            'total_contracts': len(pipe_contracts),
            'expiring_count': len(expiring),
            'expiring_mdq': sum(c['mdq'] for c in expiring),
            'contracts': pipe_contracts,
        }
        
        total_contracts += len(pipe_contracts)
        total_expiring += len(expiring)
        total_expiring_mdq += sum(c['mdq'] for c in expiring)
    
    output['summary'] = {
        'total_contracts': total_contracts,
        'total_pipelines': len(output['pipelines']),
        'expiring_2026_2027': total_expiring,
        'expiring_mdq_dth': total_expiring_mdq,
    }
    
    return output


def main():
    print(f"IOC Refresh - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)
    
    all_data = {}
    
    # Energy Transfer iPost pipelines
    print("\nEnergy Transfer (iPost):")
    et_pipes = [
        ('PEPL', 'https://peplmessenger.energytransfer.com/ipost', 'Panhandle Eastern'),
        ('TGC', 'https://tgcmessenger.energytransfer.com/ipost', 'Trunkline'),
        ('FGT', 'https://fgttransfer.energytransfer.com/ipost', 'Florida Gas'),
        ('TW', 'https://twtransfer.energytransfer.com/ipost', 'Transwestern'),
        ('TGR', 'https://tigertransfer.energytransfer.com/ipost', 'Tiger'),
        ('SPC', 'https://spcmessenger.energytransfer.com/ipost', 'Sea Robin'),
        ('ROVER', 'https://pipelines.energytransfer.com/ipost', 'Rover Pipeline'),
        ('MRT', 'https://pipelines.energytransfer.com/ipost', 'Enable MRT'),
    ]
    
    for asset, base, name in et_pipes:
        contracts = fetch_et_ioc(asset, base, name)
        all_data[name] = (f'iPost/{asset}', contracts)
        time.sleep(0.5)
    
    # Enbridge Link pipelines
    print("\nEnbridge Link:")
    enbridge_pipes = [
        ('TE', 'Texas Eastern'),
        ('SESH', 'Southeast Supply Header'),
    ]
    
    for code, name in enbridge_pipes:
        contracts = fetch_enbridge_ioc(code, name)
        all_data[name] = (f'Enbridge/{code}', contracts)
        time.sleep(0.5)
    
    # Build output
    print("\nBuilding output...")
    output = build_output(all_data)
    
    # Write to data directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    output_path = os.path.join(repo_root, 'data', 'ioc_contracts.json')
    
    with open(output_path, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    
    size = os.path.getsize(output_path)
    print(f"\nOutput: {output_path} ({size/1024:.0f} KB)")
    print(f"Summary: {json.dumps(output['summary'], indent=2)}")
    
    # Show top expiring contracts
    print(f"\nTop expiring contracts (2026-2027):")
    all_expiring = []
    for pipe, pdata in output['pipelines'].items():
        for c in pdata['contracts']:
            if c['status'] == 'expiring_soon':
                all_expiring.append({**c, 'pipeline': pipe})
    
    all_expiring.sort(key=lambda c: -c['mdq'])
    for c in all_expiring[:15]:
        print(f"  {c['pipeline']:20s}  {c['shipper'][:35]:35s}  MDQ={c['mdq']:>9,}  exp={c['end']}")


if __name__ == '__main__':
    main()

