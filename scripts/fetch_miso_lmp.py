#!/usr/bin/env python3
"""Fetch MISO nodal LMP data and geocode to substation coordinates.

TEMPORARY: ~16-25% node coverage due to MISO's abbreviated internal naming.
Full coverage requires FERC CEII PSS/E bus models (request submitted 2026-03-05,
follow up with Matthew Nutter on 2026-03-12). MRO region covers MISO footprint.

Sources for coordinate matching:
1. HIFLD Substations across 17 MISO states
2. EIA-860 power plants
3. Fuzzy/prefix matching with owner-to-state narrowing
"""
import json, os, sys, csv, io
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import requests

# MISO utility owner codes -> primary states
OWNER_STATES = {
    'AECI':['MO','IA'],'ALTE':['WI'],'ALTW':['IA','MN'],'AMIL':['IL'],
    'AMMO':['MO'],'BREC':['KY'],'CIN':['IN','OH'],'CLEC':['LA'],
    'CONS':['MI'],'CWLP':['IL'],'DECO':['MI'],'DPC':['IN'],
    'EAI':['AR'],'EES':['LA','TX','MS'],'EMBA':['MN'],'GRE':['MN'],
    'HE':['MN'],'IPL':['IN'],'KCPL':['MO','KS'],'LAFA':['LA'],
    'LAGN':['LA'],'LEPA':['LA'],'LIND':['MI'],'MDU':['ND','SD','MT'],
    'MEC':['IA'],'MGE':['WI'],'MIPU':['MO'],'MPW':['IA'],
    'NIPS':['IN'],'NSP':['MN','WI'],'OTP':['MN','ND','SD'],
    'SIGE':['IN'],'SIPC':['IL'],'SME':['AR'],'SMP':['AR','MO'],
    'SPA':['AR'],'UPPC':['MI'],'WEC':['WI'],'WPS':['WI'],'WR':['KS'],
}

MISO_STATES = sorted(set(s for states in OWNER_STATES.values() for s in states))


def clean(s):
    return s.upper().replace(' ','').replace('-','').replace('.','').replace("'",'').replace('_','').replace(',','')


def load_coordinate_sources():
    """Load HIFLD substations for all MISO states + EIA plants."""
    subs = {}
    
    # HIFLD substations for each MISO state
    for state in MISO_STATES:
        try:
            r = requests.get(
                f"https://services6.arcgis.com/OO2s4OoyCZkYJ6oE/arcgis/rest/services/Substations/FeatureServer/0/query"
                f"?where=STATE='{state}'&outFields=NAME,MAX_VOLT,LATITUDE,LONGITUDE,STATE&returnGeometry=false&f=json&resultRecordCount=5000",
                timeout=15
            )
            for f in r.json().get('features', []):
                a = f['attributes']
                name = (a.get('NAME','') or '').upper().strip()
                st = a.get('STATE','')
                if name and a.get('LATITUDE') and a.get('LONGITUDE'):
                    c = clean(name).replace('SUBSTATION','').replace('SWITCHINGSTATION','').replace('STATION','').strip()
                    if not c:
                        continue
                    entry = {'lat':round(a['LATITUDE'],4),'lng':round(a['LONGITUDE'],4),'kv':a.get('MAX_VOLT',0) or 0,'state':st}
                    subs[f"{st}_{c}"] = entry
                    if c not in subs:
                        subs[c] = entry
        except Exception as e:
            print(f"  HIFLD {state} failed: {e}")
    
    print(f"  HIFLD substations: {len(subs)}")
    
    # EIA plants for MISO states
    plants = {}
    for state in MISO_STATES:
        try:
            r = requests.get("https://api.eia.gov/v2/electricity/operating-generator-capacity/data", params={
                "api_key": "DEMO_KEY",
                "frequency": "monthly",
                "data[0]": "latitude",
                "data[1]": "longitude",
                "facets[stateid][]": state,
                "start": "2024-12",
                "end": "2024-12",
                "length": 5000,
            }, timeout=15)
            for rec in r.json().get('response', {}).get('data', []):
                name = (rec.get('plantName','') or '').upper().strip()
                lat, lng = rec.get('latitude'), rec.get('longitude')
                if name and lat and lng:
                    try:
                        lat_f, lng_f = float(lat), float(lng)
                    except:
                        continue
                    c = clean(name)
                    if c and c not in plants:
                        plants[c] = {'lat':round(lat_f,4),'lng':round(lng_f,4),'state':state}
        except:
            pass
    
    print(f"  EIA plants: {len(plants)}")
    return subs, plants


def find_match(node, subs, plants):
    """Match a MISO node name to coordinates."""
    parts = node.split('.')
    owner = parts[0]
    sub_name = parts[1] if len(parts) > 1 else ''
    if not sub_name:
        return None
    
    states = OWNER_STATES.get(owner, [])
    sc = clean(sub_name)
    
    # Build candidates
    candidates = [sc]
    for sfx in ['G1','G2','G3','G4','G5','S1','S2','S3','W1','W2','W3','W4',
                 'B1','B2','LD','ARR','AZ','ESR','BESS','SLR','WND','CC','ST',
                 'CT','GT','UN1','UN2','UN3','UN4']:
        if sc.endswith(sfx) and len(sc) > len(sfx) + 2:
            candidates.append(sc[:-len(sfx)])
    stripped = sc.rstrip('0123456789')
    if stripped and stripped != sc:
        candidates.append(stripped)
    
    # 1. State-specific direct match
    for st in states:
        for c in candidates:
            key = f"{st}_{c}"
            if key in subs:
                return subs[key]
    
    # 2. Any-state direct match
    for c in candidates:
        if c in subs:
            return subs[c]
        if c in plants:
            return plants[c]
    
    # 3. Prefix match (6+) within owner states
    for st in states:
        for c in candidates:
            for n in range(min(len(c), 12), 5, -1):
                p = c[:n]
                for k in subs:
                    if k.startswith(f"{st}_{p}") and abs(len(k) - len(f"{st}_{c}")) < 6:
                        return subs[k]
    
    # 4. Prefix match against plants
    for c in candidates:
        for n in range(min(len(c), 12), 5, -1):
            p = c[:n]
            for k in plants:
                if k.startswith(p) and abs(len(k) - len(c)) < 8:
                    return plants[k]
    
    # 5. Fuzzy (>0.75)
    best_r, best_v = 0, None
    for c in candidates[:2]:
        for st in states:
            for k in subs:
                if not k.startswith(f"{st}_"):
                    continue
                sk = k[len(f"{st}_"):]
                if abs(len(sk) - len(c)) > 5:
                    continue
                ratio = SequenceMatcher(None, c, sk).ratio()
                if ratio > best_r:
                    best_r, best_v = ratio, subs[k]
    if best_r > 0.75:
        return best_v
    
    return None


def fetch_miso_lmp_30d():
    """Fetch 30 days of MISO DAM LMP data from CSV reports."""
    end = datetime.now()
    start = end - timedelta(days=30)
    
    all_prices = {}  # node -> list of daily averages
    days_loaded = 0
    
    current = start
    while current <= end:
        date_str = current.strftime('%Y%m%d')
        url = f"https://docs.misoenergy.org/marketreports/{date_str}_da_expost_lmp.csv"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                current += timedelta(days=1)
                continue
            
            lines = r.text.strip().split('\n')
            data_start = next((i for i, l in enumerate(lines) if l.startswith('Node,Type,Value')), None)
            if data_start is None:
                current += timedelta(days=1)
                continue
            
            reader = csv.reader(io.StringIO('\n'.join(lines[data_start:])))
            next(reader)  # skip header
            
            for row in reader:
                if len(row) < 5:
                    continue
                node, ntype, vtype = row[0].strip(), row[1].strip(), row[2].strip()
                if not node or vtype != 'LMP':
                    continue
                prices = []
                for h in row[3:27]:
                    try:
                        prices.append(float(h))
                    except:
                        pass
                if prices:
                    if node not in all_prices:
                        all_prices[node] = {'type': ntype, 'daily_avgs': []}
                    all_prices[node]['daily_avgs'].append(sum(prices) / len(prices))
            
            days_loaded += 1
        except:
            pass
        current += timedelta(days=1)
    
    print(f"  Loaded {days_loaded} days of MISO DAM LMP data")
    print(f"  Nodes with prices: {len(all_prices)}")
    return all_prices, start, end


def main():
    print("Loading coordinate sources for MISO states...")
    subs, plants = load_coordinate_sources()
    
    print("Fetching 30-day MISO DAM LMP...")
    all_prices, start, end = fetch_miso_lmp_30d()
    
    # Build output for Gennodes only (Loadzones/Hubs don't have substation coords)
    print("Geocoding nodes...")
    results = []
    no_match = 0
    
    for node, data in all_prices.items():
        if data['type'] != 'Gennode':
            continue
        
        coords = find_match(node, subs, plants)
        if not coords:
            no_match += 1
            continue
        
        daily = data['daily_avgs']
        avg_30d = sum(daily) / len(daily)
        min_30d = min(daily)
        max_30d = max(daily)
        std_30d = (sum((x - avg_30d)**2 for x in daily) / len(daily))**0.5 if len(daily) > 1 else 0
        today_avg = daily[-1] if daily else None
        
        parts = node.split('.')
        owner = parts[0]
        sub_name = parts[1] if len(parts) > 1 else node
        
        results.append({
            'node': node,
            'sub': sub_name,
            'owner': owner,
            'lat': coords['lat'],
            'lng': coords['lng'],
            'kv': coords.get('kv', 0),
            'state': coords.get('state', ''),
            'avg_30d': round(avg_30d, 2),
            'min_30d': round(min_30d, 2),
            'max_30d': round(max_30d, 2),
            'std_30d': round(std_30d, 2),
            'today': round(today_avg, 2) if today_avg else None,
        })
    
    total_gen = sum(1 for d in all_prices.values() if d['type'] == 'Gennode')
    
    output = {
        'iso': 'MISO',
        'date': end.strftime('%Y-%m-%d'),
        'period': f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}",
        '_note': 'TEMPORARY: ~20% coverage. Full coverage pending FERC CEII PSS/E models (submitted 2026-03-05)',
        'nodes': results
    }
    
    outpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'miso_lmp.json')
    with open(outpath, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    
    print(f"\nOutput: {len(results)} nodes -> {outpath}")
    print(f"  Gennodes total: {total_gen}")
    print(f"  No coordinates: {no_match}")
    print(f"  Coverage: {len(results)}/{total_gen} ({100*len(results)/total_gen:.0f}%)")
    if results:
        print(f"  30d avg range: ${min(r['avg_30d'] for r in results):.2f} - ${max(r['avg_30d'] for r in results):.2f}")


if __name__ == '__main__':
    main()
