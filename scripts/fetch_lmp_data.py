#!/usr/bin/env python3
"""Fetch ERCOT nodal LMP data and geocode to substation coordinates."""
import json, os, sys, zipfile, io
from datetime import datetime, timedelta
from difflib import SequenceMatcher

import requests
import gridstatus
from gridstatus.base import Markets

def fetch_ercot_bus_mapping():
    """Download ERCOT Settlement Point to Electrical Bus mapping."""
    r = requests.get("https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=10008", timeout=15)
    docs = r.json().get('ListDocsByRptTypeRes', {}).get('DocumentList', [])
    if not docs:
        raise Exception("No bus mapping documents found")
    doc_id = docs[0].get('Document', {}).get('DocID', '')
    r2 = requests.get(f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}", timeout=30)
    z = zipfile.ZipFile(io.BytesIO(r2.content))
    
    # Find the CSV files
    rn_file = [n for n in z.namelist() if 'Resource_Node_to_Unit' in n][0]
    sp_file = [n for n in z.namelist() if 'Settlement_Points' in n][0]
    
    import csv
    rn_to_sub = {}
    with z.open(rn_file) as f:
        reader = csv.DictReader(io.TextIOWrapper(f))
        for row in reader:
            node = row.get('RESOURCE_NODE', '')
            sub = row.get('UNIT_SUBSTATION', '')
            if node and sub and node not in rn_to_sub:
                rn_to_sub[node] = sub
    
    sp_sub_zone = {}
    with z.open(sp_file) as f:
        reader = csv.DictReader(io.TextIOWrapper(f))
        for row in reader:
            sub = row.get('SUBSTATION', '')
            zone = row.get('SETTLEMENT_LOAD_ZONE', '')
            if sub and zone and sub not in sp_sub_zone:
                sp_sub_zone[sub] = zone
    
    return rn_to_sub, sp_sub_zone

def load_substation_coordinates():
    """Load HIFLD TX substations + capacity_screening substations."""
    geo = {}
    
    # HIFLD
    r = requests.get(
        "https://services6.arcgis.com/OO2s4OoyCZkYJ6oE/arcgis/rest/services/Substations/FeatureServer/0/query"
        "?where=STATE='TX'&outFields=NAME,MAX_VOLT,LATITUDE,LONGITUDE&returnGeometry=false&f=json&resultRecordCount=5000",
        timeout=15
    )
    for f in r.json().get('features', []):
        a = f['attributes']
        name = (a.get('NAME', '') or '').upper().strip()
        if name and a.get('LATITUDE') and a.get('LONGITUDE'):
            clean = name.replace(' ', '').replace('-', '').replace('.', '').replace("'", '')
            geo[clean] = {'lat': round(a['LATITUDE'], 4), 'lng': round(a['LONGITUDE'], 4), 'kv': a.get('MAX_VOLT', 0) or 0}
    
    # Capacity screening
    try:
        r2 = requests.get("https://app.thecorridor.io/data/capacity_screening.json", timeout=15)
        for s in r2.json():
            name = s.get('name', '').upper().strip()
            if name and s.get('lat') and s.get('lng'):
                clean = name.replace(' ', '').replace('-', '').replace('.', '').replace("'", '')
                if clean not in geo:
                    geo[clean] = {'lat': s['lat'], 'lng': s['lng'], 'kv': s.get('kv', 0)}
    except:
        pass
    
    return geo

def best_match(ercot_sub, geo):
    """Fuzzy match ERCOT substation name to geocoded substation."""
    ec = ercot_sub.upper().strip().replace(' ', '').replace('-', '').replace('.', '').replace("'", '').replace('_', '')
    
    if ec in geo:
        return geo[ec]
    
    ec_nonum = ec.rstrip('0123456789')
    if ec_nonum and ec_nonum in geo:
        return geo[ec_nonum]
    
    best = None
    best_len = 0
    for gk in geo:
        minlen = min(len(ec), len(gk))
        for i in range(minlen, 4, -1):
            if ec[:i] == gk[:i] and i > best_len:
                best_len = i
                best = geo[gk]
                break
    if best_len >= 5:
        return best
    
    best_ratio = 0
    for gk in geo:
        if abs(len(ec) - len(gk)) > 4:
            continue
        ratio = SequenceMatcher(None, ec, gk).ratio()
        if ratio > best_ratio and ratio > 0.7:
            best_ratio = ratio
            best = geo[gk]
    
    return best

def main():
    print("Fetching ERCOT bus mapping...")
    rn_to_sub, sp_sub_zone = fetch_ercot_bus_mapping()
    print(f"  Resource nodes: {len(rn_to_sub)}, Substations with zones: {len(sp_sub_zone)}")
    
    print("Loading substation coordinates...")
    geo = load_substation_coordinates()
    print(f"  Geocoded substations: {len(geo)}")
    
    print("Fetching 30-day ERCOT DAM SPP...")
    ercot = gridstatus.Ercot()
    end = datetime.now()
    start = end - timedelta(days=30)
    df = ercot.get_spp(date=start.date().isoformat(), end=end.date().isoformat(), market=Markets.DAY_AHEAD_HOURLY)
    print(f"  Rows: {len(df)}, Locations: {df['Location'].nunique()}")
    
    # Compute averages
    rn_df = df[df['Location Type'] == 'Resource Node']
    avg30 = rn_df.groupby('Location').agg(
        avg_30d=('SPP', 'mean'),
        min_30d=('SPP', 'min'),
        max_30d=('SPP', 'max'),
        std_30d=('SPP', 'std')
    ).reset_index()
    
    # Today's prices
    today_rn = df[(df['Location Type'] == 'Resource Node') & (df['Interval Start'].dt.date == end.date())]
    avg_today = today_rn.groupby('Location')['SPP'].mean().to_dict()
    
    # Geocode and build output
    results = []
    for _, row in avg30.iterrows():
        node = row['Location']
        sub = rn_to_sub.get(node)
        if not sub:
            continue
        coords = best_match(sub, geo)
        if not coords:
            continue
        
        today_price = avg_today.get(node)
        results.append({
            'node': node,
            'sub': sub,
            'lat': coords['lat'],
            'lng': coords['lng'],
            'kv': coords['kv'],
            'avg_30d': round(row['avg_30d'], 2),
            'min_30d': round(row['min_30d'], 2),
            'max_30d': round(row['max_30d'], 2),
            'std_30d': round(row['std_30d'], 2),
            'today': round(today_price, 2) if today_price else None,
            'zone': sp_sub_zone.get(sub, '')
        })
    
    output = {
        'date': end.strftime('%Y-%m-%d'),
        'period': f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}",
        'nodes': results
    }
    
    outpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'ercot_lmp.json')
    with open(outpath, 'w') as f:
        json.dump(output, f, separators=(',', ':'))
    
    print(f"\nOutput: {len(results)} nodes -> {outpath}")
    print(f"30d avg range: ${min(r['avg_30d'] for r in results):.2f} - ${max(r['avg_30d'] for r in results):.2f}")

if __name__ == '__main__':
    main()
