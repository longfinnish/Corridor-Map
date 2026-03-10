#!/usr/bin/env python3
"""Fetch ERCOT nodal LMP data and geocode to substation coordinates.

Sources for coordinate matching (in priority order):
1. HIFLD Substations (services6.arcgis.com) — ~2000 TX substations
2. Capacity screening data (app.thecorridor.io) — ~1600 additional
3. EIA-860 power plants (api.eia.gov) — ~900 TX plants with coords
4. Fuzzy matching against all three sources using PSSE bus names
"""
import json, os, sys, zipfile, io, csv
from datetime import datetime, timedelta
from difflib import SequenceMatcher

import requests
import gridstatus
from gridstatus.base import Markets


def clean_name(s):
    """Normalize a name for matching."""
    return s.upper().replace(' ', '').replace('-', '').replace('.', '').replace("'", '').replace('_', '').replace(',', '')


def fetch_ercot_bus_mapping():
    """Download ERCOT Settlement Point to Electrical Bus mapping."""
    r = requests.get("https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=10008", timeout=15)
    docs = r.json().get('ListDocsByRptTypeRes', {}).get('DocumentList', [])
    if not docs:
        raise Exception("No bus mapping documents found")
    doc_id = docs[0].get('Document', {}).get('DocID', '')
    r2 = requests.get(f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}", timeout=30)
    z = zipfile.ZipFile(io.BytesIO(r2.content))

    rn_file = [n for n in z.namelist() if 'Resource_Node_to_Unit' in n][0]
    sp_file = [n for n in z.namelist() if 'Settlement_Points' in n][0]

    rn_to_sub = {}
    with z.open(rn_file) as f:
        for row in csv.DictReader(io.TextIOWrapper(f)):
            node = row.get('RESOURCE_NODE', '')
            sub = row.get('UNIT_SUBSTATION', '')
            if node and sub and node not in rn_to_sub:
                rn_to_sub[node] = sub

    sp_data = {}
    with z.open(sp_file) as f:
        for row in csv.DictReader(io.TextIOWrapper(f)):
            sub = row.get('SUBSTATION', '')
            if sub and sub not in sp_data:
                sp_data[sub] = {
                    'psse': row.get('PSSE_BUS_NAME', ''),
                    'lz': row.get('SETTLEMENT_LOAD_ZONE', ''),
                    'kv': row.get('VOLTAGE_LEVEL', ''),
                    'bus': row.get('PSSE_BUS_NUMBER', '')
                }

    return rn_to_sub, sp_data


def load_coordinate_sources():
    """Load all coordinate sources: HIFLD substations, capacity screening, EIA plants."""
    subs = {}
    plants = {}

    # HIFLD TX substations
    try:
        r = requests.get(
            "https://services6.arcgis.com/OO2s4OoyCZkYJ6oE/arcgis/rest/services/Substations/FeatureServer/0/query"
            "?where=STATE='TX'&outFields=NAME,MAX_VOLT,LATITUDE,LONGITUDE&returnGeometry=false&f=json&resultRecordCount=5000",
            timeout=15
        )
        for f in r.json().get('features', []):
            a = f['attributes']
            name = (a.get('NAME', '') or '').upper().strip()
            if name and a.get('LATITUDE') and a.get('LONGITUDE'):
                c = clean_name(name).replace('SUBSTATION', '').replace('SWITCHINGSTATION', '').replace('STATION', '').strip()
                if c:
                    subs[c] = {'lat': round(a['LATITUDE'], 4), 'lng': round(a['LONGITUDE'], 4), 'kv': a.get('MAX_VOLT', 0) or 0}
        print(f"  HIFLD substations: {len(subs)}")
    except Exception as e:
        print(f"  HIFLD substations failed: {e}")

    # Capacity screening substations
    try:
        r2 = requests.get("https://app.thecorridor.io/data/capacity_screening.json", timeout=15)
        added = 0
        for s in r2.json():
            name = s.get('name', '').upper().strip()
            if name and s.get('lat') and s.get('lng'):
                c = clean_name(name)
                if c and c not in subs:
                    subs[c] = {'lat': s['lat'], 'lng': s['lng'], 'kv': s.get('kv', 0)}
                    added += 1
        print(f"  Capacity screening: +{added} (total {len(subs)})")
    except Exception as e:
        print(f"  Capacity screening failed: {e}")

    # EIA-860 TX power plants with coordinates
    try:
        r3 = requests.get("https://api.eia.gov/v2/electricity/operating-generator-capacity/data", params={
            "api_key": "DEMO_KEY",
            "frequency": "monthly",
            "data[0]": "latitude",
            "data[1]": "longitude",
            "data[2]": "nameplate-capacity-mw",
            "facets[stateid][]": "TX",
            "start": "2024-12",
            "end": "2024-12",
            "length": 5000,
        }, timeout=30)
        for rec in r3.json().get('response', {}).get('data', []):
            name = (rec.get('plantName', '') or '').upper().strip()
            lat = rec.get('latitude')
            lng = rec.get('longitude')
            if name and lat and lng:
                try:
                    lat_f, lng_f = float(lat), float(lng)
                except (ValueError, TypeError):
                    continue
                c = clean_name(name)
                if c and c not in plants:
                    plants[c] = {'lat': round(lat_f, 4), 'lng': round(lng_f, 4), 'raw': name}
        print(f"  EIA plants: {len(plants)}")
    except Exception as e:
        print(f"  EIA plants failed: {e}")

    return subs, plants


def best_match(ercot_sub, sp_data, subs, plants):
    """Multi-strategy coordinate matching for an ERCOT substation name."""
    ec = clean_name(ercot_sub)
    psse = clean_name(sp_data.get(ercot_sub, {}).get('psse', ''))

    # Build candidate names
    candidates = [ec]
    if psse:
        candidates.append(psse)
    for c in list(candidates):
        # Strip common ERCOT suffixes
        for sfx in ['SLR', 'WND', 'ESS', 'BESS', 'WIND', 'SOLAR', 'CC', 'GEN', 'STG', 'POI', 'RN', 'ALL']:
            if c.endswith(sfx) and len(c) > len(sfx) + 2:
                candidates.append(c[:-len(sfx)])
        stripped = c.rstrip('0123456789')
        if stripped and stripped != c:
            candidates.append(stripped)

    # 1. Direct match
    for c in candidates:
        if c in subs:
            return subs[c]
        if c in plants:
            return plants[c]

    # 2. Prefix match (6+ chars)
    for c in candidates:
        for n in range(min(len(c), 15), 5, -1):
            p = c[:n]
            for k in subs:
                if k.startswith(p) and abs(len(k) - len(c)) < 6:
                    return subs[k]
            for k in plants:
                if k.startswith(p) and abs(len(k) - len(c)) < 8:
                    return plants[k]

    # 3. Fuzzy match (>0.72)
    best_r, best_v = 0, None
    for c in candidates[:2]:
        for k in subs:
            if abs(len(k) - len(c)) > 6:
                continue
            ratio = SequenceMatcher(None, c, k).ratio()
            if ratio > best_r:
                best_r, best_v = ratio, subs[k]
        for k in plants:
            if abs(len(k) - len(c)) > 6:
                continue
            ratio = SequenceMatcher(None, c, k).ratio()
            if ratio > best_r:
                best_r, best_v = ratio, plants[k]
    if best_r > 0.72:
        return best_v

    return None


def main():
    print("Fetching ERCOT bus mapping...")
    rn_to_sub, sp_data = fetch_ercot_bus_mapping()
    print(f"  Resource nodes: {len(rn_to_sub)}, Settlement points: {len(sp_data)}")

    print("Loading coordinate sources...")
    subs, plants = load_coordinate_sources()

    print("Fetching 30-day ERCOT DAM SPP...")
    ercot = gridstatus.Ercot()
    end = datetime.now()
    start = end - timedelta(days=30)
    df = ercot.get_spp(date=start.date().isoformat(), end=end.date().isoformat(), market=Markets.DAY_AHEAD_HOURLY)
    print(f"  Rows: {len(df)}, Locations: {df['Location'].nunique()}")

    # Compute averages for resource nodes
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
    no_sub = 0
    no_coords = 0
    for _, row in avg30.iterrows():
        node = row['Location']
        sub = rn_to_sub.get(node)
        if not sub:
            no_sub += 1
            continue
        coords = best_match(sub, sp_data, subs, plants)
        if not coords:
            no_coords += 1
            continue

        sp_info = sp_data.get(sub, {})
        kv = coords.get('kv', 0)
        if not kv:
            try:
                kv_val = float(sp_info.get('kv', 0))
                if kv_val >= 69:
                    kv = kv_val
            except (ValueError, TypeError):
                pass

        today_price = avg_today.get(node)
        results.append({
            'node': node,
            'sub': sub,
            'lat': coords['lat'],
            'lng': coords['lng'],
            'kv': kv,
            'lz': sp_info.get('lz', ''),
            'avg_30d': round(row['avg_30d'], 2),
            'min_30d': round(row['min_30d'], 2),
            'max_30d': round(row['max_30d'], 2),
            'std_30d': round(row['std_30d'], 2),
            'today': round(today_price, 2) if today_price else None,
        })

    output = {
        'date': end.strftime('%Y-%m-%d'),
        'period': f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}",
        'nodes': results
    }

    outpath = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'ercot_lmp.json')
    with open(outpath, 'w') as f:
        json.dump(output, f, separators=(',', ':'))

    total_rn = len(avg30)
    print(f"\nOutput: {len(results)} nodes -> {outpath}")
    print(f"  No sub mapping: {no_sub}")
    print(f"  No coordinates: {no_coords}")
    print(f"  Coverage: {len(results)}/{total_rn} ({100*len(results)/total_rn:.0f}%)")
    if results:
        print(f"  30d avg range: ${min(r['avg_30d'] for r in results):.2f} - ${max(r['avg_30d'] for r in results):.2f}")


if __name__ == '__main__':
    main()
