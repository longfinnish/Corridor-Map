#!/usr/bin/env python3
"""Convert utility_territories.geojson to PMTiles."""
import json, os, math, gzip, warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

import mapbox_vector_tile as mvt
from shapely.geometry import shape, mapping, box
from pmtiles.writer import Writer as PMWriter
from pmtiles.tile import TileType, Compression, zxy_to_tileid

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
IN_GEOJSON = os.path.join(DATA_DIR, 'utility_territories.geojson')
OUT_PMTILES = os.path.join(DATA_DIR, 'utility_territories.pmtiles')

MIN_ZOOM = 0
MAX_ZOOM = 10

def lng_lat_to_tile(lng, lat, zoom):
    n = 2 ** zoom
    x = int((lng + 180) / 360 * n)
    lat_rad = math.radians(max(-85, min(85, lat)))
    y = int((1 - math.log(math.tan(lat_rad) + 1/math.cos(lat_rad)) / math.pi) / 2 * n)
    return max(0, min(n-1, x)), max(0, min(n-1, y))

def tile_bounds(x, y, z):
    n = 2 ** z
    lng1 = x / n * 360 - 180
    lng2 = (x + 1) / n * 360 - 180
    lat1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    lat2 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    return lng1, lat1, lng2, lat2

print('Loading GeoJSON...')
with open(IN_GEOJSON) as f:
    geojson = json.load(f)

print(f'Parsing {len(geojson["features"])} features...')
parsed = []
for feat in geojson['features']:
    try:
        geom = shape(feat['geometry'])
        if geom.is_valid and not geom.is_empty:
            parsed.append((geom, feat['properties']))
    except Exception:
        continue
print(f'{len(parsed)} valid geometries')

# Build tile index
print('Computing tile coverage...')
tiles = {}
for geom, props in parsed:
    bounds = geom.bounds
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        tx1, ty1 = lng_lat_to_tile(bounds[0], bounds[3], z)
        tx2, ty2 = lng_lat_to_tile(bounds[2], bounds[1], z)
        span = (tx2 - tx1 + 1) * (ty2 - ty1 + 1)
        if span > 4096:
            continue
        for tx in range(tx1, tx2 + 1):
            for ty in range(ty1, ty2 + 1):
                key = (z, tx, ty)
                if key not in tiles:
                    tiles[key] = []
                tiles[key].append((geom, props))

print(f'{len(tiles)} tiles to generate across z{MIN_ZOOM}-z{MAX_ZOOM}')

# Generate tiles
tile_data = {}
total = len(tiles)
done = 0
for (z, x, y), tile_features in tiles.items():
    bbox = tile_bounds(x, y, z)
    tile_box = box(bbox[0], bbox[1], bbox[2], bbox[3])
    mvt_features = []
    for geom, props in tile_features:
        try:
            clipped = geom.intersection(tile_box)
            if clipped.is_empty:
                continue
            if z < 8:
                tol = 360 / (2 ** z) / 4096 * 4
                clipped = clipped.simplify(tol, preserve_topology=True)
                if clipped.is_empty:
                    continue
            clean_props = {}
            for k, v in props.items():
                if v is None:
                    continue
                if isinstance(v, float):
                    if v != v:
                        continue
                    clean_props[k] = v
                elif isinstance(v, int):
                    clean_props[k] = v
                else:
                    clean_props[k] = str(v)
            mvt_features.append({
                'geometry': mapping(clipped),
                'properties': clean_props
            })
        except Exception:
            continue

    if mvt_features:
        try:
            tile_bytes = mvt.encode([{
                'name': 'utility_territories',
                'features': mvt_features
            }], quantize_bounds=(bbox[0], bbox[1], bbox[2], bbox[3]))
            tile_data[(z, x, y)] = gzip.compress(tile_bytes)
        except Exception:
            pass

    done += 1
    if done % 1000 == 0:
        print(f'  {done}/{total} tiles...')

print(f'Generated {len(tile_data)} non-empty tiles')

# Write PMTiles
print('Writing PMTiles...')
with open(OUT_PMTILES, 'wb') as f:
    writer = PMWriter(f)
    for (z, x, y), data in sorted(tile_data.items()):
        tileid = zxy_to_tileid(z, x, y)
        writer.write_tile(tileid, data)

    metadata = {
        'name': 'utility_territories',
        'description': 'US Electric Utility Service Territories (HIFLD + EIA 861 2024)',
        'format': 'pbf',
        'type': 'overlay',
        'minzoom': str(MIN_ZOOM),
        'maxzoom': str(MAX_ZOOM),
        'vector_layers': [{
            'id': 'utility_territories',
            'description': 'Electric utility service territory polygons',
            'fields': {
                'utility_id': 'Number', 'name': 'String', 'state': 'String',
                'type': 'String', 'holding_company': 'String', 'control_area': 'String',
                'nerc_region': 'String', 'regulated': 'String',
                'summer_peak_mw': 'Number', 'winter_peak_mw': 'Number',
                'net_generation_mwh': 'Number', 'total_purchases_mwh': 'Number',
                'total_customers': 'Number', 'website': 'String',
                'telephone': 'String', 'data_year': 'String'
            }
        }]
    }
    writer.finalize(
        header={
            'tile_type': TileType.MVT,
            'tile_compression': Compression.GZIP,
            'min_zoom': MIN_ZOOM,
            'max_zoom': MAX_ZOOM,
            'min_lon_e7': -1800000000,
            'min_lat_e7': -900000000,
            'max_lon_e7': 1800000000,
            'max_lat_e7': 900000000,
        },
        metadata=metadata
    )

size_mb = os.path.getsize(OUT_PMTILES) / 1024 / 1024
print(f'Done! {OUT_PMTILES} ({size_mb:.1f} MB)')
