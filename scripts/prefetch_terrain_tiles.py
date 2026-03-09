"""
Pre-fetch slope and hillshade tiles from USGS 3DEP ImageServer.
Saves as static PNGs in TMS directory structure for Cloudflare R2 hosting.

Usage: python scripts/prefetch_terrain_tiles.py --type slope --min-zoom 6 --max-zoom 10
"""

import math
import os
import sys
import time
import argparse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# MISO + ERCOT + SPP coverage area
WEST, EAST = -106, -82
SOUTH, NORTH = 25, 49

USGS_URL = "https://elevation.nationalmap.gov/arcgis/rest/services/3DEPElevation/ImageServer/exportImage"

RENDERING_RULES = {
    'slope': '{"rasterFunction":"Slope Map"}',
    'hillshade': '{"rasterFunction":"Hillshade Gray"}',
}

TILE_SIZE = 256
MAX_RETRIES = 3
CONCURRENT = 4  # Be polite to USGS


def tile_bounds(x, y, z):
    """Convert tile coords to EPSG:4326 bounds."""
    n = 2 ** z
    lng_min = x / n * 360 - 180
    lng_max = (x + 1) / n * 360 - 180
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lng_min, lat_min, lng_max, lat_max


def get_tile_range(z):
    """Get x/y tile range for our coverage area at zoom level z."""
    n = 2 ** z
    x_min = int((WEST + 180) / 360 * n)
    x_max = int((EAST + 180) / 360 * n)
    y_min = int((1 - math.log(math.tan(math.radians(NORTH)) + 1 / math.cos(math.radians(NORTH))) / math.pi) / 2 * n)
    y_max = int((1 - math.log(math.tan(math.radians(SOUTH)) + 1 / math.cos(math.radians(SOUTH))) / math.pi) / 2 * n)
    return x_min, x_max, y_min, y_max


def fetch_tile(x, y, z, tile_type, out_dir):
    """Fetch a single tile from USGS and save as PNG."""
    path = os.path.join(out_dir, str(z), str(x), f"{y}.png")
    
    # Skip if already exists
    if os.path.exists(path) and os.path.getsize(path) > 100:
        return 'cached', 0
    
    lng_min, lat_min, lng_max, lat_max = tile_bounds(x, y, z)
    
    params = {
        'bbox': f"{lng_min},{lat_min},{lng_max},{lat_max}",
        'bboxSR': '4326',
        'imageSR': '4326',
        'size': f'{TILE_SIZE},{TILE_SIZE}',
        'format': 'png',
        'renderingRule': RENDERING_RULES[tile_type],
        'f': 'image',
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(USGS_URL, params=params, timeout=60)
            if r.status_code == 200 and len(r.content) > 100:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'wb') as f:
                    f.write(r.content)
                return 'ok', len(r.content)
            elif r.status_code in (502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            else:
                return 'error', r.status_code
        except (requests.Timeout, requests.ConnectionError):
            time.sleep(2 ** attempt)
            continue
    
    return 'failed', 0


def main():
    parser = argparse.ArgumentParser(description='Pre-fetch terrain tiles from USGS 3DEP')
    parser.add_argument('--type', choices=['slope', 'hillshade', 'both'], default='both')
    parser.add_argument('--min-zoom', type=int, default=6)
    parser.add_argument('--max-zoom', type=int, default=10)
    parser.add_argument('--out-dir', default='tiles')
    parser.add_argument('--concurrent', type=int, default=CONCURRENT)
    args = parser.parse_args()
    
    types = ['slope', 'hillshade'] if args.type == 'both' else [args.type]
    
    for tile_type in types:
        out_dir = os.path.join(args.out_dir, tile_type)
        
        # Count total tiles
        total = 0
        all_tiles = []
        for z in range(args.min_zoom, args.max_zoom + 1):
            x_min, x_max, y_min, y_max = get_tile_range(z)
            for x in range(x_min, x_max + 1):
                for y in range(y_min, y_max + 1):
                    all_tiles.append((x, y, z))
            count = (x_max - x_min + 1) * (y_max - y_min + 1)
            total += count
            print(f"  Zoom {z}: {count:,} tiles (x:{x_min}-{x_max}, y:{y_min}-{y_max})")
        
        print(f"\n{tile_type}: {total:,} total tiles to fetch")
        
        # Fetch with thread pool
        ok_count = 0
        cached_count = 0
        error_count = 0
        bytes_total = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=args.concurrent) as pool:
            futures = {
                pool.submit(fetch_tile, x, y, z, tile_type, out_dir): (x, y, z)
                for x, y, z in all_tiles
            }
            
            for i, future in enumerate(as_completed(futures)):
                status, size = future.result()
                if status == 'ok':
                    ok_count += 1
                    bytes_total += size
                elif status == 'cached':
                    cached_count += 1
                else:
                    error_count += 1
                
                if (i + 1) % 100 == 0 or i + 1 == total:
                    elapsed = time.time() - start_time
                    rate = (ok_count + cached_count) / max(elapsed, 1)
                    remaining = (total - i - 1) / max(rate, 0.1)
                    print(f"  [{i+1}/{total}] ok:{ok_count} cached:{cached_count} err:{error_count} "
                          f"({bytes_total/1024/1024:.1f} MB, {rate:.1f}/s, ~{remaining/60:.0f}min left)")
        
        elapsed = time.time() - start_time
        print(f"\n{tile_type} complete: {ok_count} fetched, {cached_count} cached, {error_count} errors")
        print(f"  {bytes_total/1024/1024:.1f} MB in {elapsed/60:.1f} minutes")


if __name__ == '__main__':
    main()
