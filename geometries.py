#!/usr/bin/env python3
"""
geometries.py

Builds precise per-source coverage geometries by fetching the actual
polygon data from Mapterhorn's MVT coverage tiles and unioning them
with Shapely.

Rather than approximating coverage with bounding boxes, this script
extracts the exact vector geometries from each coverage tile and merges
them into a single MultiPolygon per data source. The result is an
accurate representation of where each elevation source (e.g. USGS 3DEP,
GLO-30) contributes data across the globe.

This script:
  1. Reads tile_sources.json (output of scrape.py)
  2. Reads attribution.json for resolution metadata
  3. For each tile, re-fetches its MVT and extracts polygon geometries
  4. Converts MVT pixel coordinates to lon/lat
  5. Unions all geometries per source using Shapely
  6. Writes the result to source_geometries.json as GeoJSON-compatible
     geometry objects

Output (source_geometries.json):
  [
    {
      "source": "usgs3dep13",
      "resolution_m": 10.0,
      "geometry": {
        "type": "MultiPolygon",
        "coordinates": [...]
      }
    },
    ...
  ]

Usage:
  python geometries.py

Dependencies:
  pip install requests mapbox-vector-tile shapely

Note:
  Run scrape.py first to generate tile_sources.json.
"""

import json
import math
import sys
import requests
import mapbox_vector_tile
from shapely.geometry import shape
from shapely.ops import unary_union
from collections import defaultdict


def load_json(path):
    with open(path) as f:
        return json.load(f)


def tile_coords_to_lonlat(x_tile, y_tile, z, px, py, extent=4096):
    """Convert MVT pixel coordinates to lon/lat."""
    n = 2 ** z
    lon = (x_tile + px / extent) / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y_tile + py / extent) / n)))
    lat = math.degrees(lat_rad)
    return lon, lat


def get_coverage_geometries(z, x, y):
    """Fetch MVT tile and return list of (source, shapely_geometry) tuples."""
    url = f"https://single-archive-tiles.mapterhorn.com/coverage/{z}/{x}/{y}.mvt"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            return []

        decoded = mapbox_vector_tile.decode(res.content, y_coord_down=True)
        layer = decoded.get('coverage', {})
        results = []

        for feature in layer.get('features', []):
            src = feature['properties'].get('source')
            geom = feature['geometry']

            if geom['type'] == 'Polygon':
                rings = []
                for ring in geom['coordinates']:
                    rings.append([tile_coords_to_lonlat(x, y, z, px, py) for px, py in ring])
                geo = shape({'type': 'Polygon', 'coordinates': rings})

            elif geom['type'] == 'MultiPolygon':
                polys = []
                for poly in geom['coordinates']:
                    rings = []
                    for ring in poly:
                        rings.append([tile_coords_to_lonlat(x, y, z, px, py) for px, py in ring])
                    polys.append(rings)
                geo = shape({'type': 'MultiPolygon', 'coordinates': polys})

            else:
                continue

            if geo.is_valid:
                results.append((src, geo))
            else:
                results.append((src, geo.buffer(0)))  # fix any invalid geometries

        return results

    except Exception as e:
        print(f"  Error fetching {z}/{x}/{y}: {e}")
        return []


def main():
    print("Loading JSON files...")
    try:
        tiles = load_json('tile_sources.json')
        attribution = load_json('attribution.json')
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Make sure tile_sources.json (from scrape.py) and attribution.json are present.")
        sys.exit(1)

    source_to_resolution = {item['source']: item['resolution'] for item in attribution}
    source_geometries = defaultdict(list)

    print(f"Fetching geometries for {len(tiles)} tiles...\n")
    for i, tile in enumerate(tiles):
        z, x, y = tile['tile'].replace('.pmtiles', '').split('-')
        print(f"[{i+1}/{len(tiles)}] {tile['tile']}...", end=' ', flush=True)
        geometries = get_coverage_geometries(int(z), int(x), int(y))
        print(f"{len(geometries)} geometry/geometries found")
        for src, geom in geometries:
            source_geometries[src].append(geom)

    print(f"\nUnioning geometries per source...")
    output = []
    for src, geoms in sorted(source_geometries.items()):
        print(f"  {src}: unioning {len(geoms)} polygon(s)...")
        unioned = unary_union(geoms)
        output.append({
            'source': src,
            'resolution_m': source_to_resolution.get(src),
            'geometry': unioned.__geo_interface__
        })

    with open('source_geometries.json', 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nDone! {len(output)} sources written to source_geometries.json")


if __name__ == '__main__':
    main()