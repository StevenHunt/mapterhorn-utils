import requests
import mapbox_vector_tile
import json
import time

"""
scrape.py

Generates a dataset mapping each Mapterhorn z6 PMTiles file to its
underlying elevation data sources and their resolutions.

Mapterhorn distributes high-resolution terrain tiles as PMTiles files
named by their zoom/x/y grid coordinate (e.g. 6-12-25.pmtiles). Each
file covers a distinct geographic bounding box at zoom level 6. Behind
the scenes, Mapterhorn's coverage map uses a vector tile endpoint to
record which data source(s) (e.g. USGS 3DEP, GLO-30) contributed
elevation data to each grid square, and at what resolution.

This script:
  1. Reads pmtiles.json (list of all downloadable tile files + bboxes)
  2. Reads attribution.json (list of data sources + resolutions in meters)
  3. For each z6 tile, fetches its coverage MVT from:
       https://single-archive-tiles.mapterhorn.com/coverage/{z}/{x}/{y}.mvt
  4. Decodes the binary vector tile and extracts the source(s) present
  5. Joins each source to its resolution from attribution.json
  6. Writes the result to tile_sources.json, with sources sorted
     by resolution ascending (best first)

Output (tile_sources.json):
  [
    {
      "tile": "6-12-25.pmtiles",
      "url": "https://download.mapterhorn.com/6-12-25.pmtiles",
      "bbox": { "min_lon": ..., "min_lat": ..., "max_lon": ..., "max_lat": ... },
      "sources": [
        { "source": "us1fa",      "resolution_m": 1.0  },
        { "source": "usgs3dep13", "resolution_m": 10.0 },
        { "source": "glo30",      "resolution_m": 30.0 }
      ]
    },
    ...
  ]

Dependencies:
  pip install requests mapbox-vector-tile
"""


def load_json(path):
    with open(path) as f:
        return json.load(f)

def get_sources_for_tile(z, x, y):
    url = f"https://single-archive-tiles.mapterhorn.com/coverage/{z}/{x}/{y}.mvt"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            return []
        decoded = mapbox_vector_tile.decode(res.content)
        layer = decoded.get('coverage', {})
        sources = set()
        for feature in layer.get('features', []):
            src = feature['properties'].get('source')
            if src:
                sources.add(src)
        return list(sources)
    except Exception as e:
        print(f"  Error fetching {z}/{x}/{y}: {e}")
        return []

def main():
    print("Loading JSON files...")
    attribution = load_json('attribution.json')
    downloads_data = load_json('pmtiles.json')

    source_to_resolution = {item['source']: item['resolution'] for item in attribution}

    tiles = [
        item for item in downloads_data['items']
        if item['name'] != 'planet.pmtiles'
    ]
    print(f"Found {len(tiles)} tiles to process.")

    results = []
    for i, item in enumerate(tiles):
        z, x, y = item['name'].replace('.pmtiles', '').split('-')
        z, x, y = int(z), int(x), int(y)

        print(f"[{i+1}/{len(tiles)}] {item['name']}...", end=' ', flush=True)
        sources = get_sources_for_tile(z, x, y)
        print(f"{len(sources)} source(s) found")

        results.append({
            'tile': item['name'],
            'url': item['url'],
            'bbox': {
                'min_lon': item['min_lon'],
                'min_lat': item['min_lat'],
                'max_lon': item['max_lon'],
                'max_lat': item['max_lat'],
            },
            'sources': [
                {
                    'source': src,
                    'resolution_m': source_to_resolution.get(src)
                }
                for src in sorted(sources, key=lambda s: source_to_resolution.get(s, 9999))
            ]
        })

        time.sleep(0.1)  # be polite to their server

    with open('tile_sources.json', 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nDone! Results written to tile_sources.json")

if __name__ == '__main__':
    main()