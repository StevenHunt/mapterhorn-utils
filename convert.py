import json

with open('source_geometries.json') as f:
    data = json.load(f)

features = []
for item in data:
    features.append({
        'type': 'Feature',
        'properties': {
            'source': item['source'],
            'resolution_m': item['resolution_m'],
        },
        'geometry': item['geometry']
    })

# Sort worst → best so high-res renders on top
features.sort(key=lambda f: f['properties']['resolution_m'] or 9999, reverse=True)

geojson = {'type': 'FeatureCollection', 'features': features}

with open('source_geometries.geojson', 'w') as f:
    json.dump(geojson, f)

print(f"Done! {len(features)} features written to source_geometries.geojson")