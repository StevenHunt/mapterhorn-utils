
# Mapterhorn

Tools for analysing, attributing, visualising, and mirroring [Mapterhorn](https://mapterhorn.com) terrain PMTiles.

Mapterhorn distributes high-resolution global elevation data as PMTiles files. The dataset consists of one planet-wide base tile (`planet.pmtiles`, z0–12) plus 457 high-resolution regional tiles (z13–18) named by their zoom-6 grid coordinate (e.g. `6-14-26.pmtiles`). Total dataset size is ~10.81 TB.

---

## Pipeline

```
pmtiles.json + attribution.json  ──►  scrape.py     ──►  tile_sources.json
tile_sources.json + attribution.json  ──►  geometries.py  ──►  source_geometries.json
source_geometries.json  ──►  convert.py  ──►  source_geometries.geojson
```

| Script | Inputs | Output | Purpose |
|---|---|---|---|
| `scrape.py` | `pmtiles.json`, `attribution.json` | `tile_sources.json` | Fetches Mapterhorn's MVT coverage tiles and maps each z6 PMTiles file to its contributing elevation sources and resolutions |
| `geometries.py` | `tile_sources.json`, `attribution.json` | `source_geometries.json` | Re-fetches MVT tiles and unions precise per-source polygon geometries using Shapely |
| `convert.py` | `source_geometries.json` | `source_geometries.geojson` | Converts to GeoJSON, sorted worst→best resolution so high-res layers render on top |
| `analyze.py` | `pmtiles.json` | stdout | Prints a full dataset summary: file counts, total size, storage costs, zoom-level breakdown, geographic extent, largest tiles |
| `attribute.py` | `pmtiles.json`, `attribution.json` | stdout / `enriched_tiles.json` | Joins tiles with attribution data; shows license distribution and top contributing sources |
| `mirror.py` | `pmtiles.json` | Cloudflare R2 | Streams tiles from Mapterhorn → R2 via multipart upload (no temp disk). Designed to run on EC2 for throughput. |

---

## Data Files

| File | Description |
|---|---|
| `pmtiles.json` | Mapterhorn download manifest (v0.0.11, 458 items with URLs, bboxes, zoom ranges, sizes, MD5s) |
| `attribution.json` | Elevation data sources with resolutions (134 sources) |
| `tile_sources.json` | Output of `scrape.py` — per-tile source/resolution mapping |
| `source_geometries.json` | Output of `geometries.py` — per-source unioned polygon geometries |
| `source_geometries.geojson` | Output of `convert.py` — GeoJSON FeatureCollection ready for Mapbox |
| `enriched_tiles.json` | Output of `attribute.py` — tiles annotated with contributing sources |

---

## Visualisation of resolution 

View on Map: https://stevenhunt.github.io/mapterhorn-utils/

`index.html` renders `source_geometries.geojson` as a Mapbox GL JS map, colouring coverage polygons by resolution (1 m → 30 m+). Requires a Mapbox token in `config.js`.

---

## analyze.py

```bash
python analyze.py pmtiles.json
python analyze.py pmtiles.json --region -130 -60 24 50   # filter to a bounding box
```

Sample output:

```
Total files         458   /   10.81 TB
Planet base (z0–12) 705.73 GB  (R2 $10.59/mo)
High-res tiles      457 files  /  10.11 TB
Storage cost (full) R2 $162.17/mo   S3 $248.67/mo
```

---

## attribute.py

```bash
python attribute.py --downloads pmtiles.json --attribution attribution.json
python attribute.py --downloads pmtiles.json --attribution attribution.json --licenses
python attribute.py --fetch   # fetch manifests directly from Mapterhorn
```

---

## mirror.py

Streams each file from `https://download.mapterhorn.com` and uploads it directly to a Cloudflare R2 bucket via S3 multipart upload. Nothing touches local disk.

**Recommended setup:** EC2 `c5n.4xlarge` (25 Gbps, ~$0.86/hr) to avoid egress bottlenecks. Cost estimate for the full dataset: ~$90–100 in EC2 egress + ~$3–4 EC2 compute. R2 ingress is free.

### Configuration

Set via environment variables (recommended) or edit the constants at the top of the script:

```bash
export R2_ENDPOINT="https://<ACCOUNT_ID>.r2.cloudflarestorage.com"
export R2_ACCESS_KEY="..."
export R2_SECRET_KEY="..."
export R2_BUCKET="my-bucket"
```

### Usage

```bash
python mirror.py                  # full run
python mirror.py --dry-run        # preview without uploading
python mirror.py --limit 10       # upload only the 10 smallest files (good for testing)
python mirror.py --dry-run --limit 10
```

Files already present in R2 (size-verified) are skipped, so the run is fully resumable.

### EC2 Quick-Start

```bash
# 1. Launch c5n.4xlarge (Ubuntu 24.04, 20 GB storage), then SSH in
ssh -i key.pem ubuntu@<ec2-public-ip>

# 2. Install deps
sudo apt update && sudo apt install python3-pip tmux -y
pip3 install requests boto3 tqdm

# 3. Copy files
scp -i key.pem mirror.py pmtiles.json ubuntu@<ec2-public-ip>:~/

# 4. Set credentials and run inside tmux
tmux new -s mirror
export R2_ENDPOINT="..." R2_ACCESS_KEY="..." R2_SECRET_KEY="..." R2_BUCKET="..."
python3 mirror.py
# Detach: Ctrl+B then D  |  Reattach: tmux attach -t mirror
```

---

## Dependencies

```bash
pip install requests boto3 tqdm mapbox-vector-tile shapely
```

