#!/usr/bin/env python3
"""
Mapterhorn PMTiles Dataset Analyzer
Usage: python analyze_pmtiles.py <path_to_json> [--region min_lon max_lon min_lat max_lat]

Examples:
  python analyze_pmtiles.py mapterhorn.json
  python analyze_pmtiles.py mapterhorn.json --region -130 -60 24 50

# ============================================================================================
(tiler) ➜  mapterhorn python analyze.py pmtiles.json                                                        

Mapterhorn PMTiles Analyzer
Version: 0.0.11
Source:  pmtiles.json  (458 items)

────────────────────────────────────────────────────────────
  SUMMARY
────────────────────────────────────────────────────────────
  Total files                    458
  Total size                     10.81 TB
  Planet base (z0–12)            705.73 GB  (R2 $10.59/mo  S3 $16.23/mo)
  High-res tiles (z13+)          457 files  /  10.11 TB
  
  Avg hi-res tile size           22.11 GB
  Storage cost (full)            R2 $162.17/mo   S3 $248.67/mo
  Storage cost (hi-res only)     R2 $151.59/mo   S3 $232.44/mo

  Note: prices are storage only. Egress is additional.
        R2 $0.015/GB · S3 $0.023/GB (us-east-1 standard)

────────────────────────────────────────────────────────────
  BREAKDOWN BY ZOOM LEVEL
────────────────────────────────────────────────────────────
  Zoom             Files          Size       %  Distribution
  ────────────── ───────  ────────────  ──────  ──────────────────────────────
  z0–12                1     705.73 GB    6.5%  ██░░░░░░░░░░░░░░░░░░░░░░░░░░
                          R2 $10.59/mo     S3 $16.23/mo
  z13                157      38.25 GB    0.4%  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░
                          R2 $0.57/mo      S3 $0.88/mo
  z13–14             108     191.62 GB    1.8%  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░
                          R2 $2.87/mo      S3 $4.41/mo
  z13–16             101       5.40 TB   50.0%  ██████████████░░░░░░░░░░░░░░
                          R2 $81.06/mo     S3 $124.29/mo
  z13–15              49     445.29 GB    4.1%  █░░░░░░░░░░░░░░░░░░░░░░░░░░░
                          R2 $6.68/mo      S3 $10.24/mo
  z13–17              37       3.51 TB   32.5%  █████████░░░░░░░░░░░░░░░░░░░
                          R2 $52.63/mo     S3 $80.70/mo
  z13–18               5     518.05 GB    4.8%  █░░░░░░░░░░░░░░░░░░░░░░░░░░░
                          R2 $7.77/mo      S3 $11.92/mo

────────────────────────────────────────────────────────────
  GROUND RESOLUTION BY ZOOM LEVEL
────────────────────────────────────────────────────────────
  Zoom            Equator           45°N           60°N   Context
  ──────── ────────────── ────────────── ──────────────   ────────────────────────────
  0            156543.0m      110692.6m       78271.5m   world overview
  12               38.2m          27.0m          19.1m   streets (planet max)
  13               19.1m          13.5m           9.6m   buildings / hi-res tiles
  14                9.6m           6.8m           4.8m   parcel level
  15                4.8m           3.4m           2.4m   
  16                2.4m           1.7m           1.2m   
  17                1.2m           0.8m           0.6m   
  18                0.6m           0.4m           0.3m   

────────────────────────────────────────────────────────────
  GEOGRAPHIC EXTENT OF HIGH-RES TILES
────────────────────────────────────────────────────────────
  Longitude coverage: -180.00° → 180.00°
  Latitude  coverage: -48.92° → 83.98°

  Empty/stub tiles (≤512 bytes): 19 of 457 (4.2%)
  These are zero-content tiles (ocean, ice, etc.) and safe to skip.
  Substantive tiles (>512 bytes): 438  /  10.11 TB
  Cost of substantive tiles only: R2 $151.59/mo   S3 $232.44/mo

────────────────────────────────────────────────────────────
  TOP 10 LARGEST TILES
────────────────────────────────────────────────────────────
  Name                                 Size    Zoom  Lat range
  ──────────────────────────── ────────────  ──────  ────────────────────────
  planet.pmtiles                  705.73 GB  z0–12  -85.1° → 85.1°
  6-14-26.pmtiles                 352.31 GB  z13–17  27.1° → 32.0°
  6-33-22.pmtiles                 311.99 GB  z13–18  45.1° → 48.9°
  6-30-24.pmtiles                 227.28 GB  z13–17  36.6° → 41.0°
  6-34-22.pmtiles                 207.63 GB  z13–17  45.1° → 48.9°
  6-15-26.pmtiles                 201.42 GB  z13–17  27.1° → 32.0°
  6-16-25.pmtiles                 178.42 GB  z13–17  32.0° → 36.6°
  6-12-24.pmtiles                 174.04 GB  z13–16  36.6° → 41.0°
  6-17-24.pmtiles                 169.77 GB  z13–16  36.6° → 41.0°
  6-17-26.pmtiles                 169.33 GB  z13–17  27.1° → 32.0°

────────────────────────────────────────────────────────────

"""

import json
import sys
import math
import argparse
from collections import defaultdict

# ── Storage pricing (per GB/month) ────────────────────────────────────────────
R2_PRICE_PER_GB  = 0.015   # Cloudflare R2
S3_PRICE_PER_GB  = 0.023   # AWS S3 us-east-1 standard

# ── Resolution math ───────────────────────────────────────────────────────────
EARTH_CIRC_M = 40_075_016.686
TILE_PX      = 256

def ground_res(zoom: int, lat_deg: float = 0.0) -> float:
    """Ground resolution in meters/pixel at a given zoom and latitude."""
    return (EARTH_CIRC_M * math.cos(math.radians(lat_deg))) / (TILE_PX * 2 ** zoom)

# ── Formatting helpers ─────────────────────────────────────────────────────────
def fmt_bytes(b: int) -> str:
    for unit, threshold in [("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("KB", 1e3)]:
        if b >= threshold:
            return f"{b/threshold:.2f} {unit}"
    return f"{b} B"

def fmt_cost(size_bytes: int) -> tuple[str, str]:
    gb = size_bytes / 1e9
    return f"${gb * R2_PRICE_PER_GB:.2f}/mo", f"${gb * S3_PRICE_PER_GB:.2f}/mo"

def bar(pct: float, width: int = 30) -> str:
    filled = round(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)

def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")

# ── Main analysis ──────────────────────────────────────────────────────────────
def analyze(items: list[dict]):

    total_size  = sum(i["size"] for i in items)
    total_files = len(items)
    planet      = next((i for i in items if i["name"] == "planet.pmtiles"), None)
    hires       = [i for i in items if i["name"] != "planet.pmtiles"]
    hires_size  = sum(i["size"] for i in hires)

    # ── Summary ───────────────────────────────────────────────────────────────
    section("SUMMARY")
    print(f"  {'Total files':<30} {total_files:,}")
    print(f"  {'Total size':<30} {fmt_bytes(total_size)}")
    if planet:
        r2, s3 = fmt_cost(planet["size"])
        print(f"  {'Planet base (z0–12)':<30} {fmt_bytes(planet['size'])}  (R2 {r2}  S3 {s3})")
    print(f"  {'High-res tiles (z13+)':<30} {len(hires):,} files  /  {fmt_bytes(hires_size)}")
    if hires:
        print(f"  {'Avg hi-res tile size':<30} {fmt_bytes(hires_size // len(hires))}")
    r2_total, s3_total = fmt_cost(total_size)
    r2_hi,    s3_hi    = fmt_cost(hires_size)
    print(f"  {'Storage cost (full)':<30} R2 {r2_total}   S3 {s3_total}")
    print(f"  {'Storage cost (hi-res only)':<30} R2 {r2_hi}   S3 {s3_hi}")
    print(f"\n  Note: prices are storage only. Egress is additional.")
    print(f"        R2 ${R2_PRICE_PER_GB}/GB · S3 ${S3_PRICE_PER_GB}/GB (us-east-1 standard)")

    # ── Breakdown by zoom level ───────────────────────────────────────────────
    section("BREAKDOWN BY ZOOM LEVEL")
    zoom_groups = defaultdict(lambda: {"count": 0, "size": 0, "zoom": None})
    for item in items:
        key = f"z{item['min_zoom']}" if item["min_zoom"] == item["max_zoom"] else f"z{item['min_zoom']}–{item['max_zoom']}"
        zoom_groups[key]["count"] += 1
        zoom_groups[key]["size"]  += item["size"]
        zoom_groups[key]["zoom"]   = item["min_zoom"]

    sorted_groups = sorted(zoom_groups.items(), key=lambda x: x[1]["zoom"])
    print(f"  {'Zoom':<14} {'Files':>7}  {'Size':>12}  {'%':>6}  {'Distribution'}")
    print(f"  {'─'*14} {'─'*7}  {'─'*12}  {'─'*6}  {'─'*30}")
    for key, g in sorted_groups:
        pct = g["size"] / total_size * 100
        r2, s3 = fmt_cost(g["size"])
        print(f"  {key:<14} {g['count']:>7,}  {fmt_bytes(g['size']):>12}  {pct:>5.1f}%  {bar(pct, 28)}")
        print(f"  {'':14} {'':>7}  R2 {r2:<12}  S3 {s3}")

    # ── Resolution table ──────────────────────────────────────────────────────
    section("GROUND RESOLUTION BY ZOOM LEVEL")
    print(f"  {'Zoom':<8} {'Equator':>14} {'45°N':>14} {'60°N':>14}   Context")
    print(f"  {'─'*8} {'─'*14} {'─'*14} {'─'*14}   {'─'*28}")
    context = {
        0:  "world overview",
        1:  "continental",
        4:  "country level",
        6:  "region/state",
        8:  "city",
        10: "neighborhood",
        12: "streets (planet max)",
        13: "buildings / hi-res tiles",
        14: "parcel level",
    }
    present_zooms = sorted({i["min_zoom"] for i in items} | {i["max_zoom"] for i in items})
    for z in present_zooms:
        ctx = context.get(z, "")
        print(f"  {z:<8} {ground_res(z,  0):>12.1f}m  {ground_res(z, 45):>12.1f}m  {ground_res(z, 60):>12.1f}m   {ctx}")

    # ── Geographic extent of hi-res tiles ─────────────────────────────────────
    if hires:
        section("GEOGRAPHIC EXTENT OF HIGH-RES TILES")
        lons = [(i["min_lon"], i["max_lon"]) for i in hires]
        lats = [(i["min_lat"], i["max_lat"]) for i in hires]
        print(f"  Longitude coverage: {min(l[0] for l in lons):.2f}° → {max(l[1] for l in lons):.2f}°")
        print(f"  Latitude  coverage: {min(l[0] for l in lats):.2f}° → {max(l[1] for l in lats):.2f}°")

        # Empty tiles (size <= 512 bytes are likely empty/stub tiles)
        empty = [i for i in hires if i["size"] <= 512]
        print(f"\n  Empty/stub tiles (≤512 bytes): {len(empty):,} of {len(hires):,} "
              f"({len(empty)/len(hires)*100:.1f}%)")
        if empty:
            print(f"  These are zero-content tiles (ocean, ice, etc.) and safe to skip.")
        real = [i for i in hires if i["size"] > 512]
        real_size = sum(i["size"] for i in real)
        r2, s3 = fmt_cost(real_size)
        print(f"  Substantive tiles (>512 bytes): {len(real):,}  /  {fmt_bytes(real_size)}")
        print(f"  Cost of substantive tiles only: R2 {r2}   S3 {s3}")

    # ── Top 10 largest tiles ──────────────────────────────────────────────────
    section("TOP 10 LARGEST TILES")
    top = sorted(items, key=lambda x: x["size"], reverse=True)[:10]
    print(f"  {'Name':<28} {'Size':>12}  {'Zoom':>6}  {'Lat range'}")
    print(f"  {'─'*28} {'─'*12}  {'─'*6}  {'─'*24}")
    for t in top:
        lat_range = f"{t['min_lat']:.1f}° → {t['max_lat']:.1f}°"
        print(f"  {t['name']:<28} {fmt_bytes(t['size']):>12}  z{t['min_zoom']}–{t['max_zoom']}  {lat_range}")


# ── Region filter ──────────────────────────────────────────────────────────────
def filter_region(items: list[dict], min_lon: float, max_lon: float,
                  min_lat: float, max_lat: float):
    matches = [
        i for i in items
        if i["max_lon"] > min_lon and i["min_lon"] < max_lon
        and i["max_lat"] > min_lat and i["min_lat"] < max_lat
    ]

    section(f"REGION FILTER  ({min_lon}° to {max_lon}° lon,  {min_lat}° to {max_lat}° lat)")

    if not matches:
        print("  No tiles found in that bounding box.")
        return

    total_size = sum(i["size"] for i in matches)
    r2, s3 = fmt_cost(total_size)
    planet_included = any(i["name"] == "planet.pmtiles" for i in matches)

    print(f"  Matching tiles:  {len(matches):,}")
    print(f"  Combined size:   {fmt_bytes(total_size)}")
    print(f"  Storage cost:    R2 {r2}   S3 {s3}")
    if not planet_included:
        print(f"\n  ⚠  planet.pmtiles is not in this filter (it always covers the whole globe).")
        print(f"     Remember to include it for base zoom 0–12 coverage.")

    print(f"\n  {'Name':<28} {'Size':>12}  {'Zoom':>6}  {'Lon':>18}  Lat")
    print(f"  {'─'*28} {'─'*12}  {'─'*6}  {'─'*18}  {'─'*20}")
    for t in sorted(matches, key=lambda x: x["size"], reverse=True):
        lon = f"{t['min_lon']:.1f}→{t['max_lon']:.1f}"
        lat = f"{t['min_lat']:.1f}→{t['max_lat']:.1f}"
        print(f"  {t['name']:<28} {fmt_bytes(t['size']):>12}  z{t['min_zoom']}–{t['max_zoom']}  {lon:>18}  {lat}")

    # Output a download list
    outfile = "download_list.txt"
    with open(outfile, "w") as f:
        for t in matches:
            f.write(t["url"] + "\n")
    print(f"\n  Download list written to: {outfile}")
    print(f"  Run with:  wget -i {outfile}  or  aria2c -i {outfile} -x 8")


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Analyze a Mapterhorn PMTiles JSON manifest.")
    parser.add_argument("json_file", help="Path to the Mapterhorn JSON file")
    parser.add_argument("--region", nargs=4, type=float,
                        metavar=("MIN_LON", "MAX_LON", "MIN_LAT", "MAX_LAT"),
                        help="Filter tiles to a bounding box")
    args = parser.parse_args()

    try:
        with open(args.json_file) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: file not found: {args.json_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON — {e}")
        sys.exit(1)

    items = data if isinstance(data, list) else data.get("items", [])
    if not items:
        print("Error: no items found in JSON.")
        sys.exit(1)

    print(f"\nMapterhorn PMTiles Analyzer")
    print(f"Version: {data.get('version', 'unknown') if isinstance(data, dict) else 'n/a'}")
    print(f"Source:  {args.json_file}  ({len(items):,} items)")

    analyze(items)

    if args.region:
        filter_region(items, *args.region)

    print(f"\n{'─' * 60}\n")


if __name__ == "__main__":
    main()