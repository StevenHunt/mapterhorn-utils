#!/usr/bin/env python3
"""
Mapterhorn PMTiles Attribution Joiner
Joins download_urls.json with attribution.json, inferring which source datasets
likely contributed to each tile based on geographic overlap and resolution.

Usage:
  python attribute_pmtiles.py --downloads download_urls.json --attribution attribution.json
  python attribute_pmtiles.py --downloads download_urls.json --attribution attribution.json --output enriched.json
  python attribute_pmtiles.py --downloads download_urls.json --attribution attribution.json --licenses

Fetches JSONs from Mapterhorn if local files aren't provided:
  python attribute_pmtiles.py --fetch

# ========================================================================================
(tiler) ➜  mapterhorn python3 attribute.py --downloads pmtiles.json --attribution attribution.json --licenses


Mapterhorn Attribution Joiner
  Manifest version : 0.0.11
  Tiles            : 458
  Attribution sources: 134

────────────────────────────────────────────────────────────
  ATTRIBUTION SUMMARY
────────────────────────────────────────────────────────────
  Total tiles:              458
  Tiles with attribution:   458
  Tiles with no match:      0  (source bbox not in table)
  Unique contributing sources: 133

  License distribution across tiles:
  License                                        Tiles    Total size
  ───────────────────────────────────────────── ──────  ────────────
  COPERNICUS full, free and open license           457      10.11 TB
  CC BY 4.0                                        219       3.30 TB
  Public Domain (U.S. Government Work)             158       5.74 TB
  Open Government Licence - Canada                 128       2.27 TB
  CC0                                               28       1.70 TB
  国土地理院コンテンツ利用規約／測量法に基づく国土地理院長承認（使用）R 7JHs 542      21     233.39 GB
  Licence Ouverte / Open Licence version 2.0        16       1.15 TB
  Attribution 4.0 International (CC BY 4.0)         15     275.51 GB
  Creative Commons Namensnennung 4.0 Internatio     11     614.55 GB
  Open Government Licence                           10     372.98 GB
  CC-BY 4.0                                          9     731.35 GB
  CC-BY-4.0                                          7     973.40 GB
  CC-BY-4.0: Land Kärnten - data.gv.at               6     889.26 GB
  Open Government Data (Art. 40a. geod. i karto      6     360.73 GB
  CC BY Creative Commons Attribution („uvedenie      6     479.39 GB
  Datenlizenz Deutschland - Namensnennung - Ver      5     694.06 GB
  Creative Commons Attribution 4.0                   5     309.27 GB
  Republic of Estonia Land and Spatial Developm      5      96.75 GB
  Datenlizenz Deutschland - Zero - Version 2.0       4     382.07 GB
  Romanian Open Government License v1.0 (OGL-RO      3      62.40 GB
  Open Government License                            2     185.68 GB
  Model license for free reuse                       2     239.52 GB
  Creative Commons CCZero                            2     519.62 GB
  CC BY 2.5                                          2     519.62 GB
  ASTER GDEM Public Domain                           2     980.88 MB
  DL-DE->BY-2.0                                      1     135.69 GB
  Datenlizenz Deutschland – Namensnennung – Ver      1     135.69 GB
  Open Government Data                               1     311.99 GB

  Top contributing sources (by tile count):
  Source                Tiles
  ──────────────────── ──────
  glo30                   457
  usgs3dep13              158
  cahrdem2                128
  dkgreenland             127
  us1ka                    60
  us1kb                    35
  no                       33
  se                       26
  jpdem10a                 21
  us1kc                    18
  fi                       15
  jpdem1a                  12
  jpdem5a                  11
  au5e                     10
  tinitaly                 10
# ========================================================================================
"""

import json
import math
import argparse
import urllib.request
from collections import defaultdict

# ── Bounding boxes for all known Mapterhorn source codes ──────────────────────
# bbox = (min_lon, min_lat, max_lon, max_lat)
# Sources are keyed by exact source code from attribution.json.
# Global/baseline datasets intentionally cover full extent.
COUNTRY_BBOXES = {

    # ── Global baseline ────────────────────────────────────────────────────────
    "glo30":          (-180.0, -90.0,  180.0,  90.0),  # Copernicus GLO-30 global 30m
    "usgs3dep13":     (-179.2,  17.6,  -65.0,  71.5),  # USGS 3DEP 1/3 arc-sec (contiguous US + AK/HI/PR)

    # ── Latvia ─────────────────────────────────────────────────────────────────
    "aalv":           ( 20.97,  55.67,  28.24,  57.97), # Latvian Geospatial Agency

    # ── Austria ───────────────────────────────────────────────────────────────
    "at1":            (  9.53,  46.37,  17.16,  49.02), # BEV national 1m
    "at10":           (  9.53,  46.37,  17.16,  49.02), # BEV national 10m
    "atburgenland":   ( 16.05,  47.37,  17.16,  48.01), # Burgenland state
    "atkaernten":     ( 12.63,  46.37,  15.01,  47.12), # Kärnten state
    "atoberoesterreich": (13.18, 47.46, 15.00,  48.78), # Oberösterreich state
    "atsalzburg":     ( 12.07,  46.97,  13.99,  48.02), # Salzburg state

    # ── Australia ─────────────────────────────────────────────────────────────
    # au5a–au5i are state/territory 5m DEMs
    "au5a":           (112.92, -38.20, 129.00, -25.99), # WA (west)
    "au5b":           (129.00, -38.10, 141.00, -25.99), # SA + NT south
    "au5c":           (141.00, -39.20, 150.00, -33.98), # VIC + NSW south
    "au5d":           (150.00, -37.60, 153.64, -27.99), # NSW coast + ACT
    "au5e":           (138.00, -29.50, 154.00, -15.01), # QLD
    "au5f":           (113.00, -35.10, 117.00, -30.01), # SW WA coast
    "au5g":           (115.00, -31.80, 122.00, -25.01), # WA midwest
    "au5h":           (130.85, -12.55, 136.00,  -9.90), # NT north (Darwin region)
    "au5i":           (144.00, -43.64, 148.50, -39.50), # Tasmania

    # ── Belgium ────────────────────────────────────────────────────────────────
    "beflanders":     (  2.54,  50.67,   5.92,  51.51), # Flanders region 1m
    "bewallonie":     (  2.74,  49.49,   6.41,  50.83), # Wallonia region 0.5m

    # ── Canada ────────────────────────────────────────────────────────────────
    "cahrdem2":       (-141.00, 41.68,  -52.65,  83.11), # Canada HRDEM 2m (national)

    # ── Switzerland ───────────────────────────────────────────────────────────
    "chzh":           (  8.35,  47.15,   8.99,  47.69), # Canton Zurich (swissALTI3D subset)
    "swissalti3d":    (  5.96,  45.82,  10.49,  47.81), # swisstopo national 0.5m

    # ── Cyprus ────────────────────────────────────────────────────────────────
    "cy":             ( 32.27,  34.57,  34.00,  35.71),

    # ── Czech Republic ────────────────────────────────────────────────────────
    "cz":             ( 12.09,  48.56,  18.86,  51.05),

    # ── Germany (national + states) ───────────────────────────────────────────
    "debayern":       ( 10.00,  47.27,  13.84,  50.56),
    "deberlin":       ( 13.09,  52.34,  13.76,  52.68),
    "debrandenburg":  ( 11.27,  51.36,  14.77,  53.56),
    "debremen":       (  8.48,  52.86,   9.01,  53.23),
    "debw":           (  7.51,  47.53,  10.50,  49.79), # Baden-Württemberg
    "dehamburg":      (  9.73,  53.39,  10.33,  53.74),
    "dehessen":       (  7.77,  49.39,  10.24,  51.66),
    "demv":           ( 10.59,  53.11,  14.41,  54.69), # Mecklenburg-Vorpommern
    "deniedersachsen":( 6.65,   51.29,  11.60,  53.89),
    "denrw":          (  5.87,  50.32,   9.46,  52.53), # Nordrhein-Westfalen
    "derlp":          (  6.11,  48.97,   8.51,  50.94), # Rheinland-Pfalz
    "desaarland":     (  6.36,  49.11,   7.40,  49.64),
    "desachsen":      ( 11.87,  50.17,  15.04,  51.68),
    "desachsenanhalt":( 10.56,  51.15,  13.19,  53.04),
    "desh":           (  8.00,  53.36,  11.33,  55.06), # Schleswig-Holstein
    "dethueringen":   (  9.87,  50.20,  12.65,  51.65),

    # ── Denmark ───────────────────────────────────────────────────────────────
    "dk":             (  8.09,  54.56,  15.20,  57.75), # mainland Denmark
    "dkfaroe":        (-7.69,   61.39,  -6.25,  62.40), # Faroe Islands
    "dkgreenland":    (-73.00,  59.74,  -12.08, 83.65), # Greenland

    # ── Estonia ───────────────────────────────────────────────────────────────
    "ee":             ( 21.84,  57.52,  28.21,  59.68),

    # ── Spain ─────────────────────────────────────────────────────────────────
    # es2a–es2c = 2m tiles, es5a–es5d = 5m tiles (regional IGN blocks)
    "es2a":           ( -9.39,  35.95,  -3.00,  43.79), # west Iberia
    "es2b":           ( -3.00,  36.00,   4.33,  43.79), # east Iberia
    "es2c":           ( -2.10,  35.17,   4.33,  36.50), # SE coast + Murcia
    "es5a":           ( -9.39,  35.95,  -3.00,  43.79),
    "es5b":           ( -3.00,  36.00,   4.33,  43.79),
    "es5c":           (-18.16,  27.64, -13.42,  29.42), # Canary Islands
    "es5d":           ( -7.97,  35.89,  -1.00,  40.00), # southern Spain

    # ── Finland ───────────────────────────────────────────────────────────────
    "fi":             ( 19.08,  59.45,  31.59,  70.09),

    # ── France (RGEAlti national + overseas) ─────────────────────────────────
    "frrgealti1metro":  (-5.14,  41.34,   9.56,  51.09), # metropolitan France 1m
    "frrgealti5metro":  (-5.14,  41.34,   9.56,  51.09), # metropolitan France 5m
    "frrgealti1corse":  (  8.54,  41.33,   9.57,  43.03), # Corsica 1m
    "frrgealti5corse":  (  8.54,  41.33,   9.57,  43.03), # Corsica 5m
    "frrgealti1caribbean": (-63.16, 14.38, -60.80, 16.54), # Guadeloupe + Martinique 1m
    "frrgealti5caribbean": (-63.16, 14.38, -60.80, 16.54),
    "frrgealti1guiana": (-54.61,  2.11,  -51.64,  5.76),  # French Guiana 1m
    "frrgealti5guiana": (-54.61,  2.11,  -51.64,  5.76),
    "frrgealti1mayotte": ( 45.02, -13.07,  45.32, -12.63), # Mayotte 1m
    "frrgealti5mayotte": ( 45.02, -13.07,  45.32, -12.63),
    "frrgealti1reunion": ( 55.21, -21.40,  55.84, -20.87), # Réunion 1m
    "frrgealti5reunion": ( 55.21, -21.40,  55.84, -20.87),
    "frrgealti1stpm":  (-56.43,  46.74, -56.14,  47.16),  # Saint-Pierre-et-Miquelon 1m
    "frrgealti5stpm":  (-56.43,  46.74, -56.14,  47.16),

    # ── Iceland ───────────────────────────────────────────────────────────────
    "is":             (-24.53,  63.39, -13.50,  66.54),

    # ── Israel ────────────────────────────────────────────────────────────────
    "isr10":          ( 34.27,  29.50,  35.90,  33.34), # Israel 10m DEM

    # ── Italy (national + autonomous regions) ────────────────────────────────
    "tinitaly":       (  6.63,  35.49,  18.78,  47.09), # TIN Italia national
    "itaosta":        (  6.85,  45.46,   7.99,  45.99), # Valle d'Aosta
    "itbozen":        ( 10.38,  46.22,  12.48,  47.09), # Alto Adige/Südtirol
    "itlombardia":    (  8.50,  44.68,  11.14,  46.64), # Lombardia
    "itpiemonte":     (  6.63,  43.80,   9.21,  46.47), # Piemonte
    "itsardegnacostiera": (  8.13, 38.86,   9.83,  41.31), # Sardegna coastal
    "itsardegnaurban":(  8.13,  38.86,   9.83,  41.31), # Sardegna urban
    "itsicily":       ( 11.93,  36.63,  15.65,  38.33), # Sicily
    "ittrentino":     ( 10.38,  45.67,  11.95,  46.54), # Trentino

    # ── Japan ─────────────────────────────────────────────────────────────────
    "jpdem10a":       (122.93,  24.05, 154.00,  45.55), # GSI 10m DEM (main + Ryukyu)
    "jpdem10b":       (141.35,  41.35, 145.82,  45.55), # Hokkaido east
    "jpdem1a":        (129.41,  31.02, 145.82,  45.55), # GSI 1m LiDAR (selected)
    "jpdem5a":        (130.19,  31.14, 141.91,  41.55), # GSI 5m DEM west
    "jpdem5b":        (138.99,  34.90, 145.82,  41.55), # GSI 5m DEM central-east
    "jpdem5c":        (122.93,  24.05, 131.00,  27.20), # Ryukyu islands

    # ── Luxembourg ────────────────────────────────────────────────────────────
    "lu":             (  5.74,  49.45,   6.53,  50.18),

    # ── Netherlands ───────────────────────────────────────────────────────────
    "nlahn5lowresfilled": (3.31, 50.75,  7.23,  53.55), # AHN5 low-res filled

    # ── Norway ────────────────────────────────────────────────────────────────
    "no":             (  4.07,  57.97,  31.14,  71.19), # mainland Norway
    "nosvalbard":     ( 10.49,  76.44,  33.51,  80.76), # Svalbard archipelago

    # ── New Zealand ───────────────────────────────────────────────────────────
    "nz":             (166.43, -47.29, 178.57, -34.40),

    # ── Poland ────────────────────────────────────────────────────────────────
    "pl1":            ( 14.12,  49.00,  24.15,  54.84), # Poland 1m LiDAR
    "pl5":            ( 14.12,  49.00,  24.15,  54.84), # Poland 5m

    # ── Portugal ──────────────────────────────────────────────────────────────
    "pt":             ( -9.52,  36.96,  -6.19,  42.15), # mainland Portugal
    "ptmadeira":      (-17.27,  32.63, -16.27,  33.12), # Madeira archipelago

    # ── Romania ───────────────────────────────────────────────────────────────
    "ro":             ( 22.09,  43.62,  30.26,  48.27),

    # ── Rwanda ────────────────────────────────────────────────────────────────
    "rw":             ( 28.86,  -2.84,  30.90,  -1.05),

    # ── Sweden ────────────────────────────────────────────────────────────────
    "se":             ( 10.96,  55.34,  24.17,  69.06),

    # ── Slovenia ──────────────────────────────────────────────────────────────
    "si":             ( 13.38,  45.42,  16.60,  46.88),

    # ── Slovakia ──────────────────────────────────────────────────────────────
    "sk":             ( 16.83,  47.73,  22.57,  49.61),

    # ── United Kingdom ────────────────────────────────────────────────────────
    "ukengland":      ( -5.73,  49.86,   1.77,  55.81),
    "ukscotland":     ( -7.58,  54.63,  -0.73,  60.86),
    "ukwales":        ( -5.35,  51.35,  -2.65,  53.43),

    # ── United States ─────────────────────────────────────────────────────────
    # us1aa–us1ma are USGS 1m LiDAR tiles by region/state grouping
    # Approximate coverage regions based on USGS 3DEP collection areas:
    "us1aa":          (-124.76,  45.54, -116.46,  49.00), # WA state
    "us1ba":          (-124.56,  41.99, -116.46,  46.26), # OR state
    "us1ca":          (-124.41,  32.53, -114.13,  42.01), # CA north
    "us1cb":          (-122.40,  32.53, -114.13,  37.00), # CA south/central
    "us1cc":          (-117.27,  32.53, -114.13,  34.00), # CA SE
    "us1da":          (-117.27,  31.33, -102.00,  37.00), # AZ + NM west
    "us1db":          (-109.05,  31.33, -103.00,  37.00), # NM + CO south
    "us1dc":          (-109.05,  36.99, -102.05,  41.00), # CO + UT south
    "us1ea":          (-104.05,  36.99,  -94.62,  41.00), # KS + NE + CO east
    "us1eb":          (-104.05,  41.00,  -96.45,  49.00), # NE + SD + ND west
    "us1ec":          ( -96.45,  43.50,  -89.50,  49.00), # MN + WI north
    "us1fa":          ( -94.62,  29.00,  -88.82,  33.02), # LA + MS + TX east
    "us1fb":          (-100.00,  25.84,  -93.51,  30.50), # TX south
    "us1fc":          (-106.65,  25.84, -100.00,  30.50), # TX west
    "us1ga":          ( -88.82,  24.54,  -79.97,  31.00), # FL + GA south
    "us1gb":          ( -85.61,  30.20,  -75.46,  36.59), # GA + SC + NC
    "us1gc":          ( -83.68,  36.54,  -75.46,  39.72), # VA + WV + KY + TN
    "us1ha":          ( -84.84,  38.40,  -74.68,  43.64), # OH + IN + MI south
    "us1hb":          ( -90.42,  41.49,  -82.41,  47.08), # IL + WI + MI north
    "us1hc":          ( -92.21,  43.50,  -82.41,  47.08), # MN + WI + MI north
    "us1ia":          ( -74.75,  38.92,  -66.93,  45.01), # NJ + NY + CT + MA
    "us1ib":          ( -73.73,  40.50,  -66.93,  47.46), # NY + NE states
    "us1ic":          ( -71.08,  41.18,  -66.93,  47.46), # ME + NH + VT
    "us1ja":          ( -80.52,  39.72,  -71.85,  45.01), # PA + NY + NJ
    "us1jb":          ( -77.12,  37.91,  -71.85,  41.30), # MD + DE + VA coast
    "us1jc":          ( -76.92,  34.98,  -75.46,  36.59), # NC coast
    "us1ka":          (-178.23,  51.22, -130.00,  71.39), # Alaska west
    "us1kb":          (-153.00,  55.00, -130.00,  71.39), # Alaska east
    "us1kc":          (-169.00,  52.60, -140.00,  60.90), # Alaska south
    "us1la":          (-160.26,  18.91, -154.81,  22.24), # Hawaii main islands
    "us1lb":          (-156.70,  18.91, -154.81,  21.00), # Hawaii south
    "us1lc":          (-158.30,  21.20, -157.60,  21.73), # Oahu
    "us1ma":          ( -67.30,  17.87,  -65.22,  18.56), # Puerto Rico
    "usgs3dep13":     (-179.23,  17.67,  -65.22,  71.39), # USGS 3DEP 1/3" national mosaic
}

DOWNLOAD_URL   = "https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/mapterhorn/mapterhorn/download_urls.json"
ATTRIBUTION_URL = "https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop/mapterhorn/mapterhorn/attribution.json"

# ── Tile math ──────────────────────────────────────────────────────────────────
def tile_to_bbox(z: int, x: int, y: int) -> tuple[float, float, float, float]:
    """Convert z/x/y tile coords to (min_lon, min_lat, max_lon, max_lat)."""
    n = 2 ** z
    min_lon = x / n * 360.0 - 180.0
    max_lon = (x + 1) / n * 360.0 - 180.0
    min_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    max_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    return min_lon, min_lat, max_lon, max_lat

def bboxes_overlap(a: tuple, b: tuple) -> bool:
    """True if two (min_lon, min_lat, max_lon, max_lat) boxes intersect."""
    return (a[0] < b[2] and a[2] > b[0] and
            a[1] < b[3] and a[3] > b[1])

def parse_tile_name(name: str) -> tuple[int, int, int] | None:
    """Parse '6-0-21.pmtiles' → (6, 0, 21). Returns None for planet.pmtiles."""
    stem = name.replace(".pmtiles", "")
    parts = stem.split("-")
    if len(parts) == 3:
        try:
            return tuple(int(p) for p in parts)
        except ValueError:
            pass
    return None

# ── Source → bbox lookup ───────────────────────────────────────────────────────
_UNKNOWN_SOURCES: set = set()

def source_bbox(source: str, item_bbox: tuple):
    """
    Return the tight bounding box for an attribution source code.
    Returns None if the source is not in the table (caller skips the match).
    """
    src = source.lower()
    if src in COUNTRY_BBOXES:
        return COUNTRY_BBOXES[src]
    if src not in _UNKNOWN_SOURCES:
        _UNKNOWN_SOURCES.add(src)
    return None


# ── Core join ─────────────────────────────────────────────────────────────────
def join(downloads: list[dict], attributions: list[dict]) -> list[dict]:
    """
    For each tile in downloads, find all attribution sources whose geographic
    bbox overlaps the tile's bbox. Sort candidates by resolution (finest first).
    Returns an enriched list of tile dicts.
    """
    results = []

    for tile in downloads:
        name = tile["name"]
        tile_box = (tile["min_lon"], tile["min_lat"], tile["max_lon"], tile["max_lat"])

        # planet.pmtiles is a global composite of all sources — don't spatial-join,
        # just tag it with the full source count and a note.
        if name == "planet.pmtiles":
            enriched = dict(tile)
            enriched["attribution_sources"] = [{
                "note": "Global composite (zoom 0–12) — all attribution sources contribute. See full attribution list.",
                "source_count": len(attributions),
            }]
            enriched["attribution_count"] = len(attributions)
            enriched["is_global_composite"] = True
            results.append(enriched)
            continue

        # For z-x-y tiles, compute exact tile bbox from the name
        coords = parse_tile_name(name)
        computed_box = tile_to_bbox(*coords) if coords else tile_box

        candidates = []
        for src in attributions:
            s_box = source_bbox(src["source"], tile_box)
            if s_box is None:
                continue
            if bboxes_overlap(computed_box, s_box):
                candidates.append({
                    "source":      src["source"],
                    "name":        src["name"],
                    "producer":    src["producer"],
                    "license":     src["license"],
                    "resolution":  src.get("resolution"),
                    "website":     src["website"],
                    "license_pdf": src.get("license_pdf"),
                })

        # Sort by resolution ascending (finest = smallest number first)
        candidates.sort(key=lambda x: x["resolution"] if x["resolution"] else 9999)

        enriched = dict(tile)
        enriched["attribution_sources"] = candidates
        enriched["attribution_count"] = len(candidates)
        enriched["is_global_composite"] = False
        results.append(enriched)

    return results


# ── Reporting ──────────────────────────────────────────────────────────────────
def fmt_bytes(b: int) -> str:
    for unit, t in [("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("KB", 1e3)]:
        if b >= t:
            return f"{b/t:.2f} {unit}"
    return f"{b} B"

def print_report(enriched: list[dict]):
    print("\n" + "─" * 60)
    print("  ATTRIBUTION SUMMARY")
    print("─" * 60)

    # Tiles with zero candidates (no source bbox match)
    no_match = [t for t in enriched if t["attribution_count"] == 0]
    print(f"  Total tiles:              {len(enriched):,}")
    print(f"  Tiles with attribution:   {len(enriched) - len(no_match):,}")
    print(f"  Tiles with no match:      {len(no_match):,}  (source bbox not in table)")

    # Unique sources across all tiles
    all_sources = set()
    for t in enriched:
        for s in t["attribution_sources"]:
            if "source" in s:
                all_sources.add(s["source"])
    print(f"  Unique contributing sources: {len(all_sources)}")

    # License breakdown
    license_counts = defaultdict(int)
    license_size   = defaultdict(int)
    for t in enriched:
        seen = set()
        for s in t["attribution_sources"]:
            if "source" not in s:
                continue
            lic = s["license"]
            if lic not in seen:
                seen.add(lic)
                license_counts[lic] += 1
                license_size[lic]   += t.get("size", 0)

    print("\n  License distribution across tiles:")
    print(f"  {'License':<45} {'Tiles':>6}  {'Total size':>12}")
    print(f"  {'─'*45} {'─'*6}  {'─'*12}")
    for lic, count in sorted(license_counts.items(), key=lambda x: -x[1]):
        print(f"  {lic[:45]:<45} {count:>6,}  {fmt_bytes(license_size[lic]):>12}")

    # Most common sources
    source_tile_counts = defaultdict(int)
    for t in enriched:
        for s in t["attribution_sources"]:
            if "source" in s:
                source_tile_counts[s["source"]] += 1
    top = sorted(source_tile_counts.items(), key=lambda x: -x[1])[:15]
    print("\n  Top contributing sources (by tile count):")
    print(f"  {'Source':<20} {'Tiles':>6}")
    print(f"  {'─'*20} {'─'*6}")
    for src, cnt in top:
        print(f"  {src:<20} {cnt:>6,}")

    if _UNKNOWN_SOURCES:
        print(f"\n  ⚠  Sources with no bbox mapping ({len(_UNKNOWN_SOURCES)}) — skipped in join:")
        for s in sorted(_UNKNOWN_SOURCES):
            print(f"    {s}")
        print(f"     Add these to COUNTRY_BBOXES to include them.")

    print("\n" + "─" * 60)


def print_license_block(enriched: list[dict], attributions: list[dict]):
    """Print a deduplicated legal attribution block suitable for a README or app footer."""
    used_sources = set()
    for t in enriched:
        for s in t["attribution_sources"]:
            if "source" in s:
                used_sources.add(s["source"])

    attr_by_source = {a["source"]: a for a in attributions}

    print("\n" + "─" * 60)
    print("  LEGAL ATTRIBUTION BLOCK  (for README / app footer)")
    print("─" * 60)
    for src in sorted(used_sources):
        a = attr_by_source.get(src)
        if not a:
            continue
        print(f"\n  {a['name']}")
        print(f"  Producer : {a['producer']}")
        print(f"  License  : {a['license']}")
        print(f"  Website  : {a['website']}")
        if a.get("license_pdf"):
            print(f"  License PDF: {a['license_pdf']}")
    print("─" * 60)


# ── CLI ────────────────────────────────────────────────────────────────────────
def fetch_json(url: str) -> list | dict:
    print(f"  Fetching {url} ...")
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode())

def main():
    parser = argparse.ArgumentParser(description="Join Mapterhorn download manifest with attribution JSON.")
    parser.add_argument("--downloads",    help="Path to download_urls.json")
    parser.add_argument("--attribution",  help="Path to attribution.json")
    parser.add_argument("--output",       default="enriched_tiles.json", help="Output file (default: enriched_tiles.json)")
    parser.add_argument("--fetch",        action="store_true", help="Fetch both JSONs directly from Mapterhorn")
    parser.add_argument("--licenses",     action="store_true", help="Also print a deduplicated legal attribution block")
    args = parser.parse_args()

    if args.fetch:
        downloads    = fetch_json(DOWNLOAD_URL)
        attributions = fetch_json(ATTRIBUTION_URL)
    else:
        if not args.downloads or not args.attribution:
            parser.error("Provide --downloads and --attribution, or use --fetch")
        with open(args.downloads)   as f: downloads    = json.load(f)
        with open(args.attribution) as f: attributions = json.load(f)

    # Normalize: handle wrapper object or bare array
    if isinstance(downloads, dict):
        version   = downloads.get("version", "unknown")
        downloads = downloads.get("items", [])
    else:
        version = "unknown"

    print(f"\nMapterhorn Attribution Joiner")
    print(f"  Manifest version : {version}")
    print(f"  Tiles            : {len(downloads):,}")
    print(f"  Attribution sources: {len(attributions):,}")

    enriched = join(downloads, attributions)
    print_report(enriched)

    if args.licenses:
        print_license_block(enriched, attributions)

    with open(args.output, "w") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)
    print(f"\n  Enriched manifest written to: {args.output}\n")


if __name__ == "__main__":
    main()