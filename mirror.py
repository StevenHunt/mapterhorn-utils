#!/usr/bin/env python3
"""
mirror.py

Downloads PMTiles files from Mapterhorn and uploads them directly
to a Cloudflare R2 bucket via multipart streaming — no temp disk storage.

Features:
  - Streams download → R2 (no local disk needed)
  - Skips files already in R2 (fully resumable, size-verified)
  - MD5 verification after upload
  - Retry with exponential backoff on transient failures
  - Progress bar per file
  - Summary report at the end

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EC2 Setup
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Launch EC2 instance
     AMI:           Ubuntu 24.04 LTS
     Instance type: c5n.4xlarge (25 Gbps dedicated baseline — no burst credits)
     Storage:       20 GB (files stream directly to R2, nothing lands on disk)
     Key pair:      create / select for SSH

2. SSH into the instance
     ssh -i key.pem ubuntu@<ec2-public-ip>

3. Install dependencies
     sudo apt update && sudo apt install python3-pip tmux -y
     pip3 install requests boto3 tqdm

4. Copy files from local machine
     scp -i key.pem mirror.py pmtiles.json ubuntu@<ec2-public-ip>:~/

5. Set R2 credentials
     export R2_ENDPOINT="https://<ACCOUNT_ID>.r2.cloudflarestorage.com"
     export R2_ACCESS_KEY="..."
     export R2_SECRET_KEY="..."
     export R2_BUCKET="r2-bucket-name"

6. Run inside tmux so it survives SSH disconnects
     tmux new -s mirror
     python3 mirror.py
     # Detach:    Ctrl+B then D
     # Reattach:  tmux attach -t mirror

Cost estimate (c5n.4xlarge):
  - EC2 c5n.4xlarge: ~$0.86/hr × ~4 hrs = ~$3-4
  - EC2 egress:      ~$0.09/GB → ~$90-100 for 10.81 TB
  - R2 ingress:      free

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Configuration
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Set the constants below or use environment variables (recommended).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Exec Examples
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# See what would be uploaded without touching anything
python3 mirror.py --dry-run

# Upload just the first 10 files (smallest first, ie testing)
python3 mirror.py --limit 10

# Combine: preview the first 10 without uploading
python3 mirror.py --dry-run --limit 10

# Full run
python3 mirror.py

"""

import argparse
import json
import os
import hashlib
import time
import boto3
import requests
from botocore.config import Config
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Configuration ─────────────────────────────────────────────────────────────

R2_ENDPOINT   = os.getenv('R2_ENDPOINT',   'https://<ACCOUNT_ID>.r2.cloudflarestorage.com')
R2_ACCESS_KEY = os.getenv('R2_ACCESS_KEY', '<R2_ACCESS_KEY>')
R2_SECRET_KEY = os.getenv('R2_SECRET_KEY', '<R2_SECRET_KEY>')
R2_BUCKET     = os.getenv('R2_BUCKET',     '<BUCKET_NAME>')

PMTILES_JSON  = 'pmtiles.json'
CHUNK_SIZE    = 128 * 1024 * 1024   # 128 MB per multipart chunk — reduces API overhead at 25 Gbps
READ_SIZE     = 8 * 1024 * 1024     # 8 MB read chunks — reduces Python loop overhead vs 1 MB
MAX_WORKERS   = 12                   # parallel uploads — saturates bandwidth across small files
MAX_RETRIES   = 3                    # retry attempts per file on transient failure

# ── R2 Client ─────────────────────────────────────────────────────────────────

def make_client():
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )

# ── Helpers ───────────────────────────────────────────────────────────────────

def already_uploaded(s3, name, expected_size):
    """
    Check if file exists in R2 AND matches expected size.
    A size mismatch means a previous upload was partial — re-upload it.
    """
    try:
        head = s3.head_object(Bucket=R2_BUCKET, Key=name)
        return head['ContentLength'] == expected_size
    except Exception:
        return False


def format_size(bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024
    return f"{bytes:.1f} PB"


# ── Core upload ───────────────────────────────────────────────────────────────

def upload_item(item, dry_run=False):
    """Stream download from Mapterhorn → R2 multipart upload."""
    s3 = make_client()
    name         = item['name']
    url          = item['url']
    size         = item['size']
    expected_md5 = item.get('md5sum')

    # Skip if already uploaded with correct size
    if already_uploaded(s3, name, size):
        return name, 'skipped', size

    if dry_run:
        return name, 'dry_run', size

    # Initiate multipart upload
    mpu = s3.create_multipart_upload(Bucket=R2_BUCKET, Key=name)
    upload_id = mpu['UploadId']
    parts = []
    part_number = 1
    md5 = hashlib.md5()

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()

            with tqdm(
                total=size,
                unit='B',
                unit_scale=True,
                desc=name,
                leave=True
            ) as pbar:
                buffer = b''
                for chunk in r.iter_content(chunk_size=READ_SIZE):
                    buffer += chunk
                    md5.update(chunk)
                    pbar.update(len(chunk))

                    if len(buffer) >= CHUNK_SIZE:
                        part = s3.upload_part(
                            Bucket=R2_BUCKET,
                            Key=name,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=buffer
                        )
                        parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                        part_number += 1
                        buffer = b''

                # Upload remaining buffer
                if buffer:
                    part = s3.upload_part(
                        Bucket=R2_BUCKET,
                        Key=name,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer
                    )
                    parts.append({'PartNumber': part_number, 'ETag': part['ETag']})

        # Complete multipart upload
        s3.complete_multipart_upload(
            Bucket=R2_BUCKET,
            Key=name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        # Verify MD5
        actual_md5 = md5.hexdigest()
        if expected_md5 and actual_md5 != expected_md5:
            return name, f'md5_mismatch (expected {expected_md5}, got {actual_md5})', size

        return name, 'ok', size

    except Exception as e:
        # Abort the multipart upload to avoid orphaned parts (which cost money)
        try:
            s3.abort_multipart_upload(Bucket=R2_BUCKET, Key=name, UploadId=upload_id)
        except Exception:
            pass
        return name, f'error: {e}', size


def upload_item_with_retry(item, dry_run=False):
    """Wrap upload_item with retries and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        name, status, size = upload_item(item, dry_run=dry_run)
        if status in ('ok', 'skipped', 'dry_run'):
            return name, status, size
        if attempt < MAX_RETRIES:
            wait = 5 * attempt  # 5s, 10s, 15s
            print(f"  ↺ {name}: attempt {attempt} failed ({status}), retrying in {wait}s...")
            time.sleep(wait)
        else:
            print(f"  ✗ {name}: all {MAX_RETRIES} attempts failed")
    return name, status, size


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Mirror PMTiles from Mapterhorn to Cloudflare R2')
    parser.add_argument('--limit',   type=int, default=None, help='Only process the first N files (smallest first)')
    parser.add_argument('--dry-run', action='store_true',    help='Check R2 status without uploading anything')
    args = parser.parse_args()

    with open(PMTILES_JSON) as f:
        data = json.load(f)

    items = data['items']
    total_size = sum(i['size'] for i in items)

    # Sort: planet.pmtiles last (it's 705 GB), smallest files first
    items = sorted(items, key=lambda i: i['size'])

    if args.limit:
        items = items[:args.limit]

    run_size = sum(i['size'] for i in items)

    print(f"Mapterhorn → R2 Mirror")
    print(f"  Bucket:     {R2_BUCKET}")
    print(f"  Files:      {len(items)}{f' (of {len(data[\"items\"])} total)' if args.limit else ''}")
    print(f"  Run size:   {format_size(run_size)}{f' (of {format_size(total_size)} total)' if args.limit else ''}")
    print(f"  Workers:    {MAX_WORKERS}")
    print(f"  Chunk size: {format_size(CHUNK_SIZE)}")
    if args.dry_run:
        print(f"  *** DRY RUN — no uploads will be performed ***")
    print()

    results = {'ok': [], 'skipped': [], 'dry_run': [], 'error': []}
    start = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(upload_item_with_retry, item, args.dry_run): item for item in items}
        for future in as_completed(futures):
            name, status, size = future.result()
            if status == 'ok':
                results['ok'].append((name, size))
                print(f"  ✓ {name}")
            elif status == 'skipped':
                results['skipped'].append((name, size))
                print(f"  — {name} (already in R2)")
            elif status == 'dry_run':
                results['dry_run'].append((name, size))
                print(f"  ~ {name} (would upload {format_size(size)})")
            else:
                results['error'].append((name, status))
                print(f"  ✗ {name}: {status}")

    elapsed = time.time() - start

    # ── Summary ───────────────────────────────────────────────────────────────
    uploaded_size = sum(s for _, s in results['ok'])
    skipped_size  = sum(s for _, s in results['skipped'])
    dry_run_size  = sum(s for _, s in results['dry_run'])

    print()
    print("─" * 50)
    print(f"Done in {elapsed/60:.1f} minutes")
    if results['ok']:
        print(f"  ✓ Uploaded:  {len(results['ok'])} files ({format_size(uploaded_size)})")
    if results['skipped']:
        print(f"  — Skipped:   {len(results['skipped'])} files ({format_size(skipped_size)}) — already in R2")
    if results['dry_run']:
        print(f"  ~ Would upload: {len(results['dry_run'])} files ({format_size(dry_run_size)})")
    if results['error']:
        print(f"  ✗ Errors:    {len(results['error'])}")
        print()
        print("Failed files:")
        for name, reason in results['error']:
            print(f"  {name}: {reason}")


if __name__ == '__main__':
    main()