#!/usr/bin/env python3
"""Download public datasets and upload them unchanged to MinIO/S3 (files are copied byte for byte).

  fetch.py --dataset clickbench --out s3://learn [--files 100]      -> s3://learn/clickbench/hits/hits_<i>.parquet   (~14 GB, 100M rows)
  fetch.py --dataset taxi --out s3://learn --years 2015-2024        -> s3://learn/taxi/yellow/<year>/yellow_tripdata_<YYYY-MM>.parquet
  fetch.py --dataset clickbench --out /some/dir --dry-run            -> list URLs and sizes, download nothing

Credentials: MINIO_ROOT_USER / MINIO_ROOT_PASSWORD / MINIO_ENDPOINT (default minio:9000). Existing keys are skipped, so a rerun resumes.
NYC taxi files keep the schema of their month (column names and types changed over the years); read them with schema merging.
"""
import argparse, os, sys, tempfile, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

CLICKBENCH = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_%d.parquet"
TAXI = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_%d-%02d.parquet"


def sources(args):
    if args.dataset == "clickbench":
        return [(CLICKBENCH % i, "clickbench/hits/hits_%d.parquet" % i) for i in range(args.files)]
    lo, hi = (int(x) for x in args.years.split("-"))
    return [(TAXI % (y, m), "taxi/yellow/%d/yellow_tripdata_%d-%02d.parquet" % (y, y, m))
            for y in range(lo, hi + 1) for m in range(1, 13)]


UA = {"User-Agent": "curl/8.5"}  # the ClickBench host answers 403 to urllib's default agent


def request(url, method="GET"):
    return urllib.request.Request(url, method=method, headers=UA)


def download(url, local):
    with urllib.request.urlopen(request(url), timeout=60) as r, open(local, "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                return
            f.write(chunk)


def size_of(url):
    try:
        with urllib.request.urlopen(request(url, "HEAD"), timeout=30) as r:
            return int(r.headers["Content-Length"])
    except urllib.error.HTTPError as e:
        return None if e.code in (403, 404) else (_ for _ in ()).throw(e)


def s3_client():
    import boto3
    return boto3.client("s3", endpoint_url="http://" + os.environ.get("MINIO_ENDPOINT", "minio:9000"),
                        aws_access_key_id=os.environ["MINIO_ROOT_USER"], aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, choices=["clickbench", "taxi"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--files", type=int, default=100)
    ap.add_argument("--years", default="2015-2024")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    items = sources(args)
    s3 = None
    bucket = prefix = None
    if args.out.startswith("s3://") and not args.dry_run:
        bucket, _, prefix = args.out[5:].partition("/")
        s3 = s3_client()

    def one(item):
        url, key = item
        size = size_of(url)
        if size is None:
            return key, 0, "missing"
        if args.dry_run:
            return key, size, "listed"
        full = (prefix.rstrip("/") + "/" if prefix else "") + key
        if s3:
            try:
                if s3.head_object(Bucket=bucket, Key=full)["ContentLength"] == size:
                    return key, size, "exists"
            except Exception:
                pass
            with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as d:
                local = os.path.join(d, "f.parquet")
                download(url, local)
                if os.path.getsize(local) != size:
                    raise RuntimeError("short download " + url)
                s3.upload_file(local, bucket, full)
        else:
            dest = os.path.join(args.out, key)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            download(url, dest)
        return key, size, "done"

    total = 0
    with ThreadPoolExecutor(args.threads) as pool:
        for key, size, state in pool.map(one, items):
            total += size
            print("%-60s %8.1f MB  %s" % (key, size / 1e6, state), flush=True)
    print("total %.2f GB in %d files" % (total / 1e9, len(items)))


if __name__ == "__main__":
    sys.exit(main())
