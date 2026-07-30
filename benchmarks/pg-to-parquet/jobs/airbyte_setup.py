"""Configure Airbyte OSS: Postgres source -> S3/MinIO Parquet destination, then sync.

Airbyte is deliberately not part of ./bench.sh. It needs a Kubernetes cluster via
abctl, it cannot write to a local filesystem, and a single sync takes tens of
minutes, so it is opt-in and run by hand.

Airbyte has no local-filesystem Parquet destination. destination-s3 against MinIO
is the only way to make it produce Parquet, which is a real difference from every
other tool in this comparison and is recorded as such rather than glossed over.

Credentials come from the environment. Get them with:

    abctl local credentials

    export AIRBYTE_CLIENT_ID=...
    export AIRBYTE_CLIENT_SECRET=...
    export AIRBYTE_WORKSPACE_ID=...
    python jobs/airbyte_setup.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("AIRBYTE_API", "http://localhost:8000/api/public/v1")
PG_HOST = os.environ.get("BENCH_PG_DOCKER_HOST", "host.docker.internal")
PG_PORT = int(os.environ.get("PGPORT", "15432"))
PG_DB = os.environ.get("PGDB", "bench")
PG_USER = os.environ.get("PGUSER", "bench")
PG_PASS = os.environ.get("PGPASS", "bench")
TABLE = os.environ.get("BENCH_TABLE", "lineitem")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://host.docker.internal:19000")
MINIO_USER = os.environ.get("MINIO_USER", "benchuser")
MINIO_PASS = os.environ.get("MINIO_PASS", "benchpass123")
BUCKET = os.environ.get("MINIO_BUCKET", "airbyte-parquet")


def env(name):
    value = os.environ.get(name)
    if not value:
        sys.exit(f"{name} is not set. Run 'abctl local credentials' and export it (see this file's docstring).")
    return value


def call(path, payload=None, token=None, method=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"{BASE}{path}", data=data, method=method or ("POST" if data else "GET"))
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            body = response.read().decode()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        print(f"HTTP {exc.code} on {path}: {exc.read().decode()[:600]}", file=sys.stderr)
        raise


def main():
    workspace = env("AIRBYTE_WORKSPACE_ID")
    token = call("/applications/token", {
        "client_id": env("AIRBYTE_CLIENT_ID"),
        "client_secret": env("AIRBYTE_CLIENT_SECRET"),
    })["access_token"]
    print("authenticated")

    source = call("/sources", {
        "name": "bench-postgres",
        "workspaceId": workspace,
        "configuration": {
            "sourceType": "postgres",
            "host": PG_HOST, "port": PG_PORT, "database": PG_DB,
            "username": PG_USER, "password": PG_PASS,
            "schemas": ["public"],
            "ssl_mode": {"mode": "disable"},
            "replication_method": {"method": "Xmin"},
            # Omitting tunnel_method makes the API reject the source with a 422.
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
        },
    }, token)
    print("source:", source["sourceId"])

    destination = call("/destinations", {
        "name": "bench-minio-parquet",
        "workspaceId": workspace,
        "configuration": {
            "destinationType": "s3",
            "access_key_id": MINIO_USER,
            "secret_access_key": MINIO_PASS,
            "s3_bucket_name": BUCKET,
            "s3_bucket_path": TABLE,
            "s3_bucket_region": "us-east-1",
            "s3_endpoint": MINIO_ENDPOINT,
            "format": {"format_type": "Parquet", "compression_codec": "SNAPPY"},
        },
    }, token)
    print("destination:", destination["destinationId"])

    connection = call("/connections", {
        "name": "bench-lineitem",
        "sourceId": source["sourceId"],
        "destinationId": destination["destinationId"],
        "configurations": {"streams": [
            {"name": TABLE, "namespace": "public", "syncMode": "full_refresh_overwrite"}
        ]},
        "schedule": {"scheduleType": "manual"},
    }, token)
    print("connection:", connection["connectionId"])

    started = time.time()
    job_id = call("/jobs", {"connectionId": connection["connectionId"], "jobType": "sync"}, token)["jobId"]
    print(f"job {job_id} started", flush=True)

    while True:
        time.sleep(20)
        job = call(f"/jobs/{job_id}", token=token)
        status = job.get("status")
        print(f"  [{time.time() - started:7.0f}s] status={status} rows={job.get('rowsSynced')}", flush=True)
        if status in ("succeeded", "failed", "cancelled", "incomplete"):
            elapsed = time.time() - started
            print(f"FINAL status={status} elapsed={elapsed:.2f}s "
                  f"rows={job.get('rowsSynced')} bytes={job.get('bytesSynced')}")
            break


if __name__ == "__main__":
    main()
