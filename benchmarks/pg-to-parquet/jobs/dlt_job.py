"""dlt: Postgres -> Parquet, at dlt's best-documented configuration.

backend="connectorx" is dlt's fastest sql_database backend and the one their own
docs point at for large extracts. loader_file_format="parquet" skips the
intermediate JSONL that the default insert-values path would otherwise write.

Connection details come from the environment so the harness stays the single
source of truth for them.
"""
import os

import dlt
from dlt.sources.sql_database import sql_database

CONN = os.environ["BENCH_PG_URI"]
OUT = os.environ.get("BENCH_OUT", "out/dlt")
TABLE = os.environ.get("BENCH_TABLE", "lineitem")


def main():
    pipeline = dlt.pipeline(
        pipeline_name="pg_to_parquet",
        destination=dlt.destinations.filesystem(OUT),
        dataset_name="bench",
        progress="log",
    )
    source = sql_database(CONN, table_names=[TABLE], backend="connectorx", chunk_size=100_000)
    print(pipeline.run(source, loader_file_format="parquet", write_disposition="replace"))


if __name__ == "__main__":
    main()
