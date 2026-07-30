# Results

Numbers this harness produced. Reproduce them with `SF=17 ./bench.sh load` then
`REPEATS=3 ./bench.sh run`, and see `README.md` for how to read them.

## Hardware and versions

| | |
|---|---|
| CPU | Intel Core i7-13650HX, 14 cores / 20 threads |
| RAM | 24 GB |
| Disk | NVMe SSD |
| OS | Windows 11, Docker Desktop |
| Postgres | 16, in Docker, settings as in `docker-compose.yml` |
| DuckDB | 1.5.4 |
| Duckle | 0.5.8 |
| ingestr | 1.1.11 |
| dlt | 1.29.1, connectorx backend |
| sling | 1.5.22 |

A single laptop is the point rather than a limitation: it is the environment
these tools are most often actually run in. Results on a server with more memory
bandwidth will differ, and the ratios matter more than the absolute times.

## SF17, 95,988,640 rows, 14 GB in Postgres

Source checksum `sum(l_orderkey)` = `4607672239254702`, matched by every run
below.

| Tool | Wall clock | vs Duckle | Output | Notes |
|---|---|---|---|---|
| Duckle 0.5.8 | **39.9 s** | - | 2.52 GB | parquet, zstd. Median of 3: 41.8 / 39.9 / 38.1 |
| DuckDB floor | 44.2 s | 1.11x | 3.58 GB | parquet, snappy. Median of 3: 44.2 / 45.7 / 42.3. Not an ETL tool |
| ingestr 1.1.11 | 120.8 s | 3.0x | 2.92 GB | DuckDB file, not parquet. Size not comparable |
| dlt 1.29.1 | 493.6 s | 12.4x | 3.93 GB | parquet, snappy |
| sling 1.5.22 | 1,897 s | 47.5x | 4.45 GB | parquet, snappy. Time derived from its own log timestamps |

Airbyte and Meltano are absent. Airbyte has no local Parquet destination and has
only been run against an earlier synthetic dataset, so it has no honest entry
here; Meltano was never wired up.

## SF1, 6,001,215 rows

Kept as a fast check that the harness itself works. Source checksum
`18005322964949`.

| Tool | Wall clock | Output |
|---|---|---|
| Duckle | 1.65 s | 149.6 MB |
| DuckDB floor | 1.91 s | 207.1 MB |
| ingestr | 7.66 s | 180.9 MB |
| dlt | 19.03 s | 226.0 MB |
| sling | 88.25 s | 261.7 MB |

The ordering is the same at both scales, which is a useful sanity check: a
harness that reversed any pair between SF1 and SF17 would be measuring itself
rather than the tools.

## Two things worth knowing about these numbers

**The dataset has to be real TPC-H.** An earlier version of this benchmark
generated synthetic data with a sequential `l_orderkey`. It delta-encoded to
almost nothing, so every tool's Parquet came out at 6.65 bytes/row against real
TPC-H's 24.89 - a 3.7x flattering distortion of the output-size comparison. All
numbers above come from the real `dbgen`.

**Cache state has to be equal.** In an early run the DuckDB floor was measured
cold at 85.3 s while Duckle ran warm at 38.1 s, which would have been a 2.2x
claim built entirely on page cache. Every run in this harness is preceded by the
same warm-up scan, and the floor's honest warm number is 44.2 s.

The gap between Duckle and the floor narrowed from 1.5x on the synthetic data to
1.11x on real TPC-H. That is the more interesting result, and it is the one that
survives scrutiny.
