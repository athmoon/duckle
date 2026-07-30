# Postgres to Parquet benchmark

A reproducible harness that moves one TPC-H `lineitem` table out of Postgres and
into Parquet with several open-source ingestion tools, and times them under
identical conditions.

Everything here is designed so you can disagree with the numbers by re-running
them. If you get different results, the harness prints enough to say why.

## What it measures

Wall-clock time for a full-refresh extract of one wide, typed table, plus the
size of what each tool wrote. That is a deliberately narrow question. It says
nothing about incremental loads, CDC, connector coverage, transformation, or
operational features, and it should not be read as a general ranking.

**No timing is recorded until the output has been verified.** After every run the
harness reopens whatever the tool produced and checks both the row count and
`sum(l_orderkey)` against the source. A tool that writes a fast but wrong file is
recorded as a failure with no number attached.

## Requirements

- **bash** (Linux, macOS, WSL, or Git Bash on Windows)
- **Docker** with the compose plugin, for the Postgres container
- **DuckDB CLI 1.5 or newer**, used to generate the data, to talk to Postgres,
  and to verify every output. Found on `PATH`, or point `DUCKLE_DUCKDB_BIN` at it.

Each benchmarked tool is optional. Any tool that is not installed is skipped with
a note saying how to install it, so a partial comparison still works.

| Tool | Install |
|---|---|
| Duckle | `cargo build --release -p duckle-runner`, or set `DUCKLE_RUNNER` |
| DuckDB floor | included with the DuckDB CLI above |
| ingestr | `pip install ingestr` |
| dlt | `pip install "dlt[duckdb]" connectorx`, or a `.venv-dlt/` in this directory |
| sling | see the sling install docs |

## Quick start

```bash
./bench.sh up            # start Postgres
./bench.sh load          # generate the data (SF=1, ~6M rows, about 15s)
./bench.sh run           # run every tool that is installed
```

Or all three at once with `./bench.sh all`.

Results land in `results.tsv` and are printed as a table when the run finishes.

### Scaling up

`SF` is the TPC-H scale factor. SF1 is about 6M rows and is the right size for
checking that the harness works. The published numbers used SF17.

```bash
SF=17 ./bench.sh load     # ~96M rows, ~14 GB in Postgres, takes a while
REPEATS=3 ./bench.sh run
```

Generation runs in `SF` slices so peak memory stays at one slice rather than the
whole table. Budget roughly 20 GB of free disk at SF17 for Postgres plus the
largest tool output.

### Other knobs

```bash
./bench.sh run duckle floor      # only these tools
REPEATS=5 ./bench.sh run         # repeats for the close comparisons
PGPORT=5555 ./bench.sh run       # point at a different server
TABLE=lineitem_alt ./bench.sh load
./bench.sh clean                 # delete out/ and logs/
./bench.sh down                  # stop Postgres
```

`REPEATS` applies to Duckle and the DuckDB floor, which finish close enough
together that one sample cannot separate them. The tools that are multiples
slower run once or twice, because more repeats would cost an hour to sharpen a
gap that is not in doubt.

## How to read a result

- **`verified` must be `yes`.** Anything else means the output did not match the
  source, and the file is left on disk for you to inspect.
- **The DuckDB floor is not a competitor.** It is `postgres_scanner` plus
  `COPY TO`: no scheduling, no typing, no incremental state, no UI. It is there
  to show how much of the wall clock is the machine reading Postgres, and how
  much is the tool. Treat it as the floor, not as an entrant.
- **Output sizes are not normalised.** Tools pick their own compression. Duckle
  writes zstd, the others snappy. Compare sizes with that in mind.
- **ingestr has no Parquet destination** and writes a DuckDB file instead. Its
  time is comparable; its output size is not.

## What is not here

- **Airbyte** is not part of `./bench.sh`. It needs a Kubernetes cluster via
  `abctl`, it has no local-filesystem Parquet destination, and one sync takes
  tens of minutes. See below to run it by hand.
- **Meltano** has not been wired up at all. It belongs in this comparison and is
  simply missing, rather than having been run and omitted.

### Running Airbyte by hand

Airbyte can only produce Parquet through `destination-s3`, so this points it at a
MinIO container. That is a genuine structural difference from every other tool
here, not an implementation detail.

```bash
docker compose --profile airbyte up -d          # MinIO on :19000
abctl local install                             # installs Airbyte, slow
abctl local credentials                         # copy the values it prints

export AIRBYTE_CLIENT_ID=...
export AIRBYTE_CLIENT_SECRET=...
export AIRBYTE_WORKSPACE_ID=...
python jobs/airbyte_setup.py
```

Create the `airbyte-parquet` bucket in the MinIO console at `localhost:19001`
first. If a sync reports success but the bucket stays empty, the destination
never reached MinIO; check that the endpoint is reachable from inside the
Airbyte cluster rather than only from your shell.

## Files

| Path | What it is |
|---|---|
| `bench.sh` | the whole harness: up, load, run, verify, record |
| `docker-compose.yml` | Postgres, and MinIO behind the `airbyte` profile |
| `pipelines/duckle.json.tpl` | the Duckle pipeline, templated for connection details |
| `jobs/dlt_job.py` | dlt at its fastest documented config (connectorx, parquet) |
| `jobs/airbyte_setup.py` | opt-in Airbyte source, destination, connection and sync |
| `RESULTS.md` | the numbers this harness produced, and on what hardware |

## Notes for anyone porting this

Two things bit hard while building it, both worth knowing:

- `bc` does not exist in Git Bash. An earlier version timed everything with
  `echo "$end - $start" | bc` and silently recorded every duration as `0.00`.
  Timing here is integer milliseconds and shell arithmetic.
- Paths are kept relative on purpose. DuckDB's `.read`, sling's `file://` target
  and ingestr's `duckdb://` URI all split an absolute path at the first space,
  which breaks silently on any checkout under a directory with a space in it.
