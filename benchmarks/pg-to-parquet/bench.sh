#!/usr/bin/env bash
# Postgres -> Parquet benchmark harness.
#
#   ./bench.sh up            start Postgres
#   ./bench.sh load          generate TPC-H lineitem into it   (SF=1 default)
#   ./bench.sh run [tool..]  run tools, verify output, record timings
#   ./bench.sh all           up + load + run
#   ./bench.sh down          stop Postgres
#   ./bench.sh clean         delete out/ and logs/
#
# Every tool is timed the same way, and no timing is recorded until the output
# has been reopened and checked for the right row count and the right
# sum(l_orderkey). A tool that produces a wrong or missing file is recorded as
# a failure with no number, because an unverified number is worse than none.
#
# Config, all overridable from the environment:
#   SF=17 ./bench.sh load          scale factor (SF1 ~= 6M rows, SF17 ~= 96M)
#   REPEATS=3 ./bench.sh run       repeats for the close comparisons
#   PGPORT=15432 PGDB=bench ...    connection details
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE"

SF=${SF:-1}
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-15432}
PGDB=${PGDB:-bench}
PGUSER=${PGUSER:-bench}
PGPASS=${PGPASS:-bench}
TABLE=${TABLE:-lineitem}
CONTAINER=${CONTAINER:-bench-pg}
REPEATS=${REPEATS:-3}
MEMLIMIT=${MEMLIMIT:-3GB}
# Deliberately relative. The harness cd's to its own directory, and several
# tools (duckdb's .read, sling's file:// target, ingestr's duckdb:// URI) split
# an absolute path on the first space, which silently breaks on any checkout
# living under a path like /home/some user/.
OUT=out
LOGS=logs
RESULTS=${RESULTS:-$HERE/results.tsv}

PGCONN="host=$PGHOST port=$PGPORT user=$PGUSER password=$PGPASS dbname=$PGDB"
PGURI="postgresql://$PGUSER:$PGPASS@$PGHOST:$PGPORT/$PGDB"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

# ---------------------------------------------------------------- timing ----
# A previous version of this harness used `echo "$e - $s" | bc`. Git Bash has no
# bc, so every recorded time silently came out as 0.00. Integer milliseconds and
# shell arithmetic have no such dependency.
_probe=$(date +%s%3N 2>/dev/null || true)
case "$_probe" in
  '' | *[!0-9]*)
    if have python3; then TIMER=python3
    elif have python; then TIMER=python
    else TIMER=seconds; log "WARNING: no ms-resolution clock, timings rounded to 1s"
    fi ;;
  *) TIMER=date ;;
esac

now_ms() {
  case $TIMER in
    date)    date +%s%3N ;;
    seconds) echo $(( $(date +%s) * 1000 )) ;;
    *)       "$TIMER" -c 'import time;print(int(time.time()*1000))' ;;
  esac
}
fmt_s() { printf '%d.%02d' $(( $1 / 1000 )) $(( ($1 % 1000) / 10 )); }

# portable byte size for a file or a directory
pathsize() {
  [ -e "$1" ] || { echo 0; return; }
  if [ -d "$1" ]; then
    echo $(( $(du -sk "$1" | cut -f1) * 1024 ))
  elif stat -c %s "$1" >/dev/null 2>&1; then
    stat -c %s "$1"
  else
    stat -f%z "$1"
  fi
}

# ------------------------------------------------------------ discovery ----
# Version-aware, because a plain glob returns .duckdb-cli-v1.2.2 before
# .duckdb-cli-v1.5.4 and the older CLI is silently too old for the runner
# ("unknown option: -storage-version").
if printf '1.10\n1.9\n' | sort -Vr >/dev/null 2>&1; then SORTV="sort -Vr"; else SORTV="sort -r"; fi
find_duckdb() {
  local c
  if [ -n "${DUCKLE_DUCKDB_BIN:-}" ] && [ -x "${DUCKLE_DUCKDB_BIN}" ]; then
    printf '%s\n' "$DUCKLE_DUCKDB_BIN"; return 0
  fi
  c=$(command -v duckdb 2>/dev/null || true)
  [ -n "$c" ] && { printf '%s\n' "$c"; return 0; }
  c=$(ls -d "$HERE"/../../.duckdb-cli-*/duckdb "$HERE"/../../.duckdb-cli-*/duckdb.exe 2>/dev/null \
      | $SORTV | head -1)
  [ -n "$c" ] && [ -x "$c" ] && { printf '%s\n' "$c"; return 0; }
  return 1
}
find_runner() {
  local c
  for c in "${DUCKLE_RUNNER:-}" "$(command -v duckle-runner 2>/dev/null || true)" \
           "$HERE/../../target/release/duckle-runner" "$HERE/../../target/release/duckle-runner.exe"; do
    [ -n "$c" ] && [ -x "$c" ] && { printf '%s\n' "$c"; return 0; }
  done
  return 1
}
find_dlt_python() {
  local c
  for c in "$HERE/.venv-dlt/bin/python" "$HERE/.venv-dlt/Scripts/python.exe" \
           "$(command -v python3 2>/dev/null || true)"; do
    [ -n "$c" ] && [ -x "$c" ] && "$c" -c 'import dlt' >/dev/null 2>&1 \
      && { printf '%s\n' "$c"; return 0; }
  done
  return 1
}

DUCKDB=$(find_duckdb) || die "no duckdb binary found. Install it, or set DUCKLE_DUCKDB_BIN."
export DUCKLE_DUCKDB_BIN="$DUCKDB"

# Postgres is queried through DuckDB's postgres extension rather than psql, so
# the harness needs no client install and works against any reachable server.
pg_query() {
  "$DUCKDB" -noheader -list -c "
    INSTALL postgres; LOAD postgres;
    ATTACH '$PGCONN' AS pg (TYPE postgres, READ_ONLY);
    $1" 2>/dev/null | tail -1
}
warm() { pg_query "SELECT count(*) FROM pg.public.$TABLE;" >/dev/null 2>&1; }

# ------------------------------------------------------------------- up ----
cmd_up() {
  have docker || die "docker is required for 'up'. Point PGHOST/PGPORT at your own server instead."
  log "starting Postgres"
  docker compose up -d 2>&1 | tail -2
  log "waiting for it to accept connections"
  local i
  for i in $(seq 1 60); do
    pg_query "SELECT 1;" >/dev/null 2>&1 && { log "Postgres is up on $PGHOST:$PGPORT"; return 0; }
    sleep 2
  done
  die "Postgres did not come up within 120s"
}
cmd_down() { docker compose down 2>&1 | tail -2; }

# ----------------------------------------------------------------- load ----
# dbgen is generated in `children` slices so peak memory stays at one slice
# instead of the whole table. At SF17 that is ~5.6M rows per slice.
cmd_load() {
  local children=$SF step action n rc try
  [ "$children" -lt 1 ] && children=1
  log "generating TPC-H lineitem at SF=$SF in $children slices (this is the slow part)"

  for step in $(seq 0 $((children - 1))); do
    if [ "$step" -eq 0 ]; then
      action="CREATE OR REPLACE TABLE pg.$TABLE AS SELECT * FROM lineitem;"
    else
      action="INSERT INTO pg.$TABLE SELECT * FROM lineitem;"
    fi
    # Slice generation failed intermittently during development and the errors
    # were being swallowed by a redirect, so failures are surfaced and retried
    # once rather than silently producing a short table.
    for try in 1 2; do
      "$DUCKDB" -c "
        SET memory_limit='$MEMLIMIT';
        INSTALL tpch; LOAD tpch; INSTALL postgres; LOAD postgres;
        CALL dbgen(sf=$SF, children=$children, step=$step);
        ATTACH '$PGCONN' AS pg (TYPE postgres);
        $action" >"$LOGS/load_$step.log" 2>&1
      rc=$?
      [ $rc -eq 0 ] && break
      log "slice $((step + 1))/$children failed (attempt $try), see logs/load_$step.log"
    done
    [ $rc -eq 0 ] || { tail -5 "$LOGS/load_$step.log" >&2; die "slice $((step + 1)) failed twice"; }
    n=$(pg_query "SELECT count(*) FROM pg.public.$TABLE;")
    log "slice $((step + 1))/$children done, $n rows"
  done

  log "VACUUM ANALYZE"
  "$DUCKDB" -c "
    INSTALL postgres; LOAD postgres;
    ATTACH '$PGCONN' AS pg (TYPE postgres);
    CALL postgres_execute('pg', 'VACUUM ANALYZE $TABLE');" >/dev/null 2>&1
  cmd_info
}

cmd_info() {
  local v rows ck
  v=$(pg_query "SELECT count(*)||' '||sum(l_orderkey) FROM pg.public.$TABLE;")
  rows=${v%% *}; ck=${v##* }
  [ -n "$rows" ] || die "cannot read $TABLE. Has 'load' been run?"
  printf '%s\t%s\n' "$rows" "$ck" > "$HERE/dataset.txt"
  log "dataset: $rows rows, checksum(sum l_orderkey) $ck"
}

# --------------------------------------------------------------- verify ----
# Reopens a tool's output and compares it against the source. Nothing is
# recorded as a success unless both the row count and the checksum match.
EXPECT_ROWS=""; EXPECT_CK=""
load_expectations() {
  [ -f "$HERE/dataset.txt" ] || cmd_info
  EXPECT_ROWS=$(cut -f1 "$HERE/dataset.txt")
  EXPECT_CK=$(cut -f2 "$HERE/dataset.txt")
}

# verify <full-sql>  ->  echoes "rows checksum"; empty if the output is unreadable.
# Takes complete SQL rather than a FROM-target because a DuckDB file has to be
# ATTACHed, it cannot be read as a path literal the way parquet can.
CKSEL="count(*)||' '||sum(l_orderkey)"
verify() { "$DUCKDB" -noheader -list -c "$1" 2>/dev/null | tail -1; }
pq_read() { printf "SELECT %s FROM read_parquet('%s');" "$CKSEL" "$1"; }

record() { # tool run seconds rows ok bytes
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" "$6" >> "$RESULTS"
  log "  $1 run$2: ${3}s rows=$4 verified=$5"
}

# run_one <tool> <run#> <reader-sql> <output-path> <command...>
run_one() {
  local tool=$1 idx=$2 reader=$3 target=$4; shift 4
  rm -rf "$target" "$target.wal"; warm
  local s e ms v rows ck
  s=$(now_ms); "$@" >"$LOGS/${tool}_$idx.log" 2>&1; local rc=$?
  e=$(now_ms); ms=$((e - s))
  if [ $rc -ne 0 ] || [ ! -e "$target" ]; then
    record "$tool" "$idx" "$(fmt_s $ms)" 0 "FAILED(rc=$rc)" 0
    log "  see logs/${tool}_$idx.log"
    return 1
  fi
  v=$(verify "$reader"); rows=${v%% *}; ck=${v##* }
  if [ "$rows" = "$EXPECT_ROWS" ] && [ "$ck" = "$EXPECT_CK" ]; then
    record "$tool" "$idx" "$(fmt_s $ms)" "$rows" yes "$(pathsize "$target")"
    rm -rf "$target"   # only on success, so disk stays bounded across tools
  else
    record "$tool" "$idx" "$(fmt_s $ms)" "${rows:-0}" "MISMATCH(ck=${ck:-unreadable})" "$(pathsize "$target")"
    log "  output kept at $target for inspection"
    return 1
  fi
}

# ----------------------------------------------------------------- tools ----
tool_duckle() {
  local runner; runner=$(find_runner) || { log "SKIP duckle: no duckle-runner (cargo build --release -p duckle-runner)"; return; }
  # Generated beside the harness, not inside out/, so the runner treats the
  # harness directory as the workspace rather than the output directory.
  sed -e "s|__HOST__|$PGHOST|; s|__PORT__|$PGPORT|; s|__DB__|$PGDB|; s|__USER__|$PGUSER|; \
          s|__PASS__|$PGPASS|; s|__TABLE__|$TABLE|; s|__OUT__|$OUT/duckle.parquet|" \
      pipelines/duckle.json.tpl > .duckle_pipeline.json
  local i; for i in $(seq 1 "$REPEATS"); do
    run_one duckle "$i" "$(pq_read "$OUT/duckle.parquet")" "$OUT/duckle.parquet" \
      "$runner" --pipeline .duckle_pipeline.json
  done
}

# Raw postgres_scanner + COPY TO. Not an ETL tool and not a competitor: no
# scheduling, typing, incremental state or UI. It is the floor of the machine.
tool_floor() {
  cat > "$OUT/.floor.sql" <<SQL
INSTALL postgres; LOAD postgres;
ATTACH '$PGCONN' AS pg (TYPE postgres, READ_ONLY);
COPY (SELECT * FROM pg.public.$TABLE) TO '$OUT/floor.parquet' (FORMAT parquet);
SQL
  local i; for i in $(seq 1 "$REPEATS"); do
    run_one floor "$i" "$(pq_read "$OUT/floor.parquet")" "$OUT/floor.parquet" \
      "$DUCKDB" -c ".read $OUT/.floor.sql"
  done
}

# ingestr has no parquet destination, so it writes a DuckDB file. Its size is
# therefore not comparable with the parquet writers; its time is.
tool_ingestr() {
  have ingestr || { log "SKIP ingestr: not installed (pip install ingestr)"; return; }
  local read_sql="ATTACH '$OUT/ingestr.duckdb' AS ig (READ_ONLY); SELECT $CKSEL FROM ig.main.$TABLE;"
  local i; for i in 1 2; do
    run_one ingestr "$i" "$read_sql" "$OUT/ingestr.duckdb" \
      ingestr ingest --source-uri "$PGURI?sslmode=disable" --source-table "public.$TABLE" \
        --dest-uri "duckdb://$OUT/ingestr.duckdb" --dest-table "main.$TABLE" --yes
  done
}

tool_dlt() {
  local py; py=$(find_dlt_python) || { log "SKIP dlt: no python with dlt (pip install 'dlt[duckdb]' connectorx)"; return; }
  export BENCH_PG_URI="$PGURI" BENCH_OUT="$OUT/dlt" BENCH_TABLE="$TABLE"
  local i; for i in 1 2; do
    run_one dlt "$i" "$(pq_read "$OUT/dlt/**/*.parquet")" "$OUT/dlt" \
      "$py" jobs/dlt_job.py
  done
}

tool_sling() {
  have sling || { log "SKIP sling: not installed"; return; }
  run_one sling 1 "$(pq_read "$OUT/sling.parquet")" "$OUT/sling.parquet" \
    sling run --src-conn "$PGURI?sslmode=disable" --src-stream "public.$TABLE" \
      --tgt-object "file://$OUT/sling.parquet" --tgt-options '{"format": "parquet"}'
}

# ------------------------------------------------------------------ run ----
cmd_run() {
  mkdir -p "$OUT" "$LOGS"
  load_expectations
  log "expecting $EXPECT_ROWS rows, checksum $EXPECT_CK"
  printf 'tool\trun\tseconds\trows\tverified\toutput_bytes\n' > "$RESULTS"

  local tools="$*"
  [ -z "$tools" ] && tools="duckle floor ingestr dlt sling"
  local t; for t in $tools; do
    log "=== $t ==="
    case $t in
      duckle)  tool_duckle ;;
      floor)   tool_floor ;;
      ingestr) tool_ingestr ;;
      dlt)     tool_dlt ;;
      sling)   tool_sling ;;
      *)       log "unknown tool '$t' (duckle floor ingestr dlt sling)" ;;
    esac
  done
  log "done. results in $(basename "$RESULTS")"
  echo; column -t -s "$(printf '\t')" "$RESULTS" 2>/dev/null || cat "$RESULTS"
}

cmd_clean() { rm -rf "$OUT" "$LOGS"; log "removed out/ and logs/"; }

mkdir -p "$OUT" "$LOGS"
case "${1:-}" in
  up)    cmd_up ;;
  load)  cmd_load ;;
  run)   shift; cmd_run "$@" ;;
  all)   cmd_up && cmd_load && cmd_run ;;
  info)  cmd_info ;;
  down)  cmd_down ;;
  clean) cmd_clean ;;
  *) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
