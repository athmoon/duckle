#!/usr/bin/env bash
# Measure whether N hosts sharing one workspace ever run a batch item twice.
#
# Until this has been run against real hosts on a real shared mount, Duckle's
# docs say multi-host is the design intent and NOT that it works. This script is
# what turns that into a measurement, and it is deliberately a measurement
# rather than a demo: it counts duplicate executions and fails if there are any.
#
# WHAT IT NEEDS
#   - A workspace on a filesystem every host mounts at the SAME path.
#   - duckle-runner on each host, same version.
#   - Passwordless ssh to each host (or run the worker leg by hand on each).
#
# USAGE
#   ./measure-multi-host-batch.sh /mnt/shared/ws host-a host-b [host-c ...]
#
# WHAT IT PROVES, AND WHAT IT DOES NOT
#   It proves that for THIS run, on THIS mount, no item was executed twice. It
#   cannot prove that in general - no test can - but a failure here is
#   conclusive: it means the lock does not exclude on that filesystem, and
#   sharing a batch across those hosts would silently multiply every load.
set -euo pipefail

WS="${1:?usage: measure-multi-host-batch.sh <shared-workspace> <host> [host...]}"
shift
HOSTS=("$@")
[ "${#HOSTS[@]}" -ge 2 ] || { echo "need at least two hosts to measure anything"; exit 2; }

say() { printf '\n=== %s ===\n' "$*"; }

say "1/4 every host must agree the lock excludes on this mount"
# Each host runs the same preflight the worker runs. A host that cannot exclude
# is the whole answer: stop before running anything.
for h in "${HOSTS[@]}"; do
    printf '  %-20s ' "$h"
    ssh "$h" "duckle-runner work --workspace '$WS' --check" || {
        echo "  ^ this host cannot lock safely on that mount. Stopping."
        exit 1
    }
done

say "2/4 the batch to measure"
BATCH=$(ls -1 "$WS"/batches/*.ndjson 2>/dev/null | grep -v '\.ledger\.' | head -1 || true)
[ -n "$BATCH" ] || { echo "no batch in $WS/batches - queue one first (For Each -> Queue for workers)"; exit 2; }
BATCH_ID=$(basename "$BATCH" .ndjson)
ITEMS=$(grep -c . "$BATCH")
echo "  $BATCH_ID with $ITEMS item(s)"
# Start from a clean ledger so the count below is of THIS run.
rm -f "$WS/batches/$BATCH_ID.ledger.ndjson"

say "3/4 all hosts work the batch at once"
for h in "${HOSTS[@]}"; do
    ssh "$h" "duckle-runner work --workspace '$WS' --batch '$BATCH_ID'" &
done
wait

say "4/4 count duplicate executions"
LEDGER="$WS/batches/$BATCH_ID.ledger.ndjson"
[ -f "$LEDGER" ] || { echo "no ledger was written - nothing ran"; exit 1; }

# One line per successful execution. An item executed twice appears twice, and
# that is the number that matters: the ledger is the only record of what
# actually ran, so a duplicate here is a duplicate load.
TOTAL_OK=$(grep -c '"status":"ok"' "$LEDGER" || true)
DISTINCT=$(grep -o '"index":[0-9]*' "$LEDGER" | sort -u | wc -l | tr -d ' ')
DUPES=$((TOTAL_OK - DISTINCT))

echo "  items in batch      : $ITEMS"
echo "  successful runs     : $TOTAL_OK"
echo "  distinct items run  : $DISTINCT"
echo "  DUPLICATE EXECUTIONS: $DUPES"
echo
echo "  per host:"
grep -o '"worker":"[^"]*"' "$LEDGER" | sort | uniq -c | sed 's/^/    /'

if [ "$DUPES" -ne 0 ]; then
    echo
    echo "FAILED: $DUPES item(s) ran more than once. The lock does not exclude across"
    echo "these hosts on this mount. Do NOT share a batch across them."
    exit 1
fi
echo
echo "PASSED: $DISTINCT items, each run exactly once, across ${#HOSTS[@]} hosts."
echo "Record the mount type and host count alongside this result - it is evidence"
echo "about THAT filesystem, not about shared filesystems in general."
