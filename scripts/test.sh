#!/usr/bin/env bash
#
# End-to-end test of the orchestrator + two workers against the local Apify
# Actor runtime (see README.md for how to bring the runtime up).
#
#   Scenario 1 (happy path)  - both workers succeed; the orchestrator must
#                              SUCCEED, must hold a result from each worker, and
#                              must not finish before the slower worker does.
#   Scenario 2 (failure path) - worker-b fails on purpose; the orchestrator must
#                              FAIL instead of reporting a green result.
#
# Notes on the Apify CLI's I/O: with --json the machine-readable result goes to
# stdout, while the Actor's log goes to stderr. (--silent would suppress the
# JSON as well, so it is not used here.)

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
FAILURES=0

pass () { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail () { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAILURES=$((FAILURES + 1)); }

if ! apify runtime status 2>&1 | grep -q 'running on'; then
    echo "The local Actor runtime is not running. Start it with:"
    echo "    apify runtime start --detach && apify runtime connect"
    exit 1
fi

# call_orchestrator <input-json> <stdout-file>
call_orchestrator () {
    (cd "$ROOT/orchestrator" && apify call --json --input "$1" >"$2" 2>/dev/null)
}

json_get () { python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));
for k in sys.argv[2].split("."):
    d = d.get(k, {}) if isinstance(d, dict) else {}
print(d if isinstance(d,str) else "")' "$1" "$2" 2>/dev/null; }

# ---------------------------------------------------------------------------
echo
echo "Scenario 1: both workers succeed"
echo "--------------------------------"

call_orchestrator '{
  "jobName": "test-happy-path",
  "waitSecs": 180,
  "workerA": {"actorId": "worker-a", "input": {"taskName": "collect", "steps": 4, "stepDurationMs": 500}},
  "workerB": {"actorId": "worker-b", "input": {"taskName": "analyze", "steps": 6, "stepDurationMs": 1500}}
}' "$WORK/happy.json"

STATUS="$(json_get "$WORK/happy.json" run.status)"
KVS="$(json_get "$WORK/happy.json" storage.defaultKeyValueStoreId)"

if [ "$STATUS" = "SUCCEEDED" ]; then
    pass "orchestrator run SUCCEEDED"
else
    fail "orchestrator run status = '$STATUS' (expected SUCCEEDED)"
fi

if [ -n "$KVS" ] && apify api GET "v2/key-value-stores/$KVS/records/OUTPUT" >"$WORK/output.json" 2>/dev/null; then
    if ! python3 - "$WORK/output.json" <<'PY'
import json, sys

out = json.load(open(sys.argv[1]))
bad = 0

def check(ok, msg):
    global bad
    if ok:
        print(f"  \033[32mPASS\033[0m {msg}")
    else:
        print(f"  \033[31mFAIL\033[0m {msg}")
        bad += 1

workers = out["workers"]

check(len(workers) == 2, f"orchestrator collected results from 2 workers (got {len(workers)})")
check(all(w["status"] == "SUCCEEDED" for w in workers), "both worker runs SUCCEEDED")
check(all(w["output"] for w in workers), "both workers delivered an OUTPUT record")

# The orchestrator must not finish before the slowest worker has finished.
orch_end = out["finishedAt"]
slowest = max(w["output"]["finishedAt"] for w in workers)
check(orch_end >= slowest,
      f"orchestrator finished ({orch_end}) at/after the slowest worker ({slowest})")

# The workers should have run side by side, not one after the other.
overlap = out["concurrency"]["workersOverlappedMs"]
check(out["concurrency"]["ranConcurrently"],
      f"workers ran concurrently (busy windows overlapped by {overlap} ms)")

expected_items = sum(w["output"]["itemsProduced"] for w in workers)
check(out["totalItems"] == expected_items,
      f"merged dataset holds all {expected_items} items (got {out['totalItems']})")

expected_total = sum(w["output"]["sum"] for w in workers)
check(out["grandTotal"] == expected_total,
      f"grand total {out['grandTotal']} matches both workers combined ({expected_total})")

sys.exit(1 if bad else 0)
PY
    then
        FAILURES=$((FAILURES + 1))
    fi
else
    fail "could not read the orchestrator's OUTPUT record"
fi

# ---------------------------------------------------------------------------
echo
echo "Scenario 2: worker-b fails on purpose"
echo "-------------------------------------"

call_orchestrator '{
  "jobName": "test-failure-path",
  "waitSecs": 180,
  "workerA": {"actorId": "worker-a", "input": {"taskName": "collect", "steps": 2, "stepDurationMs": 300}},
  "workerB": {"actorId": "worker-b", "input": {"taskName": "analyze", "steps": 2, "stepDurationMs": 300, "failOnPurpose": true}}
}' "$WORK/failure.json"

STATUS="$(json_get "$WORK/failure.json" run.status)"

if [ "$STATUS" = "FAILED" ]; then
    pass "orchestrator FAILED because a worker did not deliver results"
else
    fail "orchestrator status = '$STATUS' (expected FAILED)"
fi

# ---------------------------------------------------------------------------
echo
if [ "$FAILURES" -eq 0 ]; then
    printf '\033[32mAll checks passed.\033[0m\n'
    exit 0
fi
printf '\033[31m%s check(s) failed.\033[0m\n' "$FAILURES"
exit 1
