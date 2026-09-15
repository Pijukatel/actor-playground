# Apify multi-Actor orchestration playground

Three Actors that work together, developed and tested entirely on the **local
Apify Actor runtime** (no cloud account, no deploy):

| Actor | Role |
|---|---|
| `orchestrator` | Starts both workers **concurrently** and finishes only once it holds the results of **both**. |
| `worker-a` | Simulated worker ("collector"). Burns time in steps, one dataset item per step. |
| `worker-b` | Simulated worker ("analyzer"). Same, with different timings and values. |

The workers do no real work — they just sleep for a configurable number of
steps. The point of the setup is the orchestration, not the payload.

## What the orchestrator guarantees

1. **Both workers run side by side.** `Actor.start()` returns as soon as a run
   is created, so both workers are in flight at the same time rather than one
   after the other.
2. **It cannot finish early.** It waits for every worker with
   `waitForFinish()` and then reads each worker's `OUTPUT` record and dataset.
   Both the wait and the collection sit inside a single `Promise.all`, so the
   orchestrator only proceeds once every worker has finished *and* its results
   have been read.
3. **It fails loudly on a missing result.** If a worker ends in any state other
   than `SUCCEEDED`, or succeeds but writes no `OUTPUT`, the orchestrator fails
   instead of reporting a green result over incomplete data.

Every run records the evidence in its own `OUTPUT`: each worker's busy window,
how long the two windows overlapped (`concurrency.workersOverlappedMs`), and
the merged item count and totals.

## Requirements

- Docker (or Podman) running
- `npm i -g apify-cli@1.10.1-runtime.4`

## Bring the runtime up

```sh
apify runtime install          # pulls apify/actor-runtime:latest
apify runtime start --detach   # API on :3333, console on :3000
apify runtime connect          # point the CLI at the runtime, not the cloud
apify login --token local-dev-token   # any non-empty token works locally
```

In a sandbox with no OS keyring, `export APIFY_DISABLE_KEYRING=1` before logging in.

## Build and run

```sh
# once per Actor - uploads and builds, and registers the directory as the
# Actor's live dev folder, so later source edits need no rebuild
(cd worker-a && apify push)
(cd worker-b && apify push)
(cd orchestrator && apify push)

# run the whole thing
cd orchestrator
apify call --input '{
  "jobName": "demo",
  "workerA": {"actorId": "worker-a", "input": {"steps": 4, "stepDurationMs": 500}},
  "workerB": {"actorId": "worker-b", "input": {"steps": 6, "stepDurationMs": 1500}}
}'
```

Only `package.json` changes need another `apify push`; source edits are picked
up by the next run.

Inspect the result in the console at <http://localhost:3000>, or from the CLI:

```sh
apify runs ls                                          # from an Actor directory
apify api GET "v2/key-value-stores/<kvsId>/records/OUTPUT"
```

## Tests

```sh
./scripts/test.sh
```

- **Scenario 1 — happy path.** Both workers succeed. Asserts the orchestrator
  succeeded, collected an `OUTPUT` from each worker, did not finish before the
  slower worker, ran the workers concurrently, and merged every item.
- **Scenario 2 — failure path.** `worker-b` is told to fail via
  `failOnPurpose`. Asserts the orchestrator **fails** rather than reporting
  success on half the data.

Sample output:

```
Scenario 1: both workers succeed
  PASS orchestrator run SUCCEEDED
  PASS orchestrator collected results from 2 workers (got 2)
  PASS both worker runs SUCCEEDED
  PASS both workers delivered an OUTPUT record
  PASS orchestrator finished (07:28:50.671Z) at/after the slowest worker (07:28:50.277Z)
  PASS workers ran concurrently (busy windows overlapped by 2042 ms)
  PASS merged dataset holds all 10 items (got 10)
  PASS grand total 2200 matches both workers combined (2200)

Scenario 2: worker-b fails on purpose
  PASS orchestrator FAILED because a worker did not deliver results
```

## A note on the CLI's output streams

`apify call --json` writes the machine-readable result to **stdout** while the
Actor's log goes to **stderr**, and the result is nested (`.run.status`,
`.storage.defaultKeyValueStoreId`). `--silent` suppresses the JSON as well, so
`scripts/test.sh` does not use it.

## Layout

```
orchestrator/    starts both workers, waits for both, merges results
worker-a/        simulated worker ("collector")
worker-b/        simulated worker ("analyzer")
scripts/test.sh  end-to-end tests (happy path + failure path)
```

Each Actor carries its own `Dockerfile` whose `CMD` invokes `node` directly
rather than `npm start`, which is what the runtime's debug mode requires in
order to attach to the Actor process.
