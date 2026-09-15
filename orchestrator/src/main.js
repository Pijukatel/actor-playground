import { Actor, log } from 'apify';

/**
 * Orchestrator Actor.
 *
 * Starts two worker Actors concurrently, then blocks until BOTH of them have
 * finished and both of their results have actually been collected. It only
 * exits successfully when it holds the OUTPUT record of each worker; if either
 * worker fails, times out, or produces no OUTPUT, the orchestrator fails.
 */

const DEFAULTS = {
    workerA: { actorId: 'worker-a', input: { taskName: 'collect', steps: 5, stepDurationMs: 1000 } },
    workerB: { actorId: 'worker-b', input: { taskName: 'analyze', steps: 3, stepDurationMs: 2000 } },
};

await Actor.init();

try {
    const input = (await Actor.getInput()) ?? {};

    const jobName = input.jobName ?? 'demo-job';
    const waitSecs = Number(input.waitSecs ?? 300);
    const workerMemoryMbytes = input.workerMemoryMbytes ? Number(input.workerMemoryMbytes) : undefined;

    const workers = [
        {
            key: 'workerA',
            actorId: input.workerA?.actorId ?? DEFAULTS.workerA.actorId,
            input: input.workerA?.input ?? DEFAULTS.workerA.input,
        },
        {
            key: 'workerB',
            actorId: input.workerB?.actorId ?? DEFAULTS.workerB.actorId,
            input: input.workerB?.input ?? DEFAULTS.workerB.input,
        },
    ];

    const client = Actor.newClient();
    const orchestratorStartedAt = new Date();

    log.info(`orchestrator: job "${jobName}" - starting ${workers.length} workers concurrently`, {
        workers: workers.map((w) => w.actorId),
        waitSecs,
    });

    // ---------------------------------------------------------------------
    // Phase 1 - start every worker WITHOUT waiting.
    //
    // Actor.start() returns as soon as the run is created, so both workers are
    // in flight at the same time rather than one after the other.
    // ---------------------------------------------------------------------
    const started = await Promise.all(
        workers.map(async (worker) => {
            const run = await Actor.start(worker.actorId, worker.input, {
                ...(workerMemoryMbytes ? { memory: workerMemoryMbytes } : {}),
            });

            log.info(`orchestrator: started ${worker.key} (${worker.actorId}) as run ${run.id}`);

            return { ...worker, runId: run.id, startedRun: run };
        }),
    );

    // ---------------------------------------------------------------------
    // Phase 2 - wait for ALL of them, then collect each one's results.
    //
    // Promise.all only settles once every worker has finished AND its OUTPUT
    // has been read, so the orchestrator cannot finish early.
    // ---------------------------------------------------------------------
    const results = await Promise.all(
        started.map(async (worker) => {
            log.info(`orchestrator: waiting for ${worker.key} (run ${worker.runId})...`);

            const finishedRun = await client.run(worker.runId).waitForFinish({ waitSecs });

            if (finishedRun.status !== 'SUCCEEDED') {
                log.warning(`orchestrator: ${worker.key} ended with status ${finishedRun.status}`);
                return { ...worker, run: finishedRun, status: finishedRun.status, output: null, items: [] };
            }

            // Collect the worker's results: its OUTPUT record and its dataset.
            const outputRecord = await client
                .keyValueStore(finishedRun.defaultKeyValueStoreId)
                .getRecord('OUTPUT');

            const { items } = await client.dataset(finishedRun.defaultDatasetId).listItems();

            log.info(`orchestrator: collected ${worker.key} - ${items.length} items`, {
                status: finishedRun.status,
                sum: outputRecord?.value?.sum,
            });

            return {
                ...worker,
                run: finishedRun,
                status: finishedRun.status,
                output: outputRecord?.value ?? null,
                items,
            };
        }),
    );

    // ---------------------------------------------------------------------
    // Phase 3 - verify we really do hold a result from every worker.
    // ---------------------------------------------------------------------
    const failed = results.filter((r) => r.status !== 'SUCCEEDED');
    const missingOutput = results.filter((r) => r.status === 'SUCCEEDED' && r.output === null);

    if (failed.length > 0 || missingOutput.length > 0) {
        const reasons = [
            ...failed.map((r) => `${r.key} (${r.actorId}) ended as ${r.status}`),
            ...missingOutput.map((r) => `${r.key} (${r.actorId}) produced no OUTPUT record`),
        ];
        throw new Error(`orchestrator: not all workers delivered results -> ${reasons.join('; ')}`);
    }

    const orchestratorFinishedAt = new Date();

    // ---------------------------------------------------------------------
    // Phase 4 - merge both workers' results into one combined result.
    // ---------------------------------------------------------------------
    const combinedItems = results.flatMap((r) => r.items);

    const workerStarts = results.map((r) => new Date(r.output.startedAt).getTime());
    const workerEnds = results.map((r) => new Date(r.output.finishedAt).getTime());
    // If the workers really ran side by side, their busy windows overlap.
    const overlapMs = Math.max(0, Math.min(...workerEnds) - Math.max(...workerStarts));
    const sequentialMs = results.reduce((acc, r) => acc + r.output.durationMs, 0);

    const summary = {
        jobName,
        orchestratorRunId: Actor.getEnv().actorRunId,
        startedAt: orchestratorStartedAt.toISOString(),
        finishedAt: orchestratorFinishedAt.toISOString(),
        durationMs: orchestratorFinishedAt.getTime() - orchestratorStartedAt.getTime(),
        workersRequested: results.length,
        workersSucceeded: results.length,
        totalItems: combinedItems.length,
        grandTotal: results.reduce((acc, r) => acc + r.output.sum, 0),
        concurrency: {
            // Proof the workers overlapped instead of running one after the other.
            workersOverlappedMs: overlapMs,
            ranConcurrently: overlapMs > 0,
            sumOfWorkerDurationsMs: sequentialMs,
        },
        workers: results.map((r) => ({
            key: r.key,
            actorId: r.actorId,
            runId: r.runId,
            status: r.status,
            output: r.output,
            itemsCollected: r.items.length,
        })),
    };

    await Actor.setValue('OUTPUT', summary);
    await Actor.setValue('COMBINED_ITEMS', combinedItems);
    await Actor.pushData(combinedItems);

    log.info('orchestrator: all workers delivered results', {
        totalItems: summary.totalItems,
        grandTotal: summary.grandTotal,
        ranConcurrently: summary.concurrency.ranConcurrently,
        workersOverlappedMs: summary.concurrency.workersOverlappedMs,
    });

    await Actor.exit(
        `Job "${jobName}" done: ${summary.workersSucceeded}/${summary.workersRequested} workers, `
        + `${summary.totalItems} items, grand total ${summary.grandTotal}.`,
    );
} catch (err) {
    log.exception(err, 'orchestrator: failed');
    await Actor.fail(err instanceof Error ? err.message : String(err));
}
