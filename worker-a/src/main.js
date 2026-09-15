import { Actor, log } from 'apify';

/**
 * Simulated worker Actor.
 *
 * It does not do any real work - it just burns a configurable amount of time in
 * a number of steps, pushing one dataset item per step, and finally writes a
 * summary to the default key-value store under OUTPUT. That OUTPUT record is
 * what the orchestrator Actor waits for and collects.
 */

const WORKER_NAME = 'worker-a';
const WORKER_ROLE = 'collector';
const VALUE_MULTIPLIER = 10;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

await Actor.init();

try {
    const input = (await Actor.getInput()) ?? {};

    const taskName = input.taskName ?? 'unnamed-task';
    const steps = Number(input.steps ?? 5);
    const stepDurationMs = Number(input.stepDurationMs ?? 1000);
    const failOnPurpose = Boolean(input.failOnPurpose);

    log.info(`${WORKER_NAME}: starting simulated work`, {
        taskName,
        steps,
        stepDurationMs,
        runId: Actor.getEnv().actorRunId,
    });

    const startedAt = new Date();
    const items = [];

    for (let step = 1; step <= steps; step += 1) {
        // This is the "work". Nothing real happens, we just wait.
        await sleep(stepDurationMs);

        const item = {
            worker: WORKER_NAME,
            role: WORKER_ROLE,
            taskName,
            step,
            value: step * VALUE_MULTIPLIER,
            processedAt: new Date().toISOString(),
        };

        items.push(item);
        await Actor.pushData(item);

        log.info(`${WORKER_NAME}: step ${step}/${steps} done (value=${item.value})`);
    }

    if (failOnPurpose) {
        // Used by the failure-path test, to prove the orchestrator notices a
        // worker that did not succeed instead of reporting a green result.
        throw new Error(`${WORKER_NAME}: deliberate failure requested via input.failOnPurpose`);
    }

    const finishedAt = new Date();

    const output = {
        worker: WORKER_NAME,
        role: WORKER_ROLE,
        taskName,
        steps,
        itemsProduced: items.length,
        // Simple deterministic aggregate so the orchestrator has something to verify.
        sum: items.reduce((acc, item) => acc + item.value, 0),
        startedAt: startedAt.toISOString(),
        finishedAt: finishedAt.toISOString(),
        durationMs: finishedAt.getTime() - startedAt.getTime(),
    };

    await Actor.setValue('OUTPUT', output);

    log.info(`${WORKER_NAME}: finished`, output);

    await Actor.exit(`${WORKER_NAME} produced ${output.itemsProduced} items (sum=${output.sum}).`);
} catch (err) {
    log.exception(err, `${WORKER_NAME}: failed`);
    await Actor.fail(err instanceof Error ? err.message : String(err));
}
