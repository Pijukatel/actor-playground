import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';

import { Actor, log } from 'apify';

await Actor.init();

const input = (await Actor.getInput()) ?? {};
const { mode = 'env', sleepSecs = 60, failUntilAttempt = 1 } = input;
const runId = Actor.config.get('actorRunId');
const env = process.env;
const startedAt = Date.now();

const PROBE_VARS = [
	'ACTOR_ID', 'ACTOR_RUN_ID', 'ACTOR_BUILD_ID', 'ACTOR_BUILD_NUMBER', 'ACTOR_BUILD_TAGS', 'ACTOR_TASK_ID',
	'ACTOR_FULL_NAME', 'ACTOR_STARTED_AT', 'ACTOR_TIMEOUT_AT', 'ACTOR_RESTART_ON_ERROR', 'ACTOR_MEMORY_MBYTES',
	'ACTOR_MAX_TOTAL_CHARGE_USD', 'ACTOR_EVENTS_WEBSOCKET_URL', 'ACTOR_WEB_SERVER_PORT', 'ACTOR_WEB_SERVER_URL',
	'ACTOR_STORAGES_JSON', 'ACTOR_DEFAULT_DATASET_ID', 'ACTOR_DEFAULT_KEY_VALUE_STORE_ID',
	'ACTOR_DEFAULT_REQUEST_QUEUE_ID', 'ACTOR_INPUT_KEY', 'APIFY_META_ORIGIN', 'APIFY_IS_AT_HOME',
	'APIFY_API_BASE_URL', 'APIFY_USER_ID', 'APIFY_PROXY_PASSWORD', 'APIFY_DEDICATED_CPUS',
	'PROBE_PLAIN', 'PROBE_SECRET', 'APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE',
];

for (const event of ['migrating', 'aborting', 'persistState', 'cpuInfo']) {
	Actor.on(event, (data) => log.info(`event ${event}: ${JSON.stringify(data ?? null)}`));
}

log.info(`feature-probe run ${runId} mode=${mode} input=${JSON.stringify(input)}`);

const server = createServer((req, res) => {
	res.writeHead(200, { 'content-type': 'text/plain' });
	res.end(`probe run ${runId} mode ${mode} up ${Math.round((Date.now() - startedAt) / 1000)}s path ${req.url}\n`);
});
if (env.ACTOR_WEB_SERVER_PORT) {
	server.listen(Number(env.ACTOR_WEB_SERVER_PORT), () =>
		log.info(`web server listening on ${env.ACTOR_WEB_SERVER_PORT}, containerUrl=${env.ACTOR_WEB_SERVER_URL}`),
	);
}

const envReport = Object.fromEntries(PROBE_VARS.map((name) => [name, env[name]]));

switch (mode) {
	case 'env': {
		for (const [name, value] of Object.entries(envReport)) log.info(`env ${name}=${value}`);
		await Actor.pushData({ kind: 'env', value: JSON.stringify(envReport) });
		const storages = JSON.parse(env.ACTOR_STORAGES_JSON ?? '{}');
		const extraId = storages?.datasets?.extra;
		if (extraId) {
			const extra = await Actor.openDataset(extraId);
			await extra.pushData({ note: `from run ${runId}` });
			log.info(`pushed to extra dataset ${extraId}`);
		} else {
			log.warning('no extra dataset id in ACTOR_STORAGES_JSON');
		}
		await Actor.setValue('OUTPUT', { mode, runId, env: envReport, secretField: input.secretField ?? null });
		break;
	}
	case 'output': {
		await Actor.pushData([{ kind: 'output', value: 'one' }, { kind: 'output', value: 'two' }]);
		await Actor.setValue('OUTPUT', { mode, runId, answer: 42 });
		await Actor.setValue('OTHER', 'plain text output', { contentType: 'text/plain' });
		break;
	}
	case 'status': {
		await Actor.setStatusMessage('Probe halfway');
		log.info('status message set to "Probe halfway"');
		await sleep(4000);
		await Actor.setStatusMessage('Probe done', { isStatusMessageTerminal: true });
		log.info('terminal status message set to "Probe done"');
		await Actor.pushData({ kind: 'status' });
		break;
	}
	case 'sleep': {
		log.info(`sleeping ${sleepSecs}s`);
		for (let elapsed = 0; elapsed < sleepSecs; elapsed += 5) {
			await sleep(Math.min(5, sleepSecs - elapsed) * 1000);
			log.info(`still sleeping, ${elapsed + 5}s elapsed`);
		}
		await Actor.pushData({ kind: 'sleep', value: String(sleepSecs) });
		break;
	}
	case 'fail': {
		const store = await Actor.openKeyValueStore();
		const attempt = ((await store.getValue('ATTEMPTS')) ?? 0) + 1;
		await store.setValue('ATTEMPTS', attempt);
		log.info(`attempt ${attempt} (failUntilAttempt=${failUntilAttempt}) restartOnError=${env.ACTOR_RESTART_ON_ERROR}`);
		if (attempt <= failUntilAttempt) {
			log.error(`attempt ${attempt}: failing on purpose`);
			await Actor.fail({ exitCode: 3, statusMessage: `Failed on attempt ${attempt}` });
		}
		await Actor.pushData({ kind: 'fail', value: `succeeded on attempt ${attempt}` });
		break;
	}
	case 'schema-bad': {
		await Actor.pushData({ kind: 'good', value: 'passes the schema' });
		log.info('pushing an item that violates the dataset schema (kind must be a string)');
		try {
			await Actor.pushData({ kind: 123 });
			log.error('UNEXPECTED: the invalid item was accepted');
		} catch (error) {
			log.info(`rejected as expected: ${error.message}`);
			await Actor.setValue('OUTPUT', { mode, rejected: true, message: error.message });
		}
		break;
	}
	default:
		throw new Error(`unknown mode ${mode}`);
}

server.close();
await Actor.exit();
