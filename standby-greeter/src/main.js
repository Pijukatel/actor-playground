import http from 'node:http';

import { Actor, log } from 'apify';

await Actor.init();

const runId = Actor.getEnv().actorRunId;

// This Actor only makes sense as an HTTP server. An ordinary run (e.g. `apify call`) has nothing to do.
if (Actor.config.get('metaOrigin') !== 'STANDBY') {
    log.info('Not started in Standby mode, nothing to do. Call this Actor through its Standby URL instead.');
    await Actor.exit();
}

const port = Number(process.env.ACTOR_STANDBY_PORT ?? 4321);
let requestCount = 0;

const sendJson = (res, status, body) => {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(body));
};

const readJsonBody = async (req) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const raw = Buffer.concat(chunks).toString('utf8');
    return raw ? JSON.parse(raw) : {};
};

const server = http.createServer(async (req, res) => {
    // The platform probes GET / until the server answers, before routing traffic to this run.
    if (req.headers['x-apify-container-server-readiness-probe']) {
        log.debug('Readiness probe received');
        return sendJson(res, 200, { ready: true });
    }

    const { pathname } = new URL(req.url, `http://localhost:${port}`);
    log.info(`<- ${req.method} ${pathname}`);

    try {
        if (req.method === 'GET' && pathname === '/') {
            return sendJson(res, 200, { actor: 'standby-greeter', runId, requestCount });
        }

        if (req.method === 'POST' && pathname === '/greet') {
            const { name = 'stranger', index } = await readJsonBody(req);
            requestCount += 1;
            const greeting = `Hello, ${name}!`;
            log.info(`Greeting #${requestCount}: ${greeting}`, { index });
            return sendJson(res, 200, { greeting, requestCount, servedByRunId: runId, servedAt: new Date().toISOString() });
        }

        return sendJson(res, 404, { error: `No route for ${req.method} ${pathname}` });
    } catch (err) {
        log.exception(err, 'Request failed');
        return sendJson(res, 400, { error: err.message });
    }
});

server.listen(port, () => log.info(`Standby greeter listening on port ${port} (run ${runId})`));

// Idle Standby runs get `aborting` before they are stopped; shut down cleanly.
Actor.on('aborting', async () => {
    log.info(`Shutting down after serving ${requestCount} greeting(s)`);
    server.close();
    await Actor.exit();
});
