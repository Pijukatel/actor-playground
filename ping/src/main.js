import { Actor } from 'apify';

const MAX_LEVEL = 3;
const OTHER_ACTOR_NAME = 'pong';

// The runtime serves /v2/actors/... but not the /v2/acts/... path used by apify-client 2.23,
// so the other Actor is started through the REST API directly.
async function callActor(actorName, input) {
    const baseUrl = process.env.APIFY_API_BASE_URL;
    const headers = { Authorization: `Bearer ${process.env.APIFY_TOKEN}`, 'Content-Type': 'application/json' };

    const startResponse = await fetch(`${baseUrl}/v2/actors/${actorName}/runs`, {
        method: 'POST',
        headers,
        body: JSON.stringify(input),
    });
    if (!startResponse.ok) throw new Error(`Starting ${actorName} failed: ${startResponse.status} ${await startResponse.text()}`);
    let { data: run } = await startResponse.json();

    while (!['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT'].includes(run.status)) {
        const response = await fetch(`${baseUrl}/v2/actor-runs/${run.id}?waitForFinish=30`, { headers });
        ({ data: run } = await response.json());
    }
    return run;
}

await Actor.init();

const { level = 1 } = (await Actor.getInput()) ?? {};
console.log(`hello from ping at recursion level ${level}`);

if (level < MAX_LEVEL) {
    console.log(`calling ${OTHER_ACTOR_NAME} with level ${level + 1}`);
    const { username } = await Actor.apifyClient.user().get();
    const run = await callActor(`${username}~${OTHER_ACTOR_NAME}`, { level: level + 1 });
    console.log(`${OTHER_ACTOR_NAME} finished with status ${run.status} (run ${run.id})`);
}

await Actor.exit();
