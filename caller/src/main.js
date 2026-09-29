import { Actor, log } from 'apify';

await Actor.init();

const { standbyActor = 'standby-greeter', names = [] } = (await Actor.getInput()) ?? {};
const client = Actor.apifyClient;
const token = Actor.config.get('token');

// A bare name is resolved against the current user, so the same input works locally and on the platform.
let actorRef = standbyActor;
if (!/[/~]/.test(actorRef)) {
    const { username } = await client.user('me').get();
    actorRef = `${username}/${actorRef}`;
}

log.info(`Looking up Standby Actor ${actorRef}`);
const actor = await client.actor(actorRef).get();
if (!actor) throw new Error(`Actor ${actorRef} not found`);
if (!actor.standbyUrl) throw new Error(`Actor ${actorRef} has no Standby URL - is Standby enabled?`);
log.info(`Standby URL: ${actor.standbyUrl}`);

const results = [];
for (const [index, name] of names.entries()) {
    log.info(`-> POST /greet {name: "${name}"}`);
    const response = await fetch(`${actor.standbyUrl}/greet`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ name, index }),
    });
    const body = await response.json();
    if (!response.ok) throw new Error(`Standby Actor answered ${response.status}: ${JSON.stringify(body)}`);

    log.info(`<- ${response.status} "${body.greeting}" (request #${body.requestCount}, standby run ${body.servedByRunId})`);
    results.push({ name, ...body });
}

await Actor.pushData(results);
await Actor.setValue('OUTPUT', { standbyActor: actorRef, standbyUrl: actor.standbyUrl, greetings: results.length });
log.info(`Done: received ${results.length} greeting(s) from the Standby Actor`);

await Actor.exit();
