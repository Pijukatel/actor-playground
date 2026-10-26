import { Actor } from 'apify';

const MAX_LEVEL = 3;
const OTHER_ACTOR_NAME = 'pong';

await Actor.init();

const { level = 1 } = (await Actor.getInput()) ?? {};
console.log(`hello from ping at recursion level ${level}`);

if (level < MAX_LEVEL) {
    console.log(`calling ${OTHER_ACTOR_NAME} with level ${level + 1}`);
    const { username } = await Actor.apifyClient.user().get();
    const run = await Actor.call(`${username}/${OTHER_ACTOR_NAME}`, { level: level + 1 });
    console.log(`${OTHER_ACTOR_NAME} finished with status ${run.status}`);
}

await Actor.exit();
