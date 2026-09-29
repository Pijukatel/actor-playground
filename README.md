# Actor playground

Two demo Actors that talk to each other over HTTP. They mostly print logs.

| Actor | Kind | What it does |
|---|---|---|
| [`standby-greeter`](standby-greeter) | Standby (`usesStandbyMode: true`) | HTTP server on `ACTOR_STANDBY_PORT`. `POST /greet {"name": "..."}` returns `Hello, <name>!` and logs every request. `GET /` returns status. Answers the platform readiness probe and shuts down cleanly on `aborting`. |
| [`caller`](caller) | Ordinary | Looks up the Standby Actor's `standbyUrl` through the API, sends one `POST /greet` per name in its input, logs each answer, and stores the results in the dataset plus a summary in `OUTPUT`. |

Caller input (both fields have defaults):

```json
{ "standbyActor": "standby-greeter", "names": ["Alice", "Bob", "Charlie"] }
```

A bare `standbyActor` name is resolved against the current user (`<username>/standby-greeter`), so the same input works on the local runtime and on the Apify platform.

## Run it on the local Actor runtime

```sh
npm install -g apify-cli@runtime
apify runtime install && apify runtime start --detach
apify runtime connect
apify login --token local-dev-token        # any token works locally

(cd standby-greeter && apify push)          # push the Standby Actor first
(cd caller && apify push && apify call --input '{"names":["Dana","Eve"]}')
```

The first request starts a Standby run of `standby-greeter` (a few seconds of cold start), and later requests reuse it. To see the greeter's side of the conversation, run `apify runs ls` on the greeter and then `apify runs log <runId>`.

To call the greeter by hand:

```sh
curl -X POST -H 'Authorization: Bearer local-dev-token' -H 'Content-Type: application/json' \
  -d '{"name":"curl"}' http://local-user-1--standby-greeter.localhost:3333/greet
```

If you run `apify call` directly on `standby-greeter`, it logs that it wasn't started in Standby mode and exits.
