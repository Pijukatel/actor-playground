# Swarm scraper (Rust)

A draft of a generic bulk scraper for the Apify platform built from two kinds of runs of **one
Actor**:

- a **hub** run that owns the frontier (the set of URLs to scrape), hands out batches, writes the
  dataset, checkpoints itself, and starts and stops the nodes;
- **node** runs that fetch and parse pages with [crawlee-rs](https://github.com/apify/crawlee-rs)
  and talk to the hub over **one websocket on the hub's container URL** (`ACTOR_WEB_SERVER_URL`).

The nodes make no Apify API calls at all. The hub makes a few hundred per crawl: batched dataset
pushes, a checkpoint record every 15 s, and one run start per node.

```text
                 wss://<key>.runs.apify.net/ws
 node run  ───────────────┐
 node run  ───────────────┤── Conductor ── worker router ──► hub run ──► Dataset (batched POST)
 node run  ───────────────┘                                   │  └──► Key-value store (checkpoint)
                                                              └── frontier in memory
```

## What it does

Input is one JSON object (see `.actor/input_schema.json`):

| Field | Meaning |
|---|---|
| `startUrls`, `urlListUrl` | Pages to scrape; the list file is one URL per line, for millions of pages |
| `extract` | `{"title": "h1", "price": ".price", "link": {"selector": "a.next", "attr": "href"}}` |
| `followSelector`, `followGlobs`, `maxDepth` | Links to enqueue from each page, and how deep to go |
| `nodes`, `nodeMemoryMbytes`, `nodeConcurrency` | Size of the swarm |
| `batchSize`, `leaseSecs`, `maxRequests` | Batch handed out per message, how long a node may hold it, global cap |
| `useApifyProxy`, `apifyProxyGroups` | Proxy for the nodes |

The wire protocol is five JSON messages: `hello`, `pull`, `done` (results plus discovered
links) from the node; `welcome` (the scrape configuration), `batch`, `idle`, `stop` from the hub.
Every request on the wire is a crawlee `Request`, so depth, labels and user data round-trip.

On the node, `HubRequestManager` implements crawlee-rs's `RequestManager` trait: `fetch_next_request`
pops from a local buffer refilled by `pull`, `mark_request_as_handled` reports results per batch,
`reclaim_request` keeps retries local, and `add_requests` (what `enqueue_links` calls) forwards
discovered links to the hub instead of queuing them locally. Everything else, including autoscaled
concurrency, retries, sessions and per-domain throttling, is stock crawlee-rs `HtmlCrawler`.

## Running it

Locally, hub and one node in one process, against the bundled fixture site:

```sh
cargo run --release --bin fixture_site      # 20 categories, 2000 products on 127.0.0.1:8787
cargo run --release -- input.json           # with {"mode": "both", "startUrls": [...], ...}
```

Results land in `storage/results.jsonl` and the checkpoint in `storage/SWARM_STATE.json`.
Separate processes work too: a hub with `"mode": "hub"` and `ACTOR_WEB_SERVER_PORT=4322`, and
nodes with `{"mode": "node", "hubUrl": "http://127.0.0.1:4322", "secret": "..."}`.

On the platform, `apify push` builds the image from `Dockerfile`. Start the Actor with
`"nodes": 8` and it starts eight runs of itself with `mode: node`, passing its own container URL
and a generated secret in their input. When the frontier drains, the hub sends `stop`, flushes
the dataset, writes a final checkpoint and aborts the node runs.

## Measured on the fixture site

| Run | Result |
|---|---|
| Follow links, depth 2, debug build, one node, concurrency 50 | 2021 pages, 2021 items, 0 failed, about 170 pages/s |
| URL list of 2000 pages, release build, one node, concurrency 100 | 7.1 s wall clock including a 2 s shutdown wait, 20 MB peak RSS for hub and node together |
| Fault test: node killed after 8 s, hub killed after 22 s, both restarted | 2000/2000 unique pages, 240 duplicate rows (pages done after the last checkpoint), 0 lost |

The fault test shows the two recovery paths. A node that dies has its batch requeued the moment its
websocket closes. A hub that dies restarts from a checkpoint at most 15 s old, nodes reconnect on
their own, and results for batches the new hub does not know are still accepted. Delivery is
at-least-once: the dataset can contain duplicates after a hub restart, never gaps.

## Does a swarm of 128 MB nodes beat one big run?

The honest answer from the platform code: **not on CPU, only on shape.**

**CPU is allocated in proportion to memory.** The worker gives a run `memory / 64 GB` of the
cores reserved for Actors on a 4xlarge-class machine, which is 8 to 12 cores depending on the
instance generation (`apify-worker`, `ContainerLimitsCalculator.getRunCpuShare`). That is a hard
quota after the first seconds of startup.

| Run memory | CPU share | Pages/s at crawlee-rs's ~1.3 ms CPU per page |
|---:|---:|---:|
| 128 MB | 1.6 to 2.3 % of a core | 12 to 18 |
| 1 GB | 12 to 19 % | 95 to 145 |
| 4 GB | 50 to 75 % | 380 to 580 |
| 32 GB (per-run maximum) | 4 to 6 cores | 3,000 to 4,600 |

So 256 nodes of 128 MB have exactly the CPU of one 32 GB run, and cost the same in compute units.
A 128 MB Rust node is memory-feasible (20 MB measured for hub and node together), but it is CPU
starved: real sites with 200 ms latency need concurrency 20 to 50 to keep even that share busy.

**The alternative the user asked about, one big run with several scrapers and local IPC, is
simpler than it sounds.** In Rust there is nothing to synchronize across processes: one tokio
process already uses every core, and "multiple scrapers" is one `HtmlCrawler` with a higher
concurrency sharing state through `Arc` and channels. There is no reason to run several
processes in one container except crash isolation. That model is the baseline, and it wins on:

- no external messaging, so no Conductor hops, no external data transfer for control traffic,
  no duplicates and no secret to protect;
- one failure domain that is also one recovery domain, with crawlee-rs's own request queue
  persistence;
- the least code.

**Where the swarm wins** is everything a single container cannot do:

- **Total size.** A single run stops at 32 GB, so about 4 to 6 cores. Plans allow 64 to 512 GB
  in total and 32 to 256 concurrent runs. With 1 GB nodes, a 256-run swarm has 256 GB of CPU
  share, 8x the biggest single run, and the hub's frontier never touches the request queue's
  875 requests/s per-queue ceiling, because it lives in the hub's memory.
- **Scheduling.** Small runs start on any worker with spare capacity; a 32 GB run waits for a
  worker that has 32 GB free.
- **Network spread.** Each node has its own worker's NIC and its own share of outbound
  connections. Bandwidth-bound crawls (large pages, many images) scale with the number of
  workers rather than one container's share.
- **Blast radius.** A migration or OOM takes out one node and one batch, not the whole crawl.

**What the swarm costs:**

- Every batch and result crosses Conductor. The billing code classifies a worker's public IP as
  AWS same-region and counts that as external transfer, so control traffic is billed at the
  external rate. It is a few hundred bytes per page, which is negligible next to the page itself,
  but bulk payloads should never ride this channel.
- The hub is one process with a bounded in-memory frontier. At 100 bytes per URL a 4 GB hub
  holds tens of millions of URLs, and the checkpoint grows with it.
- At-least-once delivery, so consumers should dedupe on `url` after a hub restart.

**Recommendation:** make node size an input, not a dogma. Default to 1 to 4 GB nodes. Use 128 MB
nodes only when the crawl is latency-bound on a slow or heavily proxied site, where many small
runs let you spread connections and proxy sessions widely without paying for idle CPU. For a
fast site where parsing dominates, a few 8 GB nodes beat hundreds of tiny ones on hub traffic,
duplicates and start-up cost, and a single 32 GB run beats them all until it hits its CPU cap.

## Platform facts this design rests on

- Nodes reach the hub through its container URL: Conductor resolves the key, the worker's run
  router proxies the upgrade, and `http-proxy` pipes the sockets with no idle timeout. The load
  balancer's idle timeout is 360 s; nodes ping every 30 s.
- Conductor and the worker put no limit on concurrent websockets per run.
- A hub migration drops every socket; nodes reconnect with backoff for up to 10 minutes.
- The container URL's only protection is the random key. The hub adds a shared secret passed to
  nodes in their input and checked on the first message.

## Not in this draft

- A request-queue-backed frontier for crawls larger than the hub's memory.
- Cross-node politeness: each node throttles per domain on its own, so the swarm as a whole can
  exceed a site's rate limit by the number of nodes.
- Standby mode for the hub, so a long-lived swarm can be fed new URL lists over time.
- Per-domain sharding across nodes, which would also give session affinity.
- Platform authentication for the websocket instead of a secret in the input.
