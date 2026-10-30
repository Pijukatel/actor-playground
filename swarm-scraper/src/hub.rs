//! The hub run: owns the frontier, serves the node websocket on the container URL, batches
//! dataset pushes, checkpoints its state, and starts and stops the node runs.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use crawlee::Request;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::apify::ApifyClient;
use crate::config::{ActorEnv, Input, Mode, ScrapeConfig};
use crate::node;
use crate::protocol::{HubMessage, NodeMessage, RequestResult};

const STATE_KEY: &str = "SWARM_STATE";
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(15);
const STATUS_LOG_INTERVAL: Duration = Duration::from_secs(10);
const FLUSH_ITEMS: usize = 500;
const FLUSH_BYTES: usize = 4 * 1024 * 1024; // estimated at 256 bytes per item
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const URL_LIST_CHUNK: usize = 1000;

struct Inflight {
    node_id: String,
    requests: Vec<Request>,
    sent_at: Instant,
}

#[derive(Default)]
struct Frontier {
    pending: VecDeque<Request>,
    inflight: HashMap<u64, Inflight>,
    seen: HashSet<String>,
    next_batch_id: u64,
    handled: u64,
    failed: u64,
    items: u64,
    loading_done: bool,
    dirty: bool,
}

struct NodeConn {
    tx: mpsc::UnboundedSender<HubMessage>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pending: usize,
    inflight_batches: usize,
    inflight_requests: usize,
    seen: usize,
    handled: u64,
    failed: u64,
    items: u64,
    nodes: usize,
    elapsed_secs: f64,
    pages_per_sec: f64,
    loading_done: bool,
    done: bool,
}

/// What survives a hub migration: the frontier and the counters. Batches in flight go back to
/// pending, since their nodes reconnect to a hub that no longer knows them.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Checkpoint {
    secret: String,
    pending: Vec<Request>,
    seen: Vec<String>,
    handled: u64,
    failed: u64,
    items: u64,
    loading_done: bool,
    done: bool,
}

pub struct Hub {
    frontier: Mutex<Frontier>,
    nodes: Mutex<HashMap<String, NodeConn>>,
    config: ScrapeConfig,
    secret: String,
    max_requests: u64,
    lease: Duration,
    sink: DatasetSink,
    started: Instant,
    stopping: AtomicBool,
}

impl Hub {
    /// Adds requests the hub has not seen, in order. Returns how many were new.
    fn enqueue(&self, requests: impl IntoIterator<Item = Request>) -> usize {
        let mut frontier = self.frontier.lock();
        let mut added = 0;
        for request in requests {
            if self.max_requests > 0 && frontier.seen.len() as u64 >= self.max_requests {
                break;
            }
            if request.crawl_depth() > self.config.max_depth {
                continue;
            }
            if frontier.seen.insert(request.unique_key.clone()) {
                frontier.pending.push_back(request);
                added += 1;
            }
        }
        if added > 0 {
            frontier.dirty = true;
        }
        added
    }

    fn take_batch(&self, node_id: &str, max: usize) -> Option<(u64, Vec<Request>)> {
        let mut frontier = self.frontier.lock();
        if frontier.pending.is_empty() {
            return None;
        }
        let count = max.max(1).min(frontier.pending.len());
        let requests: Vec<Request> = frontier.pending.drain(..count).collect();
        let batch_id = frontier.next_batch_id;
        frontier.next_batch_id += 1;
        frontier.inflight.insert(
            batch_id,
            Inflight {
                node_id: node_id.to_owned(),
                requests: requests.clone(),
                sent_at: Instant::now(),
            },
        );
        frontier.dirty = true;
        Some((batch_id, requests))
    }

    /// Records a finished batch. Items are returned for the sink. A batch the hub no longer
    /// tracks (its lease expired, or the node reconnected after a hub restart) still contributes
    /// its items, so slow nodes lose nothing; its requests may then be scraped twice.
    fn complete(
        &self,
        batch_id: u64,
        results: Vec<RequestResult>,
        discovered: Vec<Request>,
    ) -> Vec<Value> {
        let mut items = Vec::new();
        {
            let mut frontier = self.frontier.lock();
            let known = frontier.inflight.remove(&batch_id).is_some();
            if !known {
                tracing::debug!(batch_id, "Results for a batch the hub no longer tracks");
            }
            for result in results {
                if result.ok {
                    frontier.handled += 1;
                } else {
                    frontier.failed += 1;
                    tracing::debug!(unique_key = %result.unique_key, error = ?result.error, "Request failed on a node");
                }
                frontier.items += result.items.len() as u64;
                items.extend(result.items);
            }
            frontier.dirty = true;
        }
        self.enqueue(discovered);
        items
    }

    /// Puts the batches of a node that went away back in front of the queue.
    fn requeue_node(&self, node_id: &str) {
        let mut frontier = self.frontier.lock();
        let expired: Vec<u64> = frontier
            .inflight
            .iter()
            .filter(|(_, b)| b.node_id == node_id)
            .map(|(id, _)| *id)
            .collect();
        requeue(&mut frontier, expired, "node disconnected");
    }

    /// Hands out again every batch whose lease ran out.
    fn reap_expired(&self) {
        let mut frontier = self.frontier.lock();
        let expired: Vec<u64> = frontier
            .inflight
            .iter()
            .filter(|(_, b)| b.sent_at.elapsed() > self.lease)
            .map(|(id, _)| *id)
            .collect();
        requeue(&mut frontier, expired, "lease expired");
    }

    fn is_done(&self) -> bool {
        let frontier = self.frontier.lock();
        frontier.loading_done && frontier.pending.is_empty() && frontier.inflight.is_empty()
    }

    fn status(&self) -> Status {
        let frontier = self.frontier.lock();
        let elapsed_secs = self.started.elapsed().as_secs_f64();
        Status {
            pending: frontier.pending.len(),
            inflight_batches: frontier.inflight.len(),
            inflight_requests: frontier.inflight.values().map(|b| b.requests.len()).sum(),
            seen: frontier.seen.len(),
            handled: frontier.handled,
            failed: frontier.failed,
            items: frontier.items,
            nodes: self.nodes.lock().len(),
            elapsed_secs,
            pages_per_sec: if elapsed_secs > 0.0 {
                (frontier.handled + frontier.failed) as f64 / elapsed_secs
            } else {
                0.0
            },
            loading_done: frontier.loading_done,
            done: self.stopping.load(Ordering::Relaxed),
        }
    }

    fn checkpoint(&self, done: bool) -> Checkpoint {
        let mut frontier = self.frontier.lock();
        frontier.dirty = false;
        let mut pending: Vec<Request> = frontier
            .inflight
            .values()
            .flat_map(|b| b.requests.iter().cloned())
            .collect();
        pending.extend(frontier.pending.iter().cloned());
        Checkpoint {
            secret: self.secret.clone(),
            pending,
            seen: frontier.seen.iter().cloned().collect(),
            handled: frontier.handled,
            failed: frontier.failed,
            items: frontier.items,
            loading_done: frontier.loading_done,
            done,
        }
    }

    fn restore(&self, checkpoint: Checkpoint) {
        let mut frontier = self.frontier.lock();
        frontier.seen = checkpoint.seen.into_iter().collect();
        frontier.pending = checkpoint.pending.into_iter().collect();
        frontier.handled = checkpoint.handled;
        frontier.failed = checkpoint.failed;
        frontier.items = checkpoint.items;
        frontier.loading_done = checkpoint.loading_done;
    }

    fn is_dirty(&self) -> bool {
        self.frontier.lock().dirty
    }

    fn broadcast(&self, message: &HubMessage) {
        let nodes = self.nodes.lock();
        for node in nodes.values() {
            let text = serde_json::to_string(message).expect("hub messages serialize");
            let _ = node
                .tx
                .send(serde_json::from_str(&text).expect("hub messages round-trip"));
        }
    }
}

fn requeue(frontier: &mut Frontier, batch_ids: Vec<u64>, reason: &str) {
    for batch_id in batch_ids {
        if let Some(batch) = frontier.inflight.remove(&batch_id) {
            tracing::warn!(batch_id, node_id = %batch.node_id, requests = batch.requests.len(), "Requeuing batch: {reason}");
            for request in batch.requests.into_iter().rev() {
                frontier.pending.push_front(request);
            }
            frontier.dirty = true;
        }
    }
}

#[derive(Deserialize)]
struct WsQuery {
    #[serde(default)]
    secret: String,
}

async fn status_route(State(hub): State<Arc<Hub>>) -> Json<Status> {
    Json(hub.status())
}

async fn ws_route(
    ws: WebSocketUpgrade,
    Query(query): Query<WsQuery>,
    State(hub): State<Arc<Hub>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_node(socket, hub, query.secret))
}

async fn handle_node(socket: WebSocket, hub: Arc<Hub>, secret: String) {
    let (mut ws_sink, mut ws_stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<HubMessage>();
    let writer = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let text = serde_json::to_string(&message).expect("hub messages serialize");
            if ws_sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    let mut node_id: Option<String> = None;
    while let Some(Ok(message)) = ws_stream.next().await {
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => break,
            _ => continue,
        };
        let parsed: NodeMessage = match serde_json::from_str(text.as_str()) {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::warn!(%error, "Dropping an unparsable node message");
                continue;
            }
        };
        match parsed {
            NodeMessage::Hello {
                node_id: id,
                run_id,
                secret: presented,
            } => {
                if presented != secret || presented != hub.secret {
                    let _ = tx.send(HubMessage::Rejected {
                        reason: "bad secret".to_owned(),
                    });
                    tracing::warn!(node_id = %id, "Rejected a node with a bad secret");
                    break;
                }
                tracing::info!(node_id = %id, ?run_id, "Node connected");
                hub.nodes
                    .lock()
                    .insert(id.clone(), NodeConn { tx: tx.clone() });
                node_id = Some(id);
                let _ = tx.send(HubMessage::Welcome {
                    config: hub.config.clone(),
                });
            }
            NodeMessage::Pull { max } => {
                let Some(id) = node_id.as_deref() else { break };
                let reply = if hub.stopping.load(Ordering::Relaxed) {
                    HubMessage::Stop
                } else {
                    match hub.take_batch(id, max.min(hub.config.batch_size.max(1) * 4)) {
                        Some((batch_id, requests)) => HubMessage::Batch { batch_id, requests },
                        None => HubMessage::Idle,
                    }
                };
                let _ = tx.send(reply);
            }
            NodeMessage::Done {
                batch_id,
                results,
                discovered,
            } => {
                if node_id.is_none() {
                    break;
                }
                let items = hub.complete(batch_id, results, discovered);
                hub.sink.push(items).await;
            }
            NodeMessage::Ping => {
                let _ = tx.send(HubMessage::Pong);
            }
        }
    }

    if let Some(id) = node_id {
        hub.nodes.lock().remove(&id);
        hub.requeue_node(&id);
        tracing::info!(node_id = %id, "Node disconnected");
    }
    writer.abort();
}

enum SinkTarget {
    Apify {
        client: ApifyClient,
        dataset_id: String,
    },
    Local {
        path: PathBuf,
    },
}

/// Buffers dataset items and writes them in large batches, so the hub makes one API call for
/// hundreds of pages instead of one per page.
pub struct DatasetSink {
    tx: Mutex<Option<mpsc::Sender<Vec<Value>>>>,
}

impl DatasetSink {
    fn start(target: SinkTarget) -> (Self, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(1024);
        let task = tokio::spawn(sink_loop(rx, target));
        (
            Self {
                tx: Mutex::new(Some(tx)),
            },
            task,
        )
    }

    async fn push(&self, items: Vec<Value>) {
        if items.is_empty() {
            return;
        }
        let sender = self.tx.lock().clone();
        match sender {
            Some(sender) if sender.send(items).await.is_ok() => {}
            _ => tracing::error!("Dataset sink is closed, items dropped"),
        }
    }

    /// Lets the sink task flush and exit once in-flight pushes complete.
    fn close(&self) {
        self.tx.lock().take();
    }
}

async fn sink_loop(mut rx: mpsc::Receiver<Vec<Value>>, target: SinkTarget) {
    let mut buffer: Vec<Value> = Vec::new();
    let mut buffered_bytes = 0usize;
    let mut oldest: Option<Instant> = None;
    loop {
        let received = tokio::time::timeout(FLUSH_INTERVAL, rx.recv()).await;
        match received {
            Ok(Some(items)) => {
                buffered_bytes += items.len() * 256;
                buffer.extend(items);
                oldest.get_or_insert_with(Instant::now);
            }
            Ok(None) => {
                flush(&target, std::mem::take(&mut buffer)).await;
                return;
            }
            Err(_) => {}
        }
        let due = oldest.is_some_and(|at| at.elapsed() >= FLUSH_INTERVAL);
        if buffer.len() >= FLUSH_ITEMS
            || buffered_bytes >= FLUSH_BYTES
            || (due && !buffer.is_empty())
        {
            flush(&target, std::mem::take(&mut buffer)).await;
            buffered_bytes = 0;
            oldest = None;
        }
    }
}

async fn flush(target: &SinkTarget, items: Vec<Value>) {
    if items.is_empty() {
        return;
    }
    let count = items.len();
    let result = match target {
        SinkTarget::Apify { client, dataset_id } => match serde_json::to_vec(&items) {
            Ok(body) => client.push_items(dataset_id, body).await,
            Err(error) => Err(error.into()),
        },
        SinkTarget::Local { path } => {
            let mut body = String::new();
            for item in &items {
                body.push_str(&item.to_string());
                body.push('\n');
            }
            append_to_file(path, body.as_bytes()).await
        }
    };
    match result {
        Ok(()) => tracing::debug!(count, "Flushed dataset items"),
        Err(error) => tracing::error!(count, %error, "Failed to write dataset items"),
    }
}

async fn append_to_file(path: &PathBuf, bytes: &[u8]) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    Ok(())
}

enum StateStore {
    Apify {
        client: ApifyClient,
        store_id: String,
    },
    Local {
        path: PathBuf,
    },
}

impl StateStore {
    async fn load(&self) -> anyhow::Result<Option<Checkpoint>> {
        match self {
            StateStore::Apify { client, store_id } => client.get_record(store_id, STATE_KEY).await,
            StateStore::Local { path } => match tokio::fs::read(path).await {
                Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error.into()),
            },
        }
    }

    async fn save(&self, checkpoint: &Checkpoint) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(checkpoint)?;
        match self {
            StateStore::Apify { client, store_id } => {
                client
                    .set_record(store_id, STATE_KEY, bytes, "application/json")
                    .await
            }
            StateStore::Local { path } => {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(path, bytes).await?;
                Ok(())
            }
        }
    }
}

fn random_secret() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn load_frontier(hub: Arc<Hub>, input: Input) {
    let start: Vec<Request> = input
        .start_urls
        .iter()
        .map(|s| Request::new(s.url.clone()))
        .collect();
    let added = hub.enqueue(start);
    tracing::info!(added, "Enqueued start URLs");

    if let Some(list_url) = &input.url_list_url {
        match reqwest::get(list_url)
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(response) => match response.text().await {
                Ok(text) => {
                    let mut chunk = Vec::with_capacity(URL_LIST_CHUNK);
                    let mut total = 0;
                    for line in text
                        .lines()
                        .map(str::trim)
                        .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    {
                        chunk.push(Request::new(line.to_owned()));
                        if chunk.len() >= URL_LIST_CHUNK {
                            total += hub.enqueue(std::mem::take(&mut chunk));
                            tokio::task::yield_now().await;
                        }
                    }
                    total += hub.enqueue(chunk);
                    tracing::info!(added = total, "Enqueued the URL list");
                }
                Err(error) => tracing::error!(%error, "Reading the URL list failed"),
            },
            Err(error) => tracing::error!(%error, "Downloading the URL list failed"),
        }
    }
    hub.frontier.lock().loading_done = true;
}

async fn start_node_runs(
    client: &ApifyClient,
    actor_id: &str,
    hub_url: &str,
    secret: &str,
    input: &Input,
) -> Vec<String> {
    let node_input = serde_json::json!({ "mode": "node", "hubUrl": hub_url, "secret": secret });
    let mut run_ids = Vec::new();
    for index in 0..input.nodes {
        match client
            .start_run(actor_id, &node_input, input.node_memory_mbytes)
            .await
        {
            Ok(run_id) => {
                tracing::info!(index, %run_id, "Started node run");
                run_ids.push(run_id);
            }
            Err(error) => tracing::error!(index, %error, "Starting a node run failed"),
        }
    }
    run_ids
}

pub async fn run_hub(input: Input, env: ActorEnv) -> anyhow::Result<()> {
    let api = match (env.api_enabled(), &env.token) {
        (true, Some(token)) => Some(ApifyClient::new(&env.api_base_url, token)?),
        _ => None,
    };
    let store = match (&api, &env.default_key_value_store_id) {
        (Some(client), Some(store_id)) => StateStore::Apify {
            client: client.clone(),
            store_id: store_id.clone(),
        },
        _ => StateStore::Local {
            path: env.storage_dir.join(format!("{STATE_KEY}.json")),
        },
    };
    let sink_target = match (&api, &env.default_dataset_id) {
        (Some(client), Some(dataset_id)) => SinkTarget::Apify {
            client: client.clone(),
            dataset_id: dataset_id.clone(),
        },
        _ => SinkTarget::Local {
            path: env.storage_dir.join("results.jsonl"),
        },
    };

    let checkpoint = match store.load().await {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            tracing::warn!(%error, "Reading the checkpoint failed, starting from scratch");
            None
        }
    };
    if checkpoint.as_ref().is_some_and(|c| c.done) {
        tracing::info!("The checkpoint says this crawl is complete, nothing to do");
        return Ok(());
    }
    let restored = checkpoint.is_some();
    let secret = checkpoint
        .as_ref()
        .map(|c| c.secret.clone())
        .or_else(|| input.secret.clone())
        .unwrap_or_else(random_secret);

    let (sink, sink_task) = DatasetSink::start(sink_target);
    let hub = Arc::new(Hub {
        frontier: Mutex::new(Frontier::default()),
        nodes: Mutex::new(HashMap::new()),
        config: input.scrape.clone(),
        secret: secret.clone(),
        max_requests: input.max_requests,
        lease: Duration::from_secs(input.lease_secs),
        sink,
        started: Instant::now(),
        stopping: AtomicBool::new(false),
    });
    if let Some(checkpoint) = checkpoint {
        tracing::info!(
            pending = checkpoint.pending.len(),
            handled = checkpoint.handled,
            "Restored the frontier from the checkpoint"
        );
        hub.restore(checkpoint);
    }

    let app = Router::new()
        .route("/", get(status_route))
        .route("/ws", get(ws_route))
        .with_state(hub.clone());
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", env.web_server_port))
        .await
        .with_context(|| format!("binding the hub server to port {}", env.web_server_port))?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(%error, "Hub server failed");
        }
    });
    tracing::info!(port = env.web_server_port, url = ?env.web_server_url, "Hub websocket server listening");

    if !restored {
        tokio::spawn(load_frontier(hub.clone(), input.clone()));
    }

    let mut node_runs = Vec::new();
    if input.nodes > 0 {
        match (&api, &env.actor_id, &env.web_server_url) {
            (Some(client), Some(actor_id), Some(hub_url)) => {
                node_runs = start_node_runs(client, actor_id, hub_url, &secret, &input).await;
            }
            _ => tracing::warn!(
                "Node runs can only be started on the platform; run external nodes against this hub instead"
            ),
        }
    }
    if input.mode == Mode::Both {
        let local_hub_url = format!("http://127.0.0.1:{}", env.web_server_port);
        let node_env = env.clone();
        let node_secret = secret.clone();
        tokio::spawn(async move {
            if let Err(error) = node::run_node(local_hub_url, node_secret, &node_env).await {
                tracing::error!(%error, "In-process node failed");
            }
        });
    }

    let mut last_status_log = Instant::now();
    let mut last_checkpoint = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        hub.reap_expired();
        if last_status_log.elapsed() >= STATUS_LOG_INTERVAL {
            tracing::info!(status = ?hub.status(), "Hub status");
            last_status_log = Instant::now();
        }
        if last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL && hub.is_dirty() {
            if let Err(error) = store.save(&hub.checkpoint(false)).await {
                tracing::warn!(%error, "Saving the checkpoint failed");
            }
            last_checkpoint = Instant::now();
        }
        if hub.is_done() {
            break;
        }
    }

    tracing::info!("Frontier drained, stopping nodes");
    hub.stopping.store(true, Ordering::Relaxed);
    hub.broadcast(&HubMessage::Stop);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let final_checkpoint = hub.checkpoint(true);
    hub.sink.close();
    if let Err(error) = sink_task.await {
        tracing::warn!(%error, "Dataset sink task failed");
    }
    if let Err(error) = store.save(&final_checkpoint).await {
        tracing::warn!(%error, "Saving the final checkpoint failed");
    }
    if let Some(client) = &api {
        for run_id in &node_runs {
            if let Err(error) = client.abort_run(run_id).await {
                tracing::warn!(%run_id, %error, "Aborting a node run failed");
            }
        }
    }
    tracing::info!(
        handled = final_checkpoint.handled,
        failed = final_checkpoint.failed,
        items = final_checkpoint.items,
        "Crawl complete"
    );
    Ok(())
}
