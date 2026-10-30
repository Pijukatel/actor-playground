//! The node run: a crawlee-rs `HtmlCrawler` whose request source is the hub, reached over one
//! websocket. Items and discovered links travel back on the same connection.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context as _, bail};
use async_trait::async_trait;
use crawlee::core::MemoryStorageBackend;
use crawlee::core::errors::StorageResult;
use crawlee::core::request::unique_key_to_request_id;
use crawlee::core::storage::backend::{
    BatchAddRequestsResult, ProcessedRequest, QueueOperationInfo,
};
use crawlee::http::HtmlPipeline;
use crawlee::{
    BasicCrawler, EnqueueLinksOptions, EnqueueStrategy, HtmlContext, HtmlCrawler,
    HttpCrawlerOptions, ProxyConfiguration, Request, RequestManager, Services,
};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::config::{ActorEnv, ScrapeConfig};
use crate::extract;
use crate::protocol::{HubMessage, NodeMessage, RequestResult};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const FIRST_CONNECT_BUDGET: Duration = Duration::from_secs(600);
const PULL_INTERVAL: Duration = Duration::from_millis(200);
const IDLE_BACKOFF: Duration = Duration::from_secs(1);
const PING_INTERVAL: Duration = Duration::from_secs(30);

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Outcome {
    ok: bool,
    items: Vec<Value>,
    error: Option<String>,
}

struct BatchState {
    remaining: usize,
    results: Vec<RequestResult>,
}

#[derive(Default)]
struct Inner {
    pending: VecDeque<Request>,
    in_progress: HashSet<String>,
    key_to_batch: HashMap<String, u64>,
    batches: HashMap<u64, BatchState>,
    outcomes: HashMap<String, Outcome>,
    discovered: Vec<Request>,
    unsent: Vec<NodeMessage>,
    idle_until: Option<Instant>,
}

/// A `RequestManager` whose queue lives on the hub. Requests arrive in batches, results leave
/// per batch, and links the crawler wants to enqueue are forwarded instead of queued locally.
pub struct HubRequestManager {
    inner: Mutex<Inner>,
    tx: Mutex<Option<mpsc::UnboundedSender<NodeMessage>>>,
    config: ScrapeConfig,
    stopped: AtomicBool,
    pull_outstanding: AtomicBool,
    handled: AtomicU64,
}

impl HubRequestManager {
    fn new(config: ScrapeConfig) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            tx: Mutex::new(None),
            config,
            stopped: AtomicBool::new(false),
            pull_outstanding: AtomicBool::new(false),
            handled: AtomicU64::new(0),
        }
    }

    fn send(&self, message: NodeMessage) {
        let sender = self.tx.lock().clone();
        let failed = match sender {
            Some(sender) => sender.send(message).err().map(|e| e.0),
            None => Some(message),
        };
        if let Some(message) = failed {
            self.inner.lock().unsent.push(message);
        }
    }

    fn on_connected(&self, tx: mpsc::UnboundedSender<NodeMessage>) {
        let unsent = std::mem::take(&mut self.inner.lock().unsent);
        for message in unsent {
            let _ = tx.send(message);
        }
        *self.tx.lock() = Some(tx);
        self.pull_outstanding.store(false, Ordering::Relaxed);
    }

    /// The hub hands the node's batches to others once it is gone, so the requests not yet
    /// started are dropped here, and results collected so far are reported on reconnect.
    fn on_disconnected(&self) {
        *self.tx.lock() = None;
        self.pull_outstanding.store(false, Ordering::Relaxed);
        let mut guard = self.inner.lock();
        let Inner {
            pending,
            key_to_batch,
            batches,
            discovered,
            unsent,
            ..
        } = &mut *guard;
        for request in pending.drain(..) {
            if let Some(batch_id) = key_to_batch.remove(&request.unique_key)
                && let Some(batch) = batches.get_mut(&batch_id)
            {
                batch.remaining = batch.remaining.saturating_sub(1);
            }
        }
        let mut discovered = Some(std::mem::take(discovered));
        for (batch_id, batch) in batches.iter_mut() {
            let results = std::mem::take(&mut batch.results);
            if !results.is_empty() {
                unsent.push(NodeMessage::Done {
                    batch_id: *batch_id,
                    results,
                    discovered: discovered.take().unwrap_or_default(),
                });
            }
        }
        if let Some(discovered) = discovered.filter(|links| !links.is_empty()) {
            unsent.push(NodeMessage::Done {
                batch_id: u64::MAX,
                results: Vec::new(),
                discovered,
            });
        }
        batches.retain(|_, batch| batch.remaining > 0);
    }

    fn on_batch(&self, batch_id: u64, requests: Vec<Request>) {
        self.pull_outstanding.store(false, Ordering::Relaxed);
        let mut inner = self.inner.lock();
        inner.batches.insert(
            batch_id,
            BatchState {
                remaining: requests.len(),
                results: Vec::new(),
            },
        );
        for request in requests {
            inner
                .key_to_batch
                .insert(request.unique_key.clone(), batch_id);
            inner.pending.push_back(request);
        }
    }

    fn on_idle(&self) {
        self.inner.lock().idle_until = Some(Instant::now() + IDLE_BACKOFF);
        self.pull_outstanding.store(false, Ordering::Relaxed);
    }

    fn on_stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    fn record_ok(&self, unique_key: &str, items: Vec<Value>) {
        self.inner.lock().outcomes.insert(
            unique_key.to_owned(),
            Outcome {
                ok: true,
                items,
                error: None,
            },
        );
    }

    /// Sends a pull when the local buffer runs low. Called periodically.
    fn maybe_pull(&self) {
        if self.stopped.load(Ordering::Relaxed) || self.tx.lock().is_none() {
            return;
        }
        {
            let inner = self.inner.lock();
            if inner.pending.len() >= self.config.node_concurrency {
                return;
            }
            if inner.idle_until.is_some_and(|until| Instant::now() < until) {
                return;
            }
        }
        if self.pull_outstanding.swap(true, Ordering::Relaxed) {
            return;
        }
        self.send(NodeMessage::Pull {
            max: self.config.batch_size.max(1),
        });
    }
}

fn operation_info(request: &Request) -> QueueOperationInfo {
    QueueOperationInfo {
        request_id: request
            .id
            .clone()
            .unwrap_or_else(|| unique_key_to_request_id(&request.unique_key)),
        was_already_present: true,
        was_already_handled: false,
    }
}

#[async_trait]
impl RequestManager for HubRequestManager {
    async fn add_requests(
        &self,
        requests: Vec<Request>,
        _forefront: bool,
    ) -> StorageResult<BatchAddRequestsResult> {
        let mut inner = self.inner.lock();
        let processed = requests
            .iter()
            .map(|request| ProcessedRequest {
                unique_key: request.unique_key.clone(),
                request_id: unique_key_to_request_id(&request.unique_key),
                was_already_present: false,
                was_already_handled: false,
            })
            .collect();
        inner.discovered.extend(requests);
        Ok(BatchAddRequestsResult {
            processed_requests: processed,
            unprocessed_requests: Vec::new(),
        })
    }

    async fn fetch_next_request(&self) -> StorageResult<Option<Request>> {
        let mut inner = self.inner.lock();
        let next = inner.pending.pop_front();
        if let Some(request) = &next {
            inner.in_progress.insert(request.unique_key.clone());
        }
        Ok(next)
    }

    async fn mark_request_as_handled(
        &self,
        request: &mut Request,
    ) -> StorageResult<Option<QueueOperationInfo>> {
        let message = {
            let mut inner = self.inner.lock();
            inner.in_progress.remove(&request.unique_key);
            let outcome = inner
                .outcomes
                .remove(&request.unique_key)
                .unwrap_or_else(|| Outcome {
                    ok: false,
                    items: Vec::new(),
                    error: request.error_messages.last().cloned(),
                });
            let result = RequestResult {
                unique_key: request.unique_key.clone(),
                ok: outcome.ok,
                items: outcome.items,
                error: outcome.error,
            };
            let batch_id = inner.key_to_batch.remove(&request.unique_key);
            let mut done = None;
            match batch_id.and_then(|id| inner.batches.get_mut(&id).map(|b| (id, b))) {
                Some((id, batch)) => {
                    batch.results.push(result);
                    batch.remaining = batch.remaining.saturating_sub(1);
                    if batch.remaining == 0 {
                        let batch = inner.batches.remove(&id).expect("batch exists");
                        let discovered = std::mem::take(&mut inner.discovered);
                        done = Some(NodeMessage::Done {
                            batch_id: id,
                            results: batch.results,
                            discovered,
                        });
                    }
                }
                None => {
                    let discovered = std::mem::take(&mut inner.discovered);
                    done = Some(NodeMessage::Done {
                        batch_id: u64::MAX,
                        results: vec![result],
                        discovered,
                    });
                }
            }
            done
        };
        if let Some(message) = message {
            self.send(message);
        }
        self.handled.fetch_add(1, Ordering::Relaxed);
        Ok(Some(operation_info(request)))
    }

    async fn reclaim_request(
        &self,
        request: &Request,
        forefront: bool,
    ) -> StorageResult<Option<QueueOperationInfo>> {
        let mut inner = self.inner.lock();
        inner.in_progress.remove(&request.unique_key);
        if forefront {
            inner.pending.push_front(request.clone());
        } else {
            inner.pending.push_back(request.clone());
        }
        Ok(Some(operation_info(request)))
    }

    async fn is_empty(&self) -> StorageResult<bool> {
        Ok(self.inner.lock().pending.is_empty())
    }

    async fn is_finished(&self) -> StorageResult<bool> {
        let inner = self.inner.lock();
        Ok(self.stopped.load(Ordering::Relaxed)
            && inner.pending.is_empty()
            && inner.in_progress.is_empty())
    }

    async fn handled_count(&self) -> StorageResult<u64> {
        Ok(self.handled.load(Ordering::Relaxed))
    }

    async fn pending_count(&self) -> StorageResult<u64> {
        let inner = self.inner.lock();
        Ok((inner.pending.len() + inner.in_progress.len()) as u64)
    }

    fn ready_at(&self) -> Option<Instant> {
        Some(Instant::now() + PULL_INTERVAL * 2)
    }
}

fn websocket_url(hub_url: &str, secret: &str) -> anyhow::Result<String> {
    let mut url = url::Url::parse(hub_url).with_context(|| format!("invalid hub URL {hub_url}"))?;
    let scheme = match url.scheme() {
        "https" | "wss" => "wss",
        _ => "ws",
    };
    url.set_scheme(scheme)
        .ok()
        .context("setting the websocket scheme")?;
    url.set_path("/ws");
    url.set_query(Some(&format!("secret={secret}")));
    Ok(url.to_string())
}

/// Opens the websocket and completes the hello/welcome handshake.
async fn connect(
    ws_url: &str,
    node_id: &str,
    run_id: Option<String>,
    secret: &str,
) -> anyhow::Result<(WsStream, ScrapeConfig)> {
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url)
        .await
        .context("websocket connect")?;
    let hello = NodeMessage::Hello {
        node_id: node_id.to_owned(),
        run_id,
        secret: secret.to_owned(),
    };
    ws.send(Message::Text(serde_json::to_string(&hello)?.into()))
        .await?;
    let welcome = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        while let Some(message) = ws.next().await {
            if let Message::Text(text) = message? {
                match serde_json::from_str::<HubMessage>(text.as_str())? {
                    HubMessage::Welcome { config } => return Ok(config),
                    HubMessage::Rejected { reason } => bail!("hub rejected the node: {reason}"),
                    _ => continue,
                }
            }
        }
        bail!("hub closed the connection during the handshake")
    })
    .await
    .context("handshake timed out")??;
    Ok((ws, welcome))
}

async fn connect_with_retries(
    ws_url: &str,
    node_id: &str,
    run_id: Option<String>,
    secret: &str,
    budget: Duration,
) -> anyhow::Result<(WsStream, ScrapeConfig)> {
    let deadline = Instant::now() + budget;
    let mut delay = Duration::from_secs(1);
    loop {
        match connect(ws_url, node_id, run_id.clone(), secret).await {
            Ok(connected) => return Ok(connected),
            Err(error) if Instant::now() + delay < deadline => {
                tracing::warn!(%error, "Connecting to the hub failed, retrying in {delay:?}");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
            Err(error) => return Err(error.context("giving up connecting to the hub")),
        }
    }
}

/// Pumps one connection: outgoing messages from the manager, incoming ones into it. Returns
/// when the connection ends.
async fn pump(ws: WsStream, manager: &HubRequestManager) {
    let (mut ws_sink, mut ws_stream): (SplitSink<WsStream, Message>, SplitStream<WsStream>) =
        ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<NodeMessage>();
    manager.on_connected(tx);
    let writer = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let text = serde_json::to_string(&message).expect("node messages serialize");
            if ws_sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });
    while let Some(Ok(message)) = ws_stream.next().await {
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => break,
            _ => continue,
        };
        match serde_json::from_str::<HubMessage>(text.as_str()) {
            Ok(HubMessage::Batch { batch_id, requests }) => manager.on_batch(batch_id, requests),
            Ok(HubMessage::Idle) => manager.on_idle(),
            Ok(HubMessage::Stop) => {
                tracing::info!("Hub reported the crawl complete");
                manager.on_stop();
            }
            Ok(HubMessage::Pong) | Ok(HubMessage::Welcome { .. }) => {}
            Ok(HubMessage::Rejected { reason }) => {
                tracing::error!(%reason, "Hub rejected this node");
                manager.on_stop();
                break;
            }
            Err(error) => tracing::warn!(%error, "Dropping an unparsable hub message"),
        }
    }
    writer.abort();
    manager.on_disconnected();
}

async fn supervise(
    first: WsStream,
    ws_url: String,
    node_id: String,
    run_id: Option<String>,
    secret: String,
    manager: Arc<HubRequestManager>,
) {
    let mut connection = Some(first);
    loop {
        if let Some(ws) = connection.take() {
            pump(ws, &manager).await;
        }
        if manager.stopped.load(Ordering::Relaxed) {
            return;
        }
        tracing::warn!("Lost the hub connection, reconnecting");
        match connect_with_retries(
            &ws_url,
            &node_id,
            run_id.clone(),
            &secret,
            FIRST_CONNECT_BUDGET,
        )
        .await
        {
            Ok((ws, _)) => connection = Some(ws),
            Err(error) => {
                tracing::error!(%error, "Could not reconnect to the hub, stopping");
                manager.on_stop();
                return;
            }
        }
    }
}

fn proxy_url(config: &ScrapeConfig, env: &ActorEnv) -> Option<String> {
    if !config.use_apify_proxy {
        return None;
    }
    let Some(password) = &env.proxy_password else {
        tracing::warn!(
            "useApifyProxy is set but APIFY_PROXY_PASSWORD is missing, crawling without a proxy"
        );
        return None;
    };
    let username = if config.apify_proxy_groups.is_empty() {
        "auto".to_owned()
    } else {
        format!("groups-{}", config.apify_proxy_groups.join("+"))
    };
    Some(format!(
        "http://{username}:{password}@{}:{}",
        env.proxy_hostname, env.proxy_port
    ))
}

fn build_crawler(
    config: Arc<ScrapeConfig>,
    manager: Arc<HubRequestManager>,
    env: &ActorEnv,
) -> anyhow::Result<BasicCrawler<HtmlPipeline>> {
    let options = HttpCrawlerOptions {
        navigation_timeout: Duration::from_secs(config.request_timeout_secs),
        ..HttpCrawlerOptions::default()
    };

    let handler_manager = manager.clone();
    let handler_config = config.clone();
    let handler = move |ctx: HtmlContext| {
        let manager = handler_manager.clone();
        let config = handler_config.clone();
        async move {
            let unique_key = ctx.request().unique_key.clone();
            let url = ctx.url().to_string();
            let status = ctx.status().as_u16();
            let extract_config = config.clone();
            let item = ctx
                .with_html(move |doc| extract::extract(&extract_config.extract, doc, &url, status))
                .await??;

            if let Some(selector) = &config.follow_selector
                && ctx.request().crawl_depth() < config.max_depth
            {
                let mut options = EnqueueLinksOptions::new().selector(selector.clone());
                if !config.follow_globs.is_empty() {
                    options = options
                        .strategy(EnqueueStrategy::All)
                        .include(config.follow_globs.iter().cloned());
                }
                ctx.enqueue_links(options).await?;
            }
            manager.record_ok(&unique_key, vec![item]);
            Ok(())
        }
    };

    let mut builder = HtmlCrawler::builder_with_options(options)
        .services(Services::new(Arc::new(MemoryStorageBackend::new())))
        .request_manager(manager.clone() as Arc<dyn RequestManager>)
        .max_concurrency(config.node_concurrency.max(1))
        .min_concurrency(config.node_concurrency.clamp(1, 5))
        .max_request_retries(config.max_retries)
        .max_crawl_depth(config.max_depth)
        .request_handler(handler);

    if let Some(url) = proxy_url(&config, env) {
        builder = builder.proxy_configuration(Arc::new(ProxyConfiguration::new([url.as_str()])?));
    }
    Ok(builder.build()?)
}

pub async fn run_node(hub_url: String, secret: String, env: &ActorEnv) -> anyhow::Result<()> {
    let ws_url = websocket_url(&hub_url, &secret)?;
    let node_id = env
        .run_id
        .clone()
        .unwrap_or_else(|| format!("local-{}", std::process::id()));
    tracing::info!(%node_id, hub = %hub_url, "Node connecting to the hub");

    let (ws, config) = connect_with_retries(
        &ws_url,
        &node_id,
        env.run_id.clone(),
        &secret,
        FIRST_CONNECT_BUDGET,
    )
    .await?;
    tracing::info!(
        concurrency = config.node_concurrency,
        batch = config.batch_size,
        "Node received the scrape configuration"
    );
    let config = Arc::new(config);
    let manager = Arc::new(HubRequestManager::new((*config).clone()));

    let supervisor = tokio::spawn(supervise(
        ws,
        ws_url,
        node_id,
        env.run_id.clone(),
        secret,
        manager.clone(),
    ));
    let pumper = {
        let manager = manager.clone();
        tokio::spawn(async move {
            let mut last_ping = Instant::now();
            loop {
                tokio::time::sleep(PULL_INTERVAL).await;
                manager.maybe_pull();
                if last_ping.elapsed() >= PING_INTERVAL {
                    manager.send(NodeMessage::Ping);
                    last_ping = Instant::now();
                }
            }
        })
    };

    let crawler = build_crawler(config, manager.clone(), env)?;
    let stats = crawler.run(Vec::<Request>::new()).await?;
    pumper.abort();
    supervisor.abort();
    tracing::info!(?stats, "Node finished");
    Ok(())
}
