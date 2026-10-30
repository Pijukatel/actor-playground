//! Actor input, the scrape configuration shared with nodes, and the platform environment.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Coordinates: owns the frontier, hands out batches, writes the dataset, starts the nodes.
    #[default]
    Hub,
    /// Fetches and extracts: connects to the hub and scrapes whatever it is handed.
    Node,
    /// Hub plus one in-process node, for local runs and tests.
    Both,
}

/// One extracted field: a CSS selector alone, or a selector with an attribute and a multiplicity.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FieldSpec {
    Selector(String),
    Full {
        selector: String,
        /// `text` (default), `html`, `innerHtml`, or an attribute name such as `href`.
        #[serde(default)]
        attr: Option<String>,
        /// Collect every match into an array instead of the first match.
        #[serde(default)]
        all: bool,
    },
}

impl FieldSpec {
    pub fn parts(&self) -> (&str, &str, bool) {
        match self {
            FieldSpec::Selector(selector) => (selector, "text", false),
            FieldSpec::Full {
                selector,
                attr,
                all,
            } => (selector, attr.as_deref().unwrap_or("text"), *all),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartUrl {
    pub url: String,
}

/// Everything a node needs in order to scrape. The hub sends it in the welcome message, so the
/// node run's own input only has to say where the hub is.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScrapeConfig {
    #[serde(default)]
    pub extract: BTreeMap<String, FieldSpec>,
    #[serde(default)]
    pub follow_selector: Option<String>,
    #[serde(default)]
    pub follow_globs: Vec<String>,
    #[serde(default)]
    pub max_depth: u32,
    #[serde(default = "default_concurrency")]
    pub node_concurrency: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub use_apify_proxy: bool,
    #[serde(default)]
    pub apify_proxy_groups: Vec<String>,
    #[serde(default = "default_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_retries")]
    pub max_retries: u32,
}

fn default_concurrency() -> usize {
    20
}
fn default_batch_size() -> usize {
    50
}
fn default_timeout_secs() -> u64 {
    30
}
fn default_retries() -> u32 {
    2
}
fn default_node_memory() -> u32 {
    1024
}
fn default_lease_secs() -> u64 {
    600
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Input {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub start_urls: Vec<StartUrl>,
    /// A text file with one URL per line, streamed into the frontier at start.
    #[serde(default)]
    pub url_list_url: Option<String>,
    #[serde(flatten)]
    pub scrape: ScrapeConfig,
    /// How many node runs the hub starts. `0` runs no nodes, for a hub that only serves external
    /// nodes, or for `both` mode.
    #[serde(default)]
    pub nodes: u32,
    #[serde(default = "default_node_memory")]
    pub node_memory_mbytes: u32,
    /// Stop enqueuing once this many requests were seen. `0` means no limit.
    #[serde(default)]
    pub max_requests: u64,
    /// A batch handed to a node that does not report back within this time is handed out again.
    #[serde(default = "default_lease_secs")]
    pub lease_secs: u64,
    /// Node mode only: the hub's container URL.
    #[serde(default)]
    pub hub_url: Option<String>,
    /// Shared secret a node presents on its websocket handshake. The hub generates one when the
    /// input has none.
    #[serde(default)]
    pub secret: Option<String>,
}

/// The parts of the Actor runtime environment this Actor reads.
#[derive(Clone, Debug)]
pub struct ActorEnv {
    pub is_at_home: bool,
    pub token: Option<String>,
    pub api_base_url: String,
    pub actor_id: Option<String>,
    pub run_id: Option<String>,
    pub default_dataset_id: Option<String>,
    pub default_key_value_store_id: Option<String>,
    pub input_key: String,
    pub web_server_port: u16,
    pub web_server_url: Option<String>,
    pub proxy_password: Option<String>,
    pub proxy_hostname: String,
    pub proxy_port: u16,
    pub storage_dir: PathBuf,
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

impl ActorEnv {
    pub fn from_env() -> Self {
        Self {
            is_at_home: env_var("APIFY_IS_AT_HOME").is_some(),
            token: env_var("APIFY_TOKEN"),
            api_base_url: env_var("APIFY_API_BASE_URL")
                .unwrap_or_else(|| "https://api.apify.com/".to_owned()),
            actor_id: env_var("ACTOR_ID"),
            run_id: env_var("ACTOR_RUN_ID"),
            default_dataset_id: env_var("ACTOR_DEFAULT_DATASET_ID"),
            default_key_value_store_id: env_var("ACTOR_DEFAULT_KEY_VALUE_STORE_ID"),
            input_key: env_var("ACTOR_INPUT_KEY").unwrap_or_else(|| "INPUT".to_owned()),
            web_server_port: env_var("ACTOR_WEB_SERVER_PORT")
                .and_then(|p| p.parse().ok())
                .unwrap_or(4321),
            web_server_url: env_var("ACTOR_WEB_SERVER_URL"),
            proxy_password: env_var("APIFY_PROXY_PASSWORD"),
            proxy_hostname: env_var("APIFY_PROXY_HOSTNAME")
                .unwrap_or_else(|| "proxy.apify.com".to_owned()),
            proxy_port: env_var("APIFY_PROXY_PORT")
                .and_then(|p| p.parse().ok())
                .unwrap_or(8000),
            storage_dir: env_var("CRAWLEE_STORAGE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("storage")),
        }
    }

    /// The platform API is usable when the run has a token; locally the Actor works from files.
    pub fn api_enabled(&self) -> bool {
        self.is_at_home && self.token.is_some()
    }
}
