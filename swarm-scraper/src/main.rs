//! Swarm scraper: one hub run coordinates node runs over a websocket served on the hub's
//! container URL. See README.md for the design.

mod apify;
mod config;
mod extract;
mod hub;
mod node;
mod protocol;

use anyhow::Context as _;
use tracing_subscriber::EnvFilter;

use crate::config::{ActorEnv, Input, Mode};

async fn load_input(env: &ActorEnv) -> anyhow::Result<Input> {
    if let Some(path) = std::env::args().nth(1) {
        let text = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("reading input file {path}"))?;
        return serde_json::from_str(&text).with_context(|| format!("parsing input file {path}"));
    }
    if env.api_enabled() {
        let client =
            apify::ApifyClient::new(&env.api_base_url, env.token.as_deref().unwrap_or_default())?;
        let store_id = env
            .default_key_value_store_id
            .as_deref()
            .context("ACTOR_DEFAULT_KEY_VALUE_STORE_ID is not set")?;
        let input: Option<Input> = client.get_record(store_id, &env.input_key).await?;
        return input.context("the default key-value store has no input record");
    }
    let path = env
        .storage_dir
        .join("key_value_stores/default")
        .join(format!("{}.json", env.input_key));
    let text = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("reading local input {}", path.display()))?;
    serde_json::from_str(&text).context("parsing local input")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,crawlee=warn")),
        )
        .init();

    let env = ActorEnv::from_env();
    let input = load_input(&env).await?;
    tracing::info!(mode = ?input.mode, at_home = env.is_at_home, "Swarm scraper starting");

    match input.mode {
        Mode::Hub | Mode::Both => hub::run_hub(input, env).await,
        Mode::Node => {
            let hub_url = input
                .hub_url
                .clone()
                .context("node mode needs hubUrl in the input")?;
            let secret = input
                .secret
                .clone()
                .context("node mode needs the hub secret in the input")?;
            node::run_node(hub_url, secret, &env).await
        }
    }
}
