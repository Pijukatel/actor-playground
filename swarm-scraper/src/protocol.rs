//! Messages exchanged over the websocket between the hub and its nodes. Every frame is one JSON
//! object with a `type` field.

use crawlee::Request;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::ScrapeConfig;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum NodeMessage {
    /// First frame on a connection. The hub answers with `Welcome` or `Rejected`.
    Hello {
        node_id: String,
        run_id: Option<String>,
        secret: String,
    },
    /// The node has room for more work.
    Pull {
        max: usize,
    },
    /// Outcome of every request of a batch, plus links the node discovered meanwhile.
    Done {
        batch_id: u64,
        results: Vec<RequestResult>,
        discovered: Vec<Request>,
    },
    Ping,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestResult {
    pub unique_key: String,
    pub ok: bool,
    #[serde(default)]
    pub items: Vec<Value>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum HubMessage {
    Welcome {
        config: ScrapeConfig,
    },
    Batch {
        batch_id: u64,
        requests: Vec<Request>,
    },
    /// Nothing to hand out right now; pull again later.
    Idle,
    /// The crawl is complete; finish in-progress requests and exit.
    Stop,
    Pong,
    Rejected {
        reason: String,
    },
}
