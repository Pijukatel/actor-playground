//! The few Apify API calls the hub makes: input and state records, dataset pushes, node runs.

use std::time::Duration;

use anyhow::{Context as _, bail};
use reqwest::{RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;

const MAX_ATTEMPTS: u32 = 6;

#[derive(Clone)]
pub struct ApifyClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl ApifyClient {
    pub fn new(base_url: &str, token: &str) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("building the API HTTP client")?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_owned(),
            token: token.to_owned(),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/v2/{}", self.base_url, path)
    }

    fn auth(&self, builder: RequestBuilder) -> RequestBuilder {
        builder.bearer_auth(&self.token)
    }

    /// Sends the request, retrying rate limits, server errors and transport errors with an
    /// exponential backoff. Returns the first response that is none of those.
    async fn send_with_retry(
        &self,
        build: impl Fn() -> RequestBuilder,
    ) -> anyhow::Result<Response> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let outcome = self.auth(build()).send().await;
            let retry_reason = match &outcome {
                Ok(response) if response.status() == StatusCode::TOO_MANY_REQUESTS => {
                    "rate limited".to_owned()
                }
                Ok(response) if response.status().is_server_error() => {
                    format!("status {}", response.status())
                }
                Ok(_) => return Ok(outcome?),
                Err(error) => format!("transport error: {error}"),
            };
            if attempt >= MAX_ATTEMPTS {
                bail!("API request failed after {attempt} attempts ({retry_reason})");
            }
            let delay =
                Duration::from_millis(500 * 2u64.pow(attempt - 1)).min(Duration::from_secs(10));
            tracing::warn!(attempt, %retry_reason, "API request failed, retrying in {delay:?}");
            tokio::time::sleep(delay).await;
        }
    }

    pub async fn get_record<T: DeserializeOwned>(
        &self,
        store_id: &str,
        key: &str,
    ) -> anyhow::Result<Option<T>> {
        let url = self.url(&format!("key-value-stores/{store_id}/records/{key}"));
        let response = self.send_with_retry(|| self.http.get(&url)).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response.error_for_status()?;
        Ok(Some(response.json().await?))
    }

    pub async fn set_record(
        &self,
        store_id: &str,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> anyhow::Result<()> {
        let url = self.url(&format!("key-value-stores/{store_id}/records/{key}"));
        let response = self
            .send_with_retry(|| {
                self.http
                    .put(&url)
                    .header("content-type", content_type)
                    .body(body.clone())
            })
            .await?;
        response.error_for_status()?;
        Ok(())
    }

    /// Pushes one JSON array of items.
    pub async fn push_items(&self, dataset_id: &str, body: Vec<u8>) -> anyhow::Result<()> {
        let url = self.url(&format!("datasets/{dataset_id}/items"));
        let response = self
            .send_with_retry(|| {
                self.http
                    .post(&url)
                    .header("content-type", "application/json")
                    .body(body.clone())
            })
            .await?;
        response.error_for_status()?;
        Ok(())
    }

    /// Starts a run of the Actor and returns its ID.
    pub async fn start_run(
        &self,
        actor_id: &str,
        input: &Value,
        memory_mbytes: u32,
    ) -> anyhow::Result<String> {
        let url = self.url(&format!("acts/{actor_id}/runs?memory={memory_mbytes}"));
        let response = self
            .send_with_retry(|| self.http.post(&url).json(input))
            .await?;
        let body: Value = response.error_for_status()?.json().await?;
        body["data"]["id"]
            .as_str()
            .map(str::to_owned)
            .context("run response without an id")
    }

    pub async fn abort_run(&self, run_id: &str) -> anyhow::Result<()> {
        let url = self.url(&format!("actor-runs/{run_id}/abort"));
        let response = self.send_with_retry(|| self.http.post(&url)).await?;
        response.error_for_status()?;
        Ok(())
    }
}
