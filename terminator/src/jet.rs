//! Yellowstone Jet SWQoS transaction propagation
//!
//! Jet provides Stake-Weighted Quality of Service (SWQoS) — direct QUIC delivery
//! to TPU leaders with priority based on validator stake. It exposes a standard
//! Solana `sendTransaction` JSON-RPC endpoint.
//!
//! Used as a fire-and-forget parallel path alongside Jito bundle submission
//! to increase transaction landing probability.

use std::time::Duration;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BS64, Engine};
use solana_sdk::transaction::VersionedTransaction;
use tracing::debug;

/// Client for Yellowstone Jet's `sendTransaction` RPC endpoint
pub struct JetClient {
    endpoint: String,
    http_client: reqwest::Client,
    enabled: bool,
}

impl JetClient {
    /// Load configuration from environment variables
    ///
    /// Environment variables:
    /// - `YELLOWSTONE_JET_URL`: Jet's Solana-like RPC endpoint (e.g., `http://127.0.0.1:8000`).
    ///   If not set or empty, Jet is disabled.
    pub fn from_env() -> Self {
        let endpoint = std::env::var("YELLOWSTONE_JET_URL")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_default();
        let enabled = !endpoint.is_empty();

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        Self {
            endpoint,
            http_client,
            enabled,
        }
    }

    /// Check if Jet is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the configured endpoint URL
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Send a transaction via Jet's `sendTransaction` JSON-RPC endpoint
    ///
    /// Returns the transaction signature string from the response.
    pub async fn send_transaction(&self, tx: &VersionedTransaction) -> Result<String> {
        if !self.enabled {
            return Err(anyhow!("Jet is not enabled"));
        }

        let serialized = bincode::serialize(tx)?;
        let encoded = BS64.encode(&serialized);

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [encoded, {"encoding": "base64"}]
        });

        debug!("Sending transaction to Jet endpoint: {}", self.endpoint);

        let response = self
            .http_client
            .post(&self.endpoint)
            .json(&request)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to send Jet request: {}", e))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!("Jet sendTransaction failed: {} - {}", status, body));
        }

        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse Jet response: {} - {}", e, body))?;

        if let Some(error) = json.get("error") {
            return Err(anyhow!("Jet error: {}", error));
        }

        let signature = json
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| anyhow!("No signature in Jet response: {}", body))?;

        Ok(signature.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_env_disabled_by_default() {
        // Without YELLOWSTONE_JET_URL set, client should be disabled
        let client = JetClient::from_env();
        assert!(!client.is_enabled());
        assert!(client.endpoint().is_empty());
    }
}
