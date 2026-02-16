//! Jito bundle submission for MEV-protected liquidations
//!
//! Jito bundles provide:
//! - Atomic execution (all-or-nothing)
//! - Priority inclusion via tips
//! - MEV protection (bundle contents hidden until execution)
//!
//! Supports two submission modes:
//! - Direct Jito Block Engine (default): Regional endpoints with failover
//! - Triton RPC pass-through: Your Triton RPC endpoint with Jito support enabled
//!   (requires IP whitelisting with Jito Block Engine)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BS64, Engine};
use futures::future::select_ok;
use solana_sdk::{
    hash::Hash,
    message::{v0, VersionedMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    system_instruction,
    transaction::VersionedTransaction,
};
use tracing::{debug, info, warn};

/// Jito tip accounts - one is randomly selected per bundle
/// These are the official Jito tip payment addresses
const JITO_TIP_ACCOUNTS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// Jito Block Engine regional endpoints (EU only for lower latency)
/// All endpoints are raced in parallel for fastest response
const JITO_REGIONAL_ENDPOINTS: [&str; 4] = [
    "https://amsterdam.mainnet.block-engine.jito.wtf",
    "https://dublin.mainnet.block-engine.jito.wtf",
    "https://frankfurt.mainnet.block-engine.jito.wtf",
    "https://london.mainnet.block-engine.jito.wtf",
];

/// Endpoint type for Jito bundle submission
#[derive(Debug, Clone, PartialEq)]
pub enum JitoEndpointType {
    /// Direct Jito Block Engine - appends /api/v1/bundles to the URL
    /// Supports multi-endpoint failover on rate limiting
    BlockEngine,
    /// Triton RPC pass-through - uses the URL as-is (standard JSON-RPC)
    /// Requires IP whitelisting with Jito Block Engine
    Triton,
}

impl Default for JitoEndpointType {
    fn default() -> Self {
        Self::BlockEngine
    }
}

/// Configuration for Jito bundle submission
#[derive(Debug, Clone)]
pub struct JitoConfig {
    /// Primary bundle submission endpoint URL (used for Triton or as first choice for BlockEngine)
    pub endpoint_url: String,
    /// Endpoint type (determines URL format and failover behavior)
    pub endpoint_type: JitoEndpointType,
    /// Tip amount in lamports (default: 10_000 = 0.00001 SOL)
    pub tip_lamports: u64,
    /// Whether Jito is enabled
    pub enabled: bool,
}

impl Default for JitoConfig {
    fn default() -> Self {
        Self {
            endpoint_url: JITO_REGIONAL_ENDPOINTS[0].to_string(),
            endpoint_type: JitoEndpointType::BlockEngine,
            tip_lamports: 10_000, // 0.00001 SOL default tip
            enabled: false,
        }
    }
}

impl JitoConfig {
    /// Load configuration from environment variables
    ///
    /// Environment variables:
    /// - JITO_ENABLED: "true" or "1" to enable Jito bundles
    /// - JITO_ENDPOINT_URL: Primary URL for bundle submission (optional for BlockEngine)
    /// - JITO_ENDPOINT_TYPE: "triton" for Triton RPC, "block_engine" for direct Jito (default)
    /// - JITO_TIP_LAMPORTS: Tip amount in lamports (default: 10_000)
    ///
    /// For BlockEngine mode (default):
    ///   - Uses regional endpoints with automatic failover on rate limiting
    ///   - JITO_ENDPOINT_URL is optional (defaults to global endpoint)
    ///
    /// For Triton mode:
    ///   JITO_ENDPOINT_URL=https://your-pool.mainnet.rpcpool.com
    ///   JITO_ENDPOINT_TYPE=triton
    ///   Note: Requires your IP to be whitelisted with Jito Block Engine
    pub fn from_env() -> Self {
        let enabled = std::env::var("JITO_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let endpoint_url = std::env::var("JITO_ENDPOINT_URL")
            .or_else(|_| std::env::var("JITO_BLOCK_ENGINE_URL")) // backwards compatibility
            .unwrap_or_else(|_| JITO_REGIONAL_ENDPOINTS[0].to_string());

        let endpoint_type = std::env::var("JITO_ENDPOINT_TYPE")
            .map(|v| match v.to_lowercase().as_str() {
                "triton" => JitoEndpointType::Triton,
                _ => JitoEndpointType::BlockEngine,
            })
            .unwrap_or(JitoEndpointType::BlockEngine);

        let tip_lamports = std::env::var("JITO_TIP_LAMPORTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);

        Self {
            endpoint_url,
            endpoint_type,
            tip_lamports,
            enabled,
        }
    }
}

/// Get a random Jito tip account
pub fn get_random_tip_account() -> Pubkey {
    use rand::Rng;
    let idx = rand::thread_rng().gen_range(0..JITO_TIP_ACCOUNTS.len());
    JITO_TIP_ACCOUNTS[idx].parse().unwrap()
}

/// Create a tip instruction to pay Jito validators
pub fn create_tip_instruction(payer: &Pubkey, tip_lamports: u64) -> solana_sdk::instruction::Instruction {
    let tip_account = get_random_tip_account();
    system_instruction::transfer(payer, &tip_account, tip_lamports)
}

/// Jito bundle submission client with multi-endpoint failover
pub struct JitoClient {
    config: JitoConfig,
    http_client: reqwest::Client,
    /// Index of last successful endpoint (for BlockEngine mode)
    last_successful_endpoint: AtomicUsize,
}

impl JitoClient {
    pub fn new(config: JitoConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        Self {
            config,
            http_client,
            last_successful_endpoint: AtomicUsize::new(0),
        }
    }

    /// Check if Jito is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the configured tip amount
    pub fn tip_lamports(&self) -> u64 {
        self.config.tip_lamports
    }

    /// Get the bundle URL for a given endpoint base URL
    fn bundle_url_for_endpoint(&self, endpoint: &str) -> String {
        match self.config.endpoint_type {
            JitoEndpointType::BlockEngine => {
                format!("{}/api/v1/bundles", endpoint)
            }
            JitoEndpointType::Triton => {
                // Triton uses standard JSON-RPC endpoint
                endpoint.to_string()
            }
        }
    }

    /// Get list of endpoints to try (in order)
    fn get_endpoints_to_try(&self) -> Vec<String> {
        match self.config.endpoint_type {
            JitoEndpointType::Triton => {
                // For Triton, only use the configured endpoint
                vec![self.config.endpoint_url.clone()]
            }
            JitoEndpointType::BlockEngine => {
                // For BlockEngine, try multiple regional endpoints
                let last_successful = self.last_successful_endpoint.load(Ordering::Relaxed);
                let mut endpoints = Vec::with_capacity(JITO_REGIONAL_ENDPOINTS.len() + 1);

                // First try the configured endpoint if it's not in the regional list
                if !JITO_REGIONAL_ENDPOINTS.contains(&self.config.endpoint_url.as_str()) {
                    endpoints.push(self.config.endpoint_url.clone());
                }

                // Then try regional endpoints, starting from the last successful one
                for i in 0..JITO_REGIONAL_ENDPOINTS.len() {
                    let idx = (last_successful + i) % JITO_REGIONAL_ENDPOINTS.len();
                    let endpoint = JITO_REGIONAL_ENDPOINTS[idx].to_string();
                    if !endpoints.contains(&endpoint) {
                        endpoints.push(endpoint);
                    }
                }

                endpoints
            }
        }
    }

    /// Submit a bundle to a specific endpoint
    async fn send_bundle_to_endpoint(
        &self,
        encoded_txs: &[String],
        endpoint: &str,
    ) -> Result<String> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [encoded_txs, {"encoding": "base64"}]
        });

        let url = self.bundle_url_for_endpoint(endpoint);

        debug!("Sending bundle to Jito endpoint: {}", url);

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to send bundle request: {}", e))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(anyhow!("Jito bundle submission failed: {} - {}", status, body));
        }

        // Parse response to get bundle ID
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse Jito response: {} - {}", e, body))?;

        if let Some(error) = json.get("error") {
            return Err(anyhow!("Jito error: {}", error));
        }

        let bundle_id = json
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| anyhow!("No bundle ID in response: {}", body))?;

        Ok(bundle_id.to_string())
    }

    /// Submit a bundle of transactions to Jito with parallel multi-endpoint submission
    ///
    /// The bundle should contain:
    /// 1. Your main transaction(s)
    /// 2. A tip transaction (transfer SOL to Jito tip account)
    ///
    /// All transactions in the bundle execute atomically or not at all.
    ///
    /// Sends to ALL regional endpoints simultaneously and returns the first successful response.
    /// This maximizes the chance of getting through during high congestion.
    pub async fn send_bundle(&self, transactions: Vec<VersionedTransaction>) -> Result<(String, String)> {
        if !self.config.enabled {
            return Err(anyhow!("Jito is not enabled"));
        }

        if transactions.is_empty() {
            return Err(anyhow!("Bundle cannot be empty"));
        }

        // Serialize transactions to base64
        let encoded_txs: Vec<String> = transactions
            .iter()
            .map(|tx| {
                let serialized = bincode::serialize(tx).unwrap();
                BS64.encode(&serialized)
            })
            .collect();

        let endpoints = self.get_endpoints_to_try();

        // For Triton mode or single endpoint, just send to that one
        if endpoints.len() == 1 {
            let bundle_id = self.send_bundle_to_endpoint(&encoded_txs, &endpoints[0]).await?;
            return Ok((bundle_id, endpoints[0].clone()));
        }

        // For BlockEngine mode, send to ALL endpoints in parallel and race them
        info!("Sending bundle to {} Jito endpoints in parallel", endpoints.len());

        let futures: Vec<_> = endpoints
            .iter()
            .map(|endpoint| {
                let encoded_txs = encoded_txs.clone();
                let endpoint = endpoint.clone();
                Box::pin(async move {
                    self.send_bundle_to_endpoint(&encoded_txs, &endpoint)
                        .await
                        .map(|bundle_id| (bundle_id, endpoint))
                })
            })
            .collect();

        // Race all futures - return the first successful result
        match select_ok(futures).await {
            Ok(((bundle_id, endpoint), _remaining)) => {
                // Remember this endpoint for future status checks
                if let Some(regional_idx) = JITO_REGIONAL_ENDPOINTS
                    .iter()
                    .position(|e| *e == endpoint.as_str())
                {
                    self.last_successful_endpoint.store(regional_idx, Ordering::Relaxed);
                }
                info!("Jito bundle submitted via {} (first to respond): {}", endpoint, bundle_id);
                Ok((bundle_id, endpoint))
            }
            Err(e) => {
                // All futures failed - return the last error
                Err(anyhow!("All Jito endpoints failed: {}", e))
            }
        }
    }

    /// Get the status of a submitted bundle
    pub async fn get_bundle_status(&self, bundle_id: &str) -> Result<BundleStatus> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBundleStatuses",
            "params": [[bundle_id]]
        });

        // Use the last successful endpoint or primary
        let endpoints = self.get_endpoints_to_try();
        let endpoint = endpoints.first().unwrap();
        let url = self.bundle_url_for_endpoint(endpoint);

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to get bundle status: {}", e))?;

        let body = response.text().await.unwrap_or_default();
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow!("Failed to parse status response: {}", e))?;

        if let Some(error) = json.get("error") {
            return Err(anyhow!("Jito error: {}", error));
        }

        let statuses = json
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("Invalid status response"))?;

        if statuses.is_empty() {
            return Ok(BundleStatus::Pending);
        }

        let status = &statuses[0];
        let confirmation_status = status
            .get("confirmation_status")
            .and_then(|s| s.as_str())
            .unwrap_or("pending");

        match confirmation_status {
            "confirmed" | "finalized" => {
                let slot = status
                    .get("slot")
                    .and_then(|s| s.as_u64())
                    .unwrap_or(0);
                Ok(BundleStatus::Landed { slot })
            }
            "processed" => Ok(BundleStatus::Processed),
            _ => Ok(BundleStatus::Pending),
        }
    }

    /// Submit bundle and wait for confirmation
    pub async fn send_bundle_and_confirm(
        &self,
        transactions: Vec<VersionedTransaction>,
        timeout: Duration,
    ) -> Result<BundleResult> {
        let (bundle_id, _endpoint) = self.send_bundle(transactions).await?;

        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(500);

        while start.elapsed() < timeout {
            match self.get_bundle_status(&bundle_id).await {
                Ok(BundleStatus::Landed { slot }) => {
                    info!("Jito bundle landed in slot {}", slot);
                    return Ok(BundleResult::Landed {
                        bundle_id,
                        slot,
                    });
                }
                Ok(BundleStatus::Processed) => {
                    debug!("Bundle processed, waiting for confirmation...");
                }
                Ok(BundleStatus::Pending) => {
                    debug!("Bundle pending...");
                }
                Ok(BundleStatus::Failed { reason }) => {
                    warn!("Bundle failed: {}", reason);
                    return Ok(BundleResult::Failed {
                        bundle_id,
                        reason,
                    });
                }
                Err(e) => {
                    debug!("Error checking bundle status: {}", e);
                }
            }

            tokio::time::sleep(poll_interval).await;
        }

        Ok(BundleResult::Timeout { bundle_id })
    }

    /// Test Jito connection by submitting a minimal bundle
    /// Creates a self-transfer of 0 lamports + tip to validate the endpoint works
    pub async fn test_connection(&self, payer: &Keypair, recent_blockhash: Hash) -> Result<String> {
        if !self.config.enabled {
            return Err(anyhow!("Jito is not enabled"));
        }

        info!("Testing Jito connection...");
        info!("  Primary endpoint: {}", self.config.endpoint_url);
        info!("  Type: {:?}", self.config.endpoint_type);
        info!("  Tip: {} lamports", self.config.tip_lamports);

        if self.config.endpoint_type == JitoEndpointType::BlockEngine {
            info!("  Failover endpoints: {} regional endpoints available", JITO_REGIONAL_ENDPOINTS.len());
        }

        let payer_pubkey = payer.pubkey();

        // Create a simple self-transfer (0 lamports) + tip instruction
        let transfer_ix = system_instruction::transfer(&payer_pubkey, &payer_pubkey, 0);
        let tip_ix = create_tip_instruction(&payer_pubkey, self.config.tip_lamports);

        // Build the transaction
        let message = v0::Message::try_compile(
            &payer_pubkey,
            &[transfer_ix, tip_ix],
            &[], // no lookup tables needed
            recent_blockhash,
        )?;

        let tx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[payer])?;

        info!("Submitting test bundle to Jito...");
        let (bundle_id, _endpoint) = self.send_bundle(vec![tx]).await?;
        info!("✓ Jito bundle accepted! Bundle ID: {}", bundle_id);

        Ok(bundle_id)
    }
}

/// Status of a submitted bundle
#[derive(Debug, Clone)]
pub enum BundleStatus {
    Pending,
    Processed,
    Landed { slot: u64 },
    Failed { reason: String },
}

/// Result of bundle submission
#[derive(Debug, Clone)]
pub enum BundleResult {
    Landed { bundle_id: String, slot: u64 },
    Failed { bundle_id: String, reason: String },
    Timeout { bundle_id: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tip_account_parsing() {
        for account_str in JITO_TIP_ACCOUNTS {
            let pubkey: Pubkey = account_str.parse().unwrap();
            assert_ne!(pubkey, Pubkey::default());
        }
    }

    #[test]
    fn test_config_from_env() {
        // Default config when no env vars set
        let config = JitoConfig::from_env();
        assert!(!config.enabled);
        assert_eq!(config.tip_lamports, 10_000);
    }

    #[test]
    fn test_regional_endpoints() {
        // Verify all regional endpoints are valid URLs
        for endpoint in JITO_REGIONAL_ENDPOINTS {
            assert!(endpoint.starts_with("https://"));
            assert!(endpoint.contains("jito.wtf"));
        }
    }

    #[test]
    fn test_get_endpoints_to_try() {
        let config = JitoConfig {
            endpoint_url: JITO_REGIONAL_ENDPOINTS[0].to_string(),
            endpoint_type: JitoEndpointType::BlockEngine,
            tip_lamports: 10_000,
            enabled: true,
        };
        let client = JitoClient::new(config);

        let endpoints = client.get_endpoints_to_try();
        assert!(!endpoints.is_empty());
        assert!(endpoints.len() <= JITO_REGIONAL_ENDPOINTS.len() + 1);
    }
}
