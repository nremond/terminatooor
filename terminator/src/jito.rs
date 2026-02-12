//! Jito bundle submission for MEV-protected liquidations
//!
//! Jito bundles provide:
//! - Atomic execution (all-or-nothing)
//! - Priority inclusion via tips
//! - MEV protection (bundle contents hidden until execution)

use std::time::Duration;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BS64, Engine};
use solana_sdk::{
    pubkey::Pubkey,
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

/// Jito Block Engine endpoints
const JITO_BLOCK_ENGINE_URL: &str = "https://mainnet.block-engine.jito.wtf";

/// Configuration for Jito bundle submission
#[derive(Debug, Clone)]
pub struct JitoConfig {
    /// Block engine URL
    pub block_engine_url: String,
    /// Tip amount in lamports (default: 10_000 = 0.00001 SOL)
    pub tip_lamports: u64,
    /// Whether Jito is enabled
    pub enabled: bool,
}

impl Default for JitoConfig {
    fn default() -> Self {
        Self {
            block_engine_url: JITO_BLOCK_ENGINE_URL.to_string(),
            tip_lamports: 10_000, // 0.00001 SOL default tip
            enabled: false,
        }
    }
}

impl JitoConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        let enabled = std::env::var("JITO_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let block_engine_url = std::env::var("JITO_BLOCK_ENGINE_URL")
            .unwrap_or_else(|_| JITO_BLOCK_ENGINE_URL.to_string());

        let tip_lamports = std::env::var("JITO_TIP_LAMPORTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000);

        Self {
            block_engine_url,
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

/// Jito bundle submission client
pub struct JitoClient {
    config: JitoConfig,
    http_client: reqwest::Client,
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

    /// Submit a bundle of transactions to Jito
    ///
    /// The bundle should contain:
    /// 1. Your main transaction(s)
    /// 2. A tip transaction (transfer SOL to Jito tip account)
    ///
    /// All transactions in the bundle execute atomically or not at all.
    pub async fn send_bundle(&self, transactions: Vec<VersionedTransaction>) -> Result<String> {
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

        // Build JSON-RPC request
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [encoded_txs]
        });

        let url = format!("{}/api/v1/bundles", self.config.block_engine_url);

        debug!("Sending bundle with {} transactions to Jito", transactions.len());

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

        info!("Jito bundle submitted: {}", bundle_id);
        Ok(bundle_id.to_string())
    }

    /// Get the status of a submitted bundle
    pub async fn get_bundle_status(&self, bundle_id: &str) -> Result<BundleStatus> {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBundleStatuses",
            "params": [[bundle_id]]
        });

        let url = format!("{}/api/v1/bundles", self.config.block_engine_url);

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
        let bundle_id = self.send_bundle(transactions).await?;

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
}
