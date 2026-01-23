//! Geyser/LaserStream integration for real-time obligation monitoring.
//!
//! This module provides streaming updates from Helius LaserStream (Yellowstone gRPC)
//! to detect obligation changes in real-time instead of polling.

use std::collections::HashMap;
use std::time::Duration;

use anchor_lang::prelude::Pubkey;
use anyhow::{anyhow, Result};
use backoff::ExponentialBackoff;
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    subscribe_update::UpdateOneof,
};

/// Configuration for connecting to Helius LaserStream
#[derive(Debug, Clone)]
pub struct GeyserConfig {
    /// LaserStream endpoint (e.g., "https://laserstream-mainnet-ewr.helius-rpc.com")
    pub endpoint: String,
    /// Helius API key for authentication
    pub api_key: String,
    /// Kamino lending program ID to monitor
    pub program_id: Pubkey,
}

/// An update received from the Geyser stream
#[derive(Debug, Clone)]
pub struct ObligationUpdate {
    /// The obligation account pubkey that was updated
    pub pubkey: Pubkey,
    /// The slot at which the update occurred
    pub slot: u64,
    /// The raw account data (can be deserialized to Obligation)
    pub data: Vec<u8>,
}

/// Geyser stream handle for receiving obligation updates
pub struct GeyserStream {
    config: GeyserConfig,
    /// Channel to receive obligation updates
    rx: mpsc::Receiver<ObligationUpdate>,
    /// Handle to the background streaming task
    _task_handle: tokio::task::JoinHandle<()>,
}

impl GeyserStream {
    /// Create a new Geyser stream and start listening for obligation updates
    pub async fn connect(config: GeyserConfig) -> Result<Self> {
        let (tx, rx) = mpsc::channel(1000);

        let config_clone = config.clone();
        let task_handle = tokio::spawn(async move {
            run_stream_with_reconnect(config_clone, tx).await;
        });

        Ok(Self {
            config,
            rx,
            _task_handle: task_handle,
        })
    }

    /// Receive the next obligation update
    /// Returns None if the stream is closed
    pub async fn recv(&mut self) -> Option<ObligationUpdate> {
        self.rx.recv().await
    }

    /// Try to receive an update without blocking
    #[allow(dead_code)]
    pub fn try_recv(&mut self) -> Option<ObligationUpdate> {
        self.rx.try_recv().ok()
    }

    /// Get the program ID being monitored
    #[allow(dead_code)]
    pub fn program_id(&self) -> &Pubkey {
        &self.config.program_id
    }
}

/// Run the gRPC stream with automatic reconnection on failures
async fn run_stream_with_reconnect(config: GeyserConfig, tx: mpsc::Sender<ObligationUpdate>) {
    let backoff = ExponentialBackoff {
        initial_interval: Duration::from_millis(500),
        max_interval: Duration::from_secs(30),
        max_elapsed_time: None, // Never give up
        ..Default::default()
    };

    loop {
        info!("Connecting to Geyser stream at {}", config.endpoint);

        match connect_and_stream(&config, &tx).await {
            Ok(()) => {
                info!("Geyser stream ended gracefully, reconnecting...");
            }
            Err(e) => {
                error!("Geyser stream error: {:?}, reconnecting...", e);
            }
        }

        // Wait before reconnecting
        let wait_time = backoff.initial_interval;
        warn!("Waiting {:?} before reconnecting to Geyser...", wait_time);
        tokio::time::sleep(wait_time).await;
    }
}

/// Connect to the gRPC endpoint and stream updates
async fn connect_and_stream(
    config: &GeyserConfig,
    tx: &mpsc::Sender<ObligationUpdate>,
) -> Result<()> {
    // Connect to Helius LaserStream using the builder pattern
    let mut client = GeyserGrpcClient::build_from_shared(config.endpoint.clone())
        .map_err(|e| anyhow!("Failed to create client builder: {:?}", e))?
        .x_token(Some(config.api_key.clone()))
        .map_err(|e| anyhow!("Failed to set token: {:?}", e))?
        .connect()
        .await
        .map_err(|e| anyhow!("Failed to connect to Geyser: {:?}", e))?;

    info!("Connected to Geyser endpoint");

    // Build subscription request for Kamino obligation accounts
    // We subscribe by owner (the Kamino program) to get all obligation updates
    let mut accounts_filter = HashMap::new();
    accounts_filter.insert(
        "kamino_obligations".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![], // Empty = all accounts
            owner: vec![config.program_id.to_string()], // Filter by program owner
            filters: vec![], // No additional filters
        },
    );

    let request = SubscribeRequest {
        accounts: accounts_filter,
        slots: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
    };

    // Subscribe and get the stream
    let (mut _subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .map_err(|e| anyhow!("Failed to subscribe: {:?}", e))?;

    info!("Subscribed to Kamino obligation updates for program {}", config.program_id);

    // Process incoming messages
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if let Some(update_oneof) = msg.update_oneof {
                    if let Err(e) = process_update(update_oneof, tx).await {
                        warn!("Error processing message: {:?}", e);
                    }
                }
            }
            Err(e) => {
                error!("Stream error: {:?}", e);
                return Err(anyhow!("Stream error: {:?}", e));
            }
        }
    }

    Ok(())
}

/// Process a single update from the Geyser stream
async fn process_update(
    update: UpdateOneof,
    tx: &mpsc::Sender<ObligationUpdate>,
) -> Result<()> {
    match update {
        UpdateOneof::Account(account_update) => {
            let account = account_update
                .account
                .ok_or_else(|| anyhow!("Missing account in update"))?;

            // Parse the pubkey
            let pubkey_bytes: [u8; 32] = account
                .pubkey
                .try_into()
                .map_err(|_| anyhow!("Invalid pubkey length"))?;
            let pubkey = Pubkey::from(pubkey_bytes);

            // Check if this looks like an obligation account
            // Obligations have a specific discriminator and minimum size
            if account.data.len() >= 8 {
                let update = ObligationUpdate {
                    pubkey,
                    slot: account_update.slot,
                    data: account.data,
                };

                debug!(
                    "Received obligation update: {} at slot {}",
                    pubkey, update.slot
                );

                // Send to the channel (non-blocking, drop if full)
                if tx.try_send(update).is_err() {
                    warn!("Update channel full, dropping update for {}", pubkey);
                }
            }
        }
        UpdateOneof::Ping(_) => {
            debug!("Received ping from Geyser");
        }
        UpdateOneof::Pong(_) => {
            debug!("Received pong from Geyser");
        }
        UpdateOneof::Slot(slot_update) => {
            debug!("Slot update: {}", slot_update.slot);
        }
        _ => {
            // Ignore other update types
        }
    }

    Ok(())
}

/// Filter to identify Kamino obligation accounts by their discriminator
#[allow(dead_code)]
pub fn is_obligation_account(data: &[u8]) -> bool {
    // Kamino Obligation discriminator (first 8 bytes)
    // This is the Anchor discriminator for the Obligation account
    const OBLIGATION_DISCRIMINATOR: [u8; 8] = [168, 206, 141, 106, 88, 76, 172, 167];

    if data.len() < 8 {
        return false;
    }

    data[..8] == OBLIGATION_DISCRIMINATOR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_obligation_discriminator() {
        // Test with valid discriminator
        let valid_data = [168, 206, 141, 106, 88, 76, 172, 167, 0, 0, 0, 0];
        assert!(is_obligation_account(&valid_data));

        // Test with invalid discriminator
        let invalid_data = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(!is_obligation_account(&invalid_data));

        // Test with too short data
        let short_data = [168, 206, 141, 106];
        assert!(!is_obligation_account(&short_data));
    }
}
