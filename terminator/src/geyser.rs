//! Yellowstone gRPC integration for real-time obligation and oracle monitoring.
//!
//! This module provides streaming updates via Yellowstone gRPC (Geyser)
//! to detect obligation changes in real-time instead of polling.
//! It also streams oracle price updates (Pyth, Switchboard, Scope) to avoid RPC latency.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anchor_lang::prelude::Pubkey;
use anchor_lang::AccountDeserialize;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use anchor_client::solana_sdk::account::Account;
use kamino_lending::Reserve;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterSlots, subscribe_update::UpdateOneof,
};

/// Type of account update received from Geyser, used for per-type counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountType {
    Oracle,
    Reserve,
    Obligation,
}

/// When a slot notification arrived and whether we've already sampled its first account delta.
struct SlotTiming {
    received_at: Instant,
    first_account_seen: bool,
}

/// Running min/max/sum/count for a latency window (milliseconds).
struct LatencyStats {
    min_ms: f64,
    max_ms: f64,
    sum_ms: f64,
    count: u64,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            min_ms: f64::MAX,
            max_ms: 0.0,
            sum_ms: 0.0,
            count: 0,
        }
    }

    fn record(&mut self, ms: f64) {
        if ms < self.min_ms {
            self.min_ms = ms;
        }
        if ms > self.max_ms {
            self.max_ms = ms;
        }
        self.sum_ms += ms;
        self.count += 1;
    }

    fn avg_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_ms / self.count as f64
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

/// Inner state of the latency tracker, protected by `RwLock`.
struct SlotLatencyTrackerInner {
    /// When each slot notification arrived (pruned periodically).
    slot_timings: HashMap<u64, SlotTiming>,
    /// Highest slot we've seen from Geyser.
    highest_geyser_slot: u64,
    /// Running latency stats for the current logging window.
    slot_to_account_stats: LatencyStats,
    /// Per-account-type update counts for the current window.
    per_type_counts: HashMap<AccountType, u64>,
    /// Last time we logged a summary.
    last_summary: Instant,
}

/// Measures the delay between Geyser slot notifications and the first account update for that
/// slot. Thread-safe; cloneable via inner `Arc`.
#[derive(Clone)]
pub struct SlotLatencyTracker {
    inner: Arc<RwLock<SlotLatencyTrackerInner>>,
}

impl SlotLatencyTracker {
    const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);
    /// Prune slot timings older than this to avoid unbounded memory growth.
    const MAX_SLOT_AGE: Duration = Duration::from_secs(120);

    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SlotLatencyTrackerInner {
                slot_timings: HashMap::new(),
                highest_geyser_slot: 0,
                slot_to_account_stats: LatencyStats::new(),
                per_type_counts: HashMap::new(),
                last_summary: Instant::now(),
            })),
        }
    }

    /// Record arrival of a Slot(Processed) notification.
    pub fn record_slot(&self, slot: u64) {
        let mut inner = self.inner.write().unwrap();
        if slot > inner.highest_geyser_slot {
            inner.highest_geyser_slot = slot;
        }
        inner.slot_timings.entry(slot).or_insert(SlotTiming {
            received_at: Instant::now(),
            first_account_seen: false,
        });
    }

    /// Record arrival of an account update for a given slot and type.
    /// If this is the first account update after the slot notification, record the delta.
    pub fn record_account_update(&self, slot: u64, account_type: AccountType) {
        let now = Instant::now();
        let mut inner = self.inner.write().unwrap();

        *inner.per_type_counts.entry(account_type).or_insert(0) += 1;

        if let Some(timing) = inner.slot_timings.get_mut(&slot) {
            if !timing.first_account_seen {
                timing.first_account_seen = true;
                let delta_ms = now.duration_since(timing.received_at).as_secs_f64() * 1000.0;
                inner.slot_to_account_stats.record(delta_ms);
            }
        }
        // If the slot hasn't been seen yet (account arrived before slot notification), we just
        // skip the delta sample — no panic.
    }

    /// Return the highest slot seen from Geyser.
    pub fn highest_slot(&self) -> u64 {
        self.inner.read().unwrap().highest_geyser_slot
    }

    /// Log a summary if the interval has elapsed, then reset the window.
    pub fn maybe_log_summary(&self) {
        let mut inner = self.inner.write().unwrap();
        if inner.last_summary.elapsed() < Self::SUMMARY_INTERVAL {
            return;
        }

        let stats = &inner.slot_to_account_stats;
        let oracle = inner.per_type_counts.get(&AccountType::Oracle).copied().unwrap_or(0);
        let reserve = inner.per_type_counts.get(&AccountType::Reserve).copied().unwrap_or(0);
        let obligation = inner.per_type_counts.get(&AccountType::Obligation).copied().unwrap_or(0);

        if stats.count > 0 {
            info!(
                "Geyser latency: slot→account avg={:.1}ms min={:.1}ms max={:.1}ms samples={} | \
                 updates: oracle={} reserve={} obligation={} | highest_slot={} slot_map_size={}",
                stats.avg_ms(),
                stats.min_ms,
                stats.max_ms,
                stats.count,
                oracle,
                reserve,
                obligation,
                inner.highest_geyser_slot,
                inner.slot_timings.len(),
            );
        } else {
            info!(
                "Geyser latency: no slot→account samples this window | \
                 updates: oracle={} reserve={} obligation={} | highest_slot={} slot_map_size={}",
                oracle,
                reserve,
                obligation,
                inner.highest_geyser_slot,
                inner.slot_timings.len(),
            );
        }

        // Reset window
        inner.slot_to_account_stats.reset();
        inner.per_type_counts.clear();
        inner.last_summary = Instant::now();

        // Prune old slot timings
        let cutoff = Instant::now() - Self::MAX_SLOT_AGE;
        inner.slot_timings.retain(|_, v| v.received_at > cutoff);
    }
}

/// Configuration for connecting to Yellowstone gRPC
#[derive(Debug, Clone)]
pub struct GeyserConfig {
    /// Yellowstone gRPC endpoint URL
    pub endpoint: String,
    /// API key for authentication (x-token)
    pub api_key: String,
    /// Kamino lending program ID to monitor
    pub program_id: Pubkey,
    /// Oracle account pubkeys to stream (Pyth, Switchboard, Scope)
    pub oracle_accounts: HashSet<Pubkey>,
    /// Reserve account pubkeys to stream for real-time reserve data
    pub reserve_accounts: HashSet<Pubkey>,
}

/// Thread-safe cache for oracle account data
/// Updated in real-time via Geyser stream
#[derive(Debug, Clone)]
pub struct OracleCache {
    /// Map from oracle pubkey to (slot, account_data)
    inner: Arc<RwLock<HashMap<Pubkey, (u64, Vec<u8>)>>>,
}

impl OracleCache {
    /// Create a new empty oracle cache
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize cache with existing oracle data from MarketState
    pub fn init_from_market_state(
        &self,
        pyth: &HashMap<Pubkey, Vec<u8>>,
        switchboard: &HashMap<Pubkey, Vec<u8>>,
        scope: &HashMap<Pubkey, Vec<u8>>,
    ) {
        let mut cache = self.inner.write().unwrap();
        for (k, v) in pyth.iter() {
            cache.insert(*k, (0, v.clone()));
        }
        for (k, v) in switchboard.iter() {
            cache.insert(*k, (0, v.clone()));
        }
        for (k, v) in scope.iter() {
            cache.insert(*k, (0, v.clone()));
        }
        info!("Oracle cache initialized with {} accounts", cache.len());
    }

    /// Update an oracle account in the cache
    pub fn update(&self, pubkey: Pubkey, slot: u64, data: Vec<u8>) {
        let mut cache = self.inner.write().unwrap();
        // Only update if slot is newer (or first update)
        if let Some((existing_slot, _)) = cache.get(&pubkey) {
            if slot <= *existing_slot {
                return; // Ignore stale updates
            }
        }
        cache.insert(pubkey, (slot, data));
    }

    /// Get oracle account data
    pub fn get(&self, pubkey: &Pubkey) -> Option<Vec<u8>> {
        self.inner.read().unwrap().get(pubkey).map(|(_, data)| data.clone())
    }

    /// Get oracle account data as Account struct (for compatibility with existing code)
    pub fn get_account(&self, pubkey: &Pubkey) -> Option<Account> {
        self.inner.read().unwrap().get(pubkey).map(|(_, data)| Account {
            lamports: 0,
            data: data.clone(),
            owner: Pubkey::default(),
            executable: false,
            rent_epoch: 0,
        })
    }

    /// Get multiple oracle accounts
    pub fn get_multiple(&self, pubkeys: &[Pubkey]) -> Vec<Option<Account>> {
        let cache = self.inner.read().unwrap();
        pubkeys
            .iter()
            .map(|pk| {
                cache.get(pk).map(|(_, data)| Account {
                    lamports: 0,
                    data: data.clone(),
                    owner: Pubkey::default(),
                    executable: false,
                    rent_epoch: 0,
                })
            })
            .collect()
    }

    /// Check if a pubkey is in the cache
    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.inner.read().unwrap().contains_key(pubkey)
    }

    /// Get the number of cached oracles
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }
}

impl Default for OracleCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe cache for reserve account data
/// Updated in real-time via Geyser stream, deserialized on write for instant reads
#[derive(Debug, Clone)]
pub struct ReserveCache {
    /// Map from reserve pubkey to (slot, deserialized_reserve)
    inner: Arc<RwLock<HashMap<Pubkey, (u64, Reserve)>>>,
}

impl ReserveCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize from existing market state reserves
    pub fn init_from_reserves(&self, reserves: &HashMap<Pubkey, Reserve>) {
        let mut cache = self.inner.write().unwrap();
        for (pubkey, reserve) in reserves {
            cache.insert(*pubkey, (0, *reserve));
        }
        info!("Reserve cache initialized with {} accounts", cache.len());
    }

    /// Update a reserve account in the cache (deserializes raw data on write)
    pub fn update(&self, pubkey: Pubkey, slot: u64, data: Vec<u8>) {
        // Deserialize on the Geyser update path (off the critical liquidation path)
        match Reserve::try_deserialize(&mut data.as_slice()) {
            Ok(reserve) => {
                let mut cache = self.inner.write().unwrap();
                if let Some((existing_slot, _)) = cache.get(&pubkey) {
                    if slot <= *existing_slot {
                        return; // Ignore stale updates
                    }
                }
                cache.insert(pubkey, (slot, reserve));
            }
            Err(e) => {
                debug!("Failed to deserialize reserve {}: {:?}", pubkey, e);
            }
        }
    }

    /// Get a single cached reserve
    pub fn get_reserve(&self, pubkey: &Pubkey) -> Option<Reserve> {
        self.inner.read().unwrap().get(pubkey).map(|(_, r)| *r)
    }

    /// Get multiple reserves, returning a map of those found in cache
    pub fn get_reserves(&self, pubkeys: &[Pubkey]) -> HashMap<Pubkey, Reserve> {
        let cache = self.inner.read().unwrap();
        let mut result = HashMap::new();
        for pk in pubkeys {
            if let Some((_, reserve)) = cache.get(pk) {
                result.insert(*pk, *reserve);
            }
        }
        result
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    #[allow(dead_code)]
    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.inner.read().unwrap().contains_key(pubkey)
    }
}

impl Default for ReserveCache {
    fn default() -> Self {
        Self::new()
    }
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
    /// Wall-clock time when this update was received from the Geyser stream
    pub received_at: std::time::Instant,
}

/// Geyser stream handle for receiving obligation updates
pub struct GeyserStream {
    config: GeyserConfig,
    /// Channel to receive obligation updates
    rx: mpsc::Receiver<ObligationUpdate>,
    /// Handle to the background streaming task
    _task_handle: tokio::task::JoinHandle<()>,
    /// Shared oracle cache updated by the stream
    oracle_cache: OracleCache,
    /// Shared reserve cache updated by the stream
    reserve_cache: ReserveCache,
    /// Latency tracker for slot→account delta measurement
    latency_tracker: SlotLatencyTracker,
}

impl GeyserStream {
    /// Create a new Geyser stream and start listening for obligation updates
    pub async fn connect(config: GeyserConfig) -> Result<Self> {
        let (tx, rx) = mpsc::channel(1000);
        let oracle_cache = OracleCache::new();
        let reserve_cache = ReserveCache::new();
        let latency_tracker = SlotLatencyTracker::new();

        let config_clone = config.clone();
        let oracle_cache_clone = oracle_cache.clone();
        let reserve_cache_clone = reserve_cache.clone();
        let latency_tracker_clone = latency_tracker.clone();
        let task_handle = tokio::spawn(async move {
            run_stream_with_reconnect(config_clone, tx, oracle_cache_clone, reserve_cache_clone, latency_tracker_clone).await;
        });

        Ok(Self {
            config,
            rx,
            _task_handle: task_handle,
            oracle_cache,
            reserve_cache,
            latency_tracker,
        })
    }

    /// Get a reference to the oracle cache
    pub fn oracle_cache(&self) -> &OracleCache {
        &self.oracle_cache
    }

    /// Get a reference to the reserve cache
    pub fn reserve_cache(&self) -> &ReserveCache {
        &self.reserve_cache
    }

    /// Get a reference to the latency tracker
    pub fn latency_tracker(&self) -> &SlotLatencyTracker {
        &self.latency_tracker
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
async fn run_stream_with_reconnect(
    config: GeyserConfig,
    tx: mpsc::Sender<ObligationUpdate>,
    oracle_cache: OracleCache,
    reserve_cache: ReserveCache,
    latency_tracker: SlotLatencyTracker,
) {
    let mut consecutive_failures: u32 = 0;
    const MAX_BACKOFF_SECS: u64 = 60;
    const INITIAL_BACKOFF_MS: u64 = 500;

    loop {
        info!("Connecting to Geyser stream at {} (attempt {})", config.endpoint, consecutive_failures + 1);

        match connect_and_stream(&config, &tx, &oracle_cache, &reserve_cache, &latency_tracker).await {
            Ok(()) => {
                // Stream ended gracefully (server closed connection)
                warn!("Geyser stream ended gracefully, will reconnect...");
                consecutive_failures = 0; // Reset on graceful close
            }
            Err(e) => {
                consecutive_failures += 1;
                error!(
                    "Geyser stream error (failure #{}): {:?}",
                    consecutive_failures, e
                );
            }
        }

        // Calculate exponential backoff: 500ms, 1s, 2s, 4s, 8s, ... up to 60s
        let backoff_ms = INITIAL_BACKOFF_MS * 2u64.saturating_pow(consecutive_failures.min(10));
        let wait_time = Duration::from_millis(backoff_ms.min(MAX_BACKOFF_SECS * 1000));

        warn!(
            "Waiting {:?} before reconnecting to Geyser (failure count: {})...",
            wait_time, consecutive_failures
        );
        tokio::time::sleep(wait_time).await;
    }
}

/// Connect to the gRPC endpoint and stream updates
async fn connect_and_stream(
    config: &GeyserConfig,
    tx: &mpsc::Sender<ObligationUpdate>,
    oracle_cache: &OracleCache,
    reserve_cache: &ReserveCache,
    latency_tracker: &SlotLatencyTracker,
) -> Result<()> {
    // Connect to Yellowstone gRPC endpoint using the builder pattern with timeout
    let connect_timeout = Duration::from_secs(30);

    info!("Attempting to connect to Geyser endpoint...");

    let connect_future = async {
        GeyserGrpcClient::build_from_shared(config.endpoint.clone())
            .map_err(|e| anyhow!("Failed to create client builder: {:?}", e))?
            .x_token(Some(config.api_key.clone()))
            .map_err(|e| anyhow!("Failed to set token: {:?}", e))?
            .connect_timeout(connect_timeout)
            .timeout(Duration::from_secs(60)) // Request timeout
            .connect()
            .await
            .map_err(|e| anyhow!("Failed to connect to Geyser: {:?}", e))
    };

    let mut client = tokio::time::timeout(connect_timeout, connect_future)
        .await
        .map_err(|_| anyhow!("Connection to Geyser timed out after {:?}", connect_timeout))??;

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

    // Also subscribe to oracle accounts (Pyth, Switchboard, Scope) by specific pubkey
    if !config.oracle_accounts.is_empty() {
        let oracle_pubkeys: Vec<String> = config
            .oracle_accounts
            .iter()
            .map(|pk| pk.to_string())
            .collect();
        info!("Subscribing to {} oracle accounts", oracle_pubkeys.len());
        accounts_filter.insert(
            "oracle_accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: oracle_pubkeys,
                owner: vec![],
                filters: vec![],
            },
        );
    }

    // Subscribe to reserve accounts for real-time reserve data (eliminates RPC fetch in liquidate_fast)
    if !config.reserve_accounts.is_empty() {
        let reserve_pubkeys: Vec<String> = config
            .reserve_accounts
            .iter()
            .map(|pk| pk.to_string())
            .collect();
        info!("Subscribing to {} reserve accounts", reserve_pubkeys.len());
        accounts_filter.insert(
            "reserve_accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: reserve_pubkeys,
                owner: vec![],
                filters: vec![],
            },
        );
    }

    let mut slots_filter = HashMap::new();
    slots_filter.insert(
        "slot_updates".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
        },
    );

    let request = SubscribeRequest {
        accounts: accounts_filter,
        slots: slots_filter,
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

    info!(
        "Subscribed to Kamino obligations (program {}), {} oracle accounts, {} reserve accounts",
        config.program_id,
        config.oracle_accounts.len(),
        config.reserve_accounts.len()
    );

    // Process incoming messages with a receive timeout
    // If we don't receive any message (including pings) for this long, consider connection stale
    let receive_timeout = Duration::from_secs(120); // 2 minutes
    let mut last_message_time = std::time::Instant::now();
    let mut messages_received: u64 = 0;

    loop {
        // Use a shorter timeout for the select to check staleness periodically
        let check_interval = Duration::from_secs(10);

        match tokio::time::timeout(check_interval, stream.next()).await {
            Ok(Some(message)) => {
                last_message_time = std::time::Instant::now();
                messages_received += 1;

                match message {
                    Ok(msg) => {
                        if let Some(update_oneof) = msg.update_oneof {
                            if let Err(e) = process_update(update_oneof, tx, oracle_cache, reserve_cache, &config.oracle_accounts, &config.reserve_accounts, latency_tracker).await {
                                warn!("Error processing message: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Stream error after {} messages: {:?}", messages_received, e);
                        return Err(anyhow!("Stream error: {:?}", e));
                    }
                }
            }
            Ok(None) => {
                // Stream ended (server closed)
                info!("Geyser stream closed by server after {} messages", messages_received);
                return Ok(());
            }
            Err(_) => {
                // Timeout - check if connection is stale
                let elapsed = last_message_time.elapsed();
                if elapsed > receive_timeout {
                    error!(
                        "No messages received for {:?} (last message: {:?} ago), connection appears stale",
                        receive_timeout, elapsed
                    );
                    return Err(anyhow!("Connection stale - no messages for {:?}", elapsed));
                }
                // Otherwise just continue waiting
                debug!("No message in {:?}, still within timeout (last: {:?} ago)", check_interval, elapsed);
            }
        }
    }
}

/// Process a single update from the Geyser stream
async fn process_update(
    update: UpdateOneof,
    tx: &mpsc::Sender<ObligationUpdate>,
    oracle_cache: &OracleCache,
    reserve_cache: &ReserveCache,
    oracle_pubkeys: &HashSet<Pubkey>,
    reserve_pubkeys: &HashSet<Pubkey>,
    latency_tracker: &SlotLatencyTracker,
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
            let slot = account_update.slot;

            // Check if this is an oracle account update
            if oracle_pubkeys.contains(&pubkey) {
                oracle_cache.update(pubkey, slot, account.data);
                latency_tracker.record_account_update(slot, AccountType::Oracle);
                debug!("Updated oracle cache: {} at slot {}", pubkey, slot);
                return Ok(());
            }

            // Check if this is a reserve account update
            if reserve_pubkeys.contains(&pubkey) {
                reserve_cache.update(pubkey, slot, account.data);
                latency_tracker.record_account_update(slot, AccountType::Reserve);
                debug!("Updated reserve cache: {} at slot {}", pubkey, slot);
                return Ok(());
            }

            // Otherwise, check if this looks like an obligation account
            // Obligations have a specific discriminator and minimum size
            if account.data.len() >= 8 {
                latency_tracker.record_account_update(slot, AccountType::Obligation);

                let update = ObligationUpdate {
                    pubkey,
                    slot,
                    data: account.data,
                    received_at: std::time::Instant::now(),
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
            // CommitmentLevel::Processed == 0
            if slot_update.status == CommitmentLevel::Processed as i32 {
                latency_tracker.record_slot(slot_update.slot);
            }
            debug!("Slot update: {} (status={})", slot_update.slot, slot_update.status);
        }
        _ => {
            // Ignore other update types
        }
    }

    latency_tracker.maybe_log_summary();

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

    #[test]
    fn test_latency_stats_basic() {
        let mut stats = LatencyStats::new();
        stats.record(10.0);
        stats.record(20.0);
        stats.record(30.0);
        assert_eq!(stats.count, 3);
        assert!((stats.avg_ms() - 20.0).abs() < 0.001);
        assert!((stats.min_ms - 10.0).abs() < 0.001);
        assert!((stats.max_ms - 30.0).abs() < 0.001);
    }

    #[test]
    fn test_latency_stats_reset() {
        let mut stats = LatencyStats::new();
        stats.record(10.0);
        stats.record(20.0);
        stats.reset();
        assert_eq!(stats.count, 0);
        assert!((stats.avg_ms() - 0.0).abs() < 0.001);
        assert_eq!(stats.max_ms, 0.0);
        assert_eq!(stats.min_ms, f64::MAX);
    }

    #[test]
    fn test_tracker_record_slot_then_account() {
        let tracker = SlotLatencyTracker::new();
        tracker.record_slot(100);
        // Small sleep to ensure a measurable delta
        std::thread::sleep(Duration::from_millis(1));
        tracker.record_account_update(100, AccountType::Oracle);

        let inner = tracker.inner.read().unwrap();
        assert!(inner.slot_to_account_stats.count == 1);
        assert!(inner.slot_to_account_stats.min_ms > 0.0);
    }

    #[test]
    fn test_tracker_only_first_account_per_slot() {
        let tracker = SlotLatencyTracker::new();
        tracker.record_slot(100);
        tracker.record_account_update(100, AccountType::Oracle);
        tracker.record_account_update(100, AccountType::Reserve);
        tracker.record_account_update(100, AccountType::Obligation);

        let inner = tracker.inner.read().unwrap();
        // Only the first account update should produce a delta sample
        assert_eq!(inner.slot_to_account_stats.count, 1);
        // But all three types should be counted
        assert_eq!(*inner.per_type_counts.get(&AccountType::Oracle).unwrap(), 1);
        assert_eq!(*inner.per_type_counts.get(&AccountType::Reserve).unwrap(), 1);
        assert_eq!(*inner.per_type_counts.get(&AccountType::Obligation).unwrap(), 1);
    }

    #[test]
    fn test_tracker_account_before_slot_is_graceful() {
        // Account arrives before slot notification — should not panic
        let tracker = SlotLatencyTracker::new();
        tracker.record_account_update(200, AccountType::Oracle);

        let inner = tracker.inner.read().unwrap();
        // No delta recorded because we haven't seen the slot yet
        assert_eq!(inner.slot_to_account_stats.count, 0);
        // But the type count is still tracked
        assert_eq!(*inner.per_type_counts.get(&AccountType::Oracle).unwrap(), 1);
    }

    #[test]
    fn test_tracker_highest_slot_monotonic() {
        let tracker = SlotLatencyTracker::new();
        tracker.record_slot(100);
        assert_eq!(tracker.highest_slot(), 100);
        tracker.record_slot(200);
        assert_eq!(tracker.highest_slot(), 200);
        // Out-of-order slot should not decrease highest
        tracker.record_slot(150);
        assert_eq!(tracker.highest_slot(), 200);
    }
}
