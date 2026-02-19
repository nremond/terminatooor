//! ShredStream integration for same-block liquidation via Jito bundles.
//!
//! Connects to a local `jito-shredstream-proxy` gRPC endpoint that delivers
//! decoded entries (transactions) from shreds *before* slot confirmation.
//! When an oracle price update is detected, obligations near the liquidation
//! threshold are triggered ~400ms earlier than the Geyser-based path.
//!
//! The Geyser path continues unchanged as a fallback. Both paths feed into the
//! same `LiquidationOrchestrator` for deduplication.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anchor_lang::prelude::Pubkey;
use kamino_lending::{Obligation, Reserve};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::shredstream_proto::{
    shredstream_proxy_client::ShredstreamProxyClient, SolanaEntry, SubscribeEntriesRequest,
};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the ShredStream gRPC connection.
#[derive(Debug, Clone)]
pub struct ShredstreamConfig {
    /// gRPC URL for the local ShredStream proxy (e.g., `http://127.0.0.1:9999`).
    pub grpc_url: String,
    /// Whether ShredStream is enabled.
    pub enabled: bool,
    /// LTV threshold percentage (of unhealthy_ltv) for watchlist inclusion.
    /// Default: 95 (i.e., obligations at >= 95% of their unhealthy LTV are watched).
    pub ltv_threshold_pct: f64,
}

impl ShredstreamConfig {
    /// Read configuration from environment variables.
    /// ShredStream is enabled when `SHREDSTREAM_GRPC_URL` is set and non-empty.
    pub fn from_env() -> Self {
        let grpc_url = std::env::var("SHREDSTREAM_GRPC_URL").unwrap_or_default();
        let enabled = !grpc_url.is_empty();
        let ltv_threshold_pct: f64 = std::env::var("SHREDSTREAM_LTV_THRESHOLD_PCT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(95.0);

        Self {
            grpc_url,
            enabled,
            ltv_threshold_pct,
        }
    }
}

// ============================================================================
// Near-Threshold Watchlist
// ============================================================================

/// Entry in the near-threshold watchlist.
#[derive(Clone, Debug)]
pub struct WatchlistEntry {
    pub obligation: Obligation,
    pub market_pubkey: Pubkey,
    /// Current LTV as a percentage of unhealthy_ltv (for logging).
    pub ltv_pct: f64,
    /// Oracle pubkeys that, if updated, could push this obligation over threshold.
    pub relevant_oracles: HashSet<Pubkey>,
}

struct WatchlistInner {
    /// obligation_pubkey → entry
    entries: HashMap<Pubkey, WatchlistEntry>,
    /// oracle_pubkey → set of obligation_pubkeys that depend on this oracle
    oracle_to_obligations: HashMap<Pubkey, HashSet<Pubkey>>,
}

/// Thread-safe watchlist of obligations near the liquidation threshold.
/// Updated by the Geyser evaluation path, queried by the ShredStream path.
#[derive(Clone)]
pub struct NearThresholdWatchlist {
    inner: Arc<RwLock<WatchlistInner>>,
    /// LTV threshold (ratio of unhealthy_ltv) for inclusion.
    threshold_ratio: f64,
}

impl NearThresholdWatchlist {
    pub fn new(ltv_threshold_pct: f64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(WatchlistInner {
                entries: HashMap::new(),
                oracle_to_obligations: HashMap::new(),
            })),
            threshold_ratio: ltv_threshold_pct / 100.0,
        }
    }

    /// Update the watchlist for an obligation based on its current LTV.
    ///
    /// If the obligation's LTV ratio (ltv / unhealthy_ltv) >= threshold_ratio,
    /// it is added/updated in the watchlist. Otherwise it is removed.
    ///
    /// `reserves` is the full reserves map for the market, used to extract
    /// oracle pubkeys from the obligation's deposit/borrow reserves.
    pub fn update(
        &self,
        obligation_pk: &Pubkey,
        obligation: &Obligation,
        market_pk: &Pubkey,
        ltv_f64: f64,
        unhealthy_ltv_f64: f64,
        reserves: &HashMap<Pubkey, Reserve>,
    ) {
        if unhealthy_ltv_f64 <= 0.0 {
            return;
        }

        let ratio = ltv_f64 / unhealthy_ltv_f64;

        let mut inner = self.inner.write().unwrap();

        if ratio >= self.threshold_ratio {
            // Compute relevant oracles from obligation's deposit/borrow reserves
            let relevant_oracles = oracles_for_obligation(obligation, reserves);

            // Remove old reverse-index entries if updating
            let old_oracles: Vec<Pubkey> = inner
                .entries
                .get(obligation_pk)
                .map(|e| e.relevant_oracles.iter().copied().collect())
                .unwrap_or_default();
            for oracle in &old_oracles {
                if let Some(set) = inner.oracle_to_obligations.get_mut(oracle) {
                    set.remove(obligation_pk);
                    if set.is_empty() {
                        inner.oracle_to_obligations.remove(oracle);
                    }
                }
            }

            // Insert reverse-index entries
            for oracle in &relevant_oracles {
                inner
                    .oracle_to_obligations
                    .entry(*oracle)
                    .or_default()
                    .insert(*obligation_pk);
            }

            inner.entries.insert(
                *obligation_pk,
                WatchlistEntry {
                    obligation: *obligation,
                    market_pubkey: *market_pk,
                    ltv_pct: ratio * 100.0,
                    relevant_oracles,
                },
            );
        } else {
            // Below threshold — remove if present
            self.remove_inner(&mut inner, obligation_pk);
        }
    }

    /// Remove an obligation from the watchlist.
    pub fn remove(&self, obligation_pk: &Pubkey) {
        let mut inner = self.inner.write().unwrap();
        self.remove_inner(&mut inner, obligation_pk);
    }

    fn remove_inner(&self, inner: &mut WatchlistInner, obligation_pk: &Pubkey) {
        if let Some(entry) = inner.entries.remove(obligation_pk) {
            for oracle in &entry.relevant_oracles {
                if let Some(set) = inner.oracle_to_obligations.get_mut(oracle) {
                    set.remove(obligation_pk);
                    if set.is_empty() {
                        inner.oracle_to_obligations.remove(oracle);
                    }
                }
            }
        }
    }

    /// Get all obligations affected by an oracle update.
    pub fn get_triggered_obligations(
        &self,
        oracle_pk: &Pubkey,
    ) -> Vec<(Pubkey, WatchlistEntry)> {
        let inner = self.inner.read().unwrap();
        let Some(obligation_set) = inner.oracle_to_obligations.get(oracle_pk) else {
            return Vec::new();
        };
        obligation_set
            .iter()
            .filter_map(|ob_pk| inner.entries.get(ob_pk).map(|e| (*ob_pk, e.clone())))
            .collect()
    }

    /// Number of obligations in the watchlist.
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().entries.len()
    }

    /// Number of distinct oracles being monitored.
    pub fn oracle_count(&self) -> usize {
        self.inner.read().unwrap().oracle_to_obligations.len()
    }
}

// ============================================================================
// ShredStream Trigger (channel message)
// ============================================================================

/// Message sent from the ShredStream task to the main loop when an oracle
/// update is detected in shreds for a near-threshold obligation.
#[derive(Debug, Clone)]
pub struct ShredstreamTrigger {
    pub obligation_pubkey: Pubkey,
    pub obligation: Obligation,
    pub market_pubkey: Pubkey,
    pub source_slot: u64,
    pub ltv_pct: f64,
}

// ============================================================================
// ShredStream stats (for periodic logging)
// ============================================================================

struct ShredstreamStats {
    entries_received: u64,
    oracle_triggers: u64,
    liquidations_attempted: u64,
    last_log: Instant,
}

impl ShredstreamStats {
    fn new() -> Self {
        Self {
            entries_received: 0,
            oracle_triggers: 0,
            liquidations_attempted: 0,
            last_log: Instant::now(),
        }
    }

    fn maybe_log(&mut self, watchlist: &NearThresholdWatchlist) {
        if self.last_log.elapsed() < Duration::from_secs(60) {
            return;
        }
        info!(
            "ShredStream watchlist: {} obligations near threshold, {} oracles monitored | \
             entries_received={} oracle_triggers={} liquidations_attempted={}",
            watchlist.len(),
            watchlist.oracle_count(),
            self.entries_received,
            self.oracle_triggers,
            self.liquidations_attempted,
        );
        self.entries_received = 0;
        self.oracle_triggers = 0;
        self.liquidations_attempted = 0;
        self.last_log = Instant::now();
    }
}

// ============================================================================
// Main ShredStream loop
// ============================================================================

/// Run the ShredStream client with automatic reconnection.
///
/// Connects to the local ShredStream proxy gRPC, subscribes to entries,
/// and scans transactions for oracle account updates. When a monitored oracle
/// is touched, triggers liquidation checks for near-threshold obligations.
pub async fn run_shredstream(
    config: ShredstreamConfig,
    watchlist: NearThresholdWatchlist,
    oracle_accounts: HashSet<Pubkey>,
    trigger_tx: mpsc::Sender<ShredstreamTrigger>,
) {
    let mut consecutive_failures: u32 = 0;
    const MAX_BACKOFF_SECS: u64 = 60;
    const INITIAL_BACKOFF_MS: u64 = 500;

    loop {
        info!(
            "ShredStream connecting to {} (attempt {})",
            config.grpc_url,
            consecutive_failures + 1
        );

        match connect_and_stream(
            &config,
            &watchlist,
            &oracle_accounts,
            &trigger_tx,
        )
        .await
        {
            Ok(()) => {
                warn!("ShredStream stream ended gracefully, will reconnect...");
                consecutive_failures = 0;
            }
            Err(e) => {
                consecutive_failures += 1;
                error!(
                    "ShredStream error (failure #{}): {:?}",
                    consecutive_failures, e
                );
            }
        }

        let backoff_ms = INITIAL_BACKOFF_MS * 2u64.saturating_pow(consecutive_failures.min(10));
        let wait_time = Duration::from_millis(backoff_ms.min(MAX_BACKOFF_SECS * 1000));
        warn!(
            "ShredStream waiting {:?} before reconnecting (failure count: {})...",
            wait_time, consecutive_failures
        );
        tokio::time::sleep(wait_time).await;
    }
}

/// Connect to the ShredStream proxy and process entries.
async fn connect_and_stream(
    config: &ShredstreamConfig,
    watchlist: &NearThresholdWatchlist,
    oracle_accounts: &HashSet<Pubkey>,
    trigger_tx: &mpsc::Sender<ShredstreamTrigger>,
) -> anyhow::Result<()> {
    let mut client = ShredstreamProxyClient::connect(config.grpc_url.clone()).await?;
    info!("ShredStream connected to {}", config.grpc_url);

    let response = client
        .subscribe_entries(SubscribeEntriesRequest {})
        .await?;
    let mut stream = response.into_inner();

    let mut stats = ShredstreamStats::new();

    loop {
        use tokio_stream::StreamExt;

        let msg = match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(entry_msg))) => entry_msg,
            Ok(Some(Err(e))) => {
                return Err(anyhow::anyhow!("ShredStream stream error: {:?}", e));
            }
            Ok(None) => {
                return Ok(()); // Stream ended gracefully
            }
            Err(_) => {
                // Timeout — keep going (proxy may be idle if no shreds)
                stats.maybe_log(watchlist);
                continue;
            }
        };

        stats.entries_received += 1;
        let slot = msg.slot;

        // Deserialize the entries batch
        let entries: Vec<SolanaEntry> = match bincode::deserialize(&msg.entries) {
            Ok(e) => e,
            Err(e) => {
                debug!("ShredStream: failed to deserialize entries for slot {}: {:?}", slot, e);
                continue;
            }
        };

        // Scan transactions for oracle account keys
        for entry in &entries {
            for tx in &entry.transactions {
                let account_keys = tx.message.static_account_keys();
                for key in account_keys {
                    if oracle_accounts.contains(key) {
                        // Oracle update detected in shreds!
                        stats.oracle_triggers += 1;
                        on_oracle_detected(
                            slot,
                            key,
                            watchlist,
                            trigger_tx,
                            &mut stats,
                        )
                        .await;
                        // One oracle match per tx is enough to trigger
                        break;
                    }
                }
            }
        }

        stats.maybe_log(watchlist);
    }
}

/// Handle detection of an oracle update in shreds.
async fn on_oracle_detected(
    slot: u64,
    oracle_pk: &Pubkey,
    watchlist: &NearThresholdWatchlist,
    trigger_tx: &mpsc::Sender<ShredstreamTrigger>,
    stats: &mut ShredstreamStats,
) {
    let triggered = watchlist.get_triggered_obligations(oracle_pk);
    if triggered.is_empty() {
        return;
    }

    info!(
        "ShredStream: oracle {} updated in slot {}, triggering {} near-threshold obligations",
        oracle_pk,
        slot,
        triggered.len()
    );

    for (obligation_pk, entry) in triggered {
        stats.liquidations_attempted += 1;
        info!(
            "ShredStream-triggered liquidation for obligation {} (LTV={:.1}%, market={})",
            obligation_pk, entry.ltv_pct, entry.market_pubkey
        );

        let trigger = ShredstreamTrigger {
            obligation_pubkey: obligation_pk,
            obligation: entry.obligation,
            market_pubkey: entry.market_pubkey,
            source_slot: slot,
            ltv_pct: entry.ltv_pct,
        };

        // Non-blocking send — drop if the main loop channel is full
        if trigger_tx.try_send(trigger).is_err() {
            warn!(
                "ShredStream trigger channel full, dropping trigger for {}",
                obligation_pk
            );
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract all oracle pubkeys relevant to an obligation's deposit/borrow reserves.
fn oracles_for_obligation(
    obligation: &Obligation,
    reserves: &HashMap<Pubkey, Reserve>,
) -> HashSet<Pubkey> {
    let mut oracles = HashSet::new();

    let reserve_keys: Vec<Pubkey> = obligation
        .deposits
        .iter()
        .filter(|d| d.deposit_reserve != Pubkey::default())
        .map(|d| d.deposit_reserve)
        .chain(
            obligation
                .borrows
                .iter()
                .filter(|b| b.borrow_reserve != Pubkey::default())
                .map(|b| b.borrow_reserve),
        )
        .collect();

    for key in reserve_keys {
        if let Some(reserve) = reserves.get(&key) {
            let ti = &reserve.config.token_info;
            let pyth = ti.pyth_configuration.price;
            let sb_price = ti.switchboard_configuration.price_aggregator;
            let sb_twap = ti.switchboard_configuration.twap_aggregator;
            let scope = ti.scope_configuration.price_feed;

            if pyth != Pubkey::default() {
                oracles.insert(pyth);
            }
            if sb_price != Pubkey::default() {
                oracles.insert(sb_price);
            }
            if sb_twap != Pubkey::default() {
                oracles.insert(sb_twap);
            }
            if scope != Pubkey::default() {
                oracles.insert(scope);
            }
        }
    }

    oracles
}

/// Build a map from oracle pubkey → reserve pubkeys that use that oracle.
/// Useful for understanding which reserves are affected by an oracle update.
#[allow(dead_code)]
pub fn build_oracle_to_reserve_map(
    reserves: &HashMap<Pubkey, Reserve>,
) -> HashMap<Pubkey, Vec<Pubkey>> {
    let mut map: HashMap<Pubkey, Vec<Pubkey>> = HashMap::new();

    for (reserve_pk, reserve) in reserves {
        let ti = &reserve.config.token_info;
        let oracle_pks = [
            ti.pyth_configuration.price,
            ti.switchboard_configuration.price_aggregator,
            ti.switchboard_configuration.twap_aggregator,
            ti.scope_configuration.price_feed,
        ];
        for oracle_pk in oracle_pks {
            if oracle_pk != Pubkey::default() {
                map.entry(oracle_pk).or_default().push(*reserve_pk);
            }
        }
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watchlist_add_remove() {
        let watchlist = NearThresholdWatchlist::new(95.0);
        assert_eq!(watchlist.len(), 0);

        let ob_pk = Pubkey::new_unique();
        let market_pk = Pubkey::new_unique();
        let obligation = Obligation::default();
        let reserves = HashMap::new();

        // LTV at 96% of unhealthy — should be added (>= 95%)
        watchlist.update(&ob_pk, &obligation, &market_pk, 0.96, 1.0, &reserves);
        assert_eq!(watchlist.len(), 1);

        // LTV at 90% of unhealthy — should be removed (< 95%)
        watchlist.update(&ob_pk, &obligation, &market_pk, 0.90, 1.0, &reserves);
        assert_eq!(watchlist.len(), 0);
    }

    #[test]
    fn test_watchlist_zero_unhealthy_ltv() {
        let watchlist = NearThresholdWatchlist::new(95.0);
        let ob_pk = Pubkey::new_unique();
        let market_pk = Pubkey::new_unique();
        let obligation = Obligation::default();
        let reserves = HashMap::new();

        // Should not panic or add with zero unhealthy_ltv
        watchlist.update(&ob_pk, &obligation, &market_pk, 0.5, 0.0, &reserves);
        assert_eq!(watchlist.len(), 0);
    }

    #[test]
    fn test_watchlist_oracle_reverse_index() {
        let watchlist = NearThresholdWatchlist::new(95.0);
        let oracle_pk = Pubkey::new_unique();

        // No obligations — should return empty
        let triggered = watchlist.get_triggered_obligations(&oracle_pk);
        assert!(triggered.is_empty());
    }

    #[test]
    fn test_config_from_env_disabled() {
        // When SHREDSTREAM_GRPC_URL is not set, should be disabled
        std::env::remove_var("SHREDSTREAM_GRPC_URL");
        let config = ShredstreamConfig::from_env();
        assert!(!config.enabled);
        assert!(config.grpc_url.is_empty());
        assert!((config.ltv_threshold_pct - 95.0).abs() < 0.01);
    }
}
