//! Parallel Liquidation Orchestrator
//!
//! This module provides parallel processing of liquidations with:
//! - Rate limiting via semaphore (configurable max concurrent)
//! - Deduplication to prevent double-attempts on same obligation
//! - Structured logging with unique task IDs for traceability
//! - Pure functions for testability
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
//! │  Geyser Stream  │────>│  LiquidationQueue    │────>│  Worker Tasks   │
//! │  (obligations)  │     │  (dedup + rate limit)│     │  (parallel)     │
//! └─────────────────┘     └──────────────────────┘     └─────────────────┘
//!                                   │
//!                                   v
//!                         ┌──────────────────────┐
//!                         │  InFlightTracker     │
//!                         │  (prevents duplicates)│
//!                         └──────────────────────┘
//! ```

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anchor_lang::prelude::Pubkey;
use kamino_lending::Obligation;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tracing::{info, info_span, warn, Instrument};

use crate::math::Fraction;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for parallel liquidation processing
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum number of concurrent liquidation attempts
    pub max_concurrent: usize,
    /// Channel buffer size for queued liquidations
    pub queue_size: usize,
    /// How long to keep an obligation in the "in-flight" set after completion
    pub cooldown_duration: Duration,
    /// Minimum time between attempts on the same obligation
    pub retry_delay: Duration,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 5,
            queue_size: 100,
            cooldown_duration: Duration::from_secs(5),
            retry_delay: Duration::from_secs(2),
        }
    }
}

// ============================================================================
// Data Types (Pure, Testable)
// ============================================================================

/// Represents a liquidation request before processing
#[derive(Debug, Clone)]
pub struct LiquidationRequest {
    /// Unique task ID for logging
    pub task_id: u64,
    /// Obligation pubkey
    pub obligation_pubkey: Pubkey,
    /// Deserialized obligation data
    pub obligation: Obligation,
    /// Market pubkey
    pub market_pubkey: Pubkey,
    /// Current LTV
    pub ltv: Fraction,
    /// Unhealthy LTV threshold
    pub unhealthy_ltv: Fraction,
    /// LTV margin (how far over threshold)
    pub ltv_margin_pct: f64,
    /// Slot when obligation was observed
    pub slot: u64,
    /// When the request was created
    pub created_at: Instant,
}

impl LiquidationRequest {
    /// Create a short ID for logging (first 8 chars of pubkey)
    pub fn short_id(&self) -> String {
        format!("{}:{}", self.task_id, &self.obligation_pubkey.to_string()[..8])
    }

    /// Calculate latency from creation to now
    pub fn latency_ms(&self) -> u128 {
        self.created_at.elapsed().as_millis()
    }
}

/// Result of a liquidation attempt
#[derive(Debug, Clone)]
pub enum LiquidationOutcome {
    /// Successfully liquidated
    Success {
        task_id: u64,
        obligation: Pubkey,
        duration_ms: u128,
        profit_estimate: Option<u64>,
    },
    /// Failed but can retry
    RetryableError {
        task_id: u64,
        obligation: Pubkey,
        error: String,
        duration_ms: u128,
    },
    /// Failed permanently (don't retry)
    PermanentError {
        task_id: u64,
        obligation: Pubkey,
        error: String,
        duration_ms: u128,
    },
    /// Skipped (e.g., became healthy, already liquidated)
    Skipped {
        task_id: u64,
        obligation: Pubkey,
        reason: String,
    },
}

impl LiquidationOutcome {
    pub fn task_id(&self) -> u64 {
        match self {
            Self::Success { task_id, .. } => *task_id,
            Self::RetryableError { task_id, .. } => *task_id,
            Self::PermanentError { task_id, .. } => *task_id,
            Self::Skipped { task_id, .. } => *task_id,
        }
    }

    pub fn obligation(&self) -> &Pubkey {
        match self {
            Self::Success { obligation, .. } => obligation,
            Self::RetryableError { obligation, .. } => obligation,
            Self::PermanentError { obligation, .. } => obligation,
            Self::Skipped { obligation, .. } => obligation,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }
}

// ============================================================================
// Pure Functions (Testable)
// ============================================================================

/// Determine if an obligation should be queued for liquidation
///
/// Returns `None` if should skip, `Some(reason)` if should skip with reason
pub fn should_skip_liquidation(
    obligation_pubkey: &Pubkey,
    in_flight: &HashSet<Pubkey>,
    recent_attempts: &HashSet<Pubkey>,
) -> Option<String> {
    if in_flight.contains(obligation_pubkey) {
        return Some("already in-flight".to_string());
    }
    if recent_attempts.contains(obligation_pubkey) {
        return Some("recently attempted (cooldown)".to_string());
    }
    None
}

/// Classify an error to determine if it's retryable
pub fn classify_error(error: &str) -> ErrorClassification {
    // Permanent errors - don't retry
    if error.contains("ObligationHealthy") {
        return ErrorClassification::Permanent("obligation became healthy");
    }
    if error.contains("TOKEN_NOT_TRADABLE") {
        return ErrorClassification::Permanent("collateral is LP token (not tradable)");
    }
    if error.contains("InsufficientLiquidity") {
        return ErrorClassification::Permanent("insufficient liquidity in reserve");
    }

    // Retryable errors
    if error.contains("SlippageToleranceExceeded") || error.contains("0x1788") {
        return ErrorClassification::Retryable("slippage exceeded - price moved");
    }
    if error.contains("ReserveStale") || error.contains("ObligationStale") {
        return ErrorClassification::Retryable("stale state - needs refresh");
    }
    if error.contains("BlockhashNotFound") || error.contains("timeout") {
        return ErrorClassification::Retryable("network issue");
    }

    // Unknown - treat as retryable once
    ErrorClassification::Unknown
}

#[derive(Debug, Clone, PartialEq)]
pub enum ErrorClassification {
    Permanent(&'static str),
    Retryable(&'static str),
    Unknown,
}

/// Format a log prefix with task ID for easy grep/filtering
pub fn log_prefix(task_id: u64, obligation: &Pubkey) -> String {
    format!("[T{}:{}]", task_id, &obligation.to_string()[..8])
}

// ============================================================================
// In-Flight Tracker
// ============================================================================

/// Tracks obligations currently being liquidated to prevent duplicates
pub struct InFlightTracker {
    /// Currently processing
    in_flight: RwLock<HashSet<Pubkey>>,
    /// Recently completed (in cooldown period) - Arc for sharing with cooldown tasks
    recent: Arc<RwLock<HashSet<Pubkey>>>,
    /// Cooldown duration
    cooldown: Duration,
}

impl InFlightTracker {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            in_flight: RwLock::new(HashSet::new()),
            recent: Arc::new(RwLock::new(HashSet::new())),
            cooldown,
        }
    }

    /// Try to acquire a slot for this obligation
    /// Returns true if acquired, false if already in-flight or in cooldown
    pub async fn try_acquire(&self, pubkey: &Pubkey) -> bool {
        // Check recent first (read lock)
        {
            let recent = self.recent.read().await;
            if recent.contains(pubkey) {
                return false;
            }
        }

        // Try to insert into in_flight (write lock)
        let mut in_flight = self.in_flight.write().await;
        if in_flight.contains(pubkey) {
            return false;
        }
        in_flight.insert(*pubkey);
        true
    }

    /// Release the slot and move to cooldown
    pub async fn release(&self, pubkey: &Pubkey) {
        // Remove from in_flight
        {
            let mut in_flight = self.in_flight.write().await;
            in_flight.remove(pubkey);
        }

        // Add to recent with cooldown
        let pubkey = *pubkey;
        let cooldown = self.cooldown;
        let recent = self.recent.clone();

        // Spawn task to remove from recent after cooldown
        tokio::spawn(async move {
            {
                let mut recent_guard = recent.write().await;
                recent_guard.insert(pubkey);
            }
            tokio::time::sleep(cooldown).await;
            {
                let mut recent_guard = recent.write().await;
                recent_guard.remove(&pubkey);
            }
        });
    }

    /// Get current counts for monitoring
    pub async fn stats(&self) -> (usize, usize) {
        let in_flight = self.in_flight.read().await.len();
        let recent = self.recent.read().await.len();
        (in_flight, recent)
    }

    /// Check if obligation can be processed (for pre-check without acquiring)
    pub async fn can_process(&self, pubkey: &Pubkey) -> bool {
        let in_flight = self.in_flight.read().await;
        let recent = self.recent.read().await;
        !in_flight.contains(pubkey) && !recent.contains(pubkey)
    }
}

// ============================================================================
// Liquidation Orchestrator
// ============================================================================

/// Orchestrates parallel liquidation processing
pub struct LiquidationOrchestrator {
    /// Configuration
    config: ParallelConfig,
    /// Semaphore for rate limiting
    semaphore: Arc<Semaphore>,
    /// Tracks in-flight obligations
    tracker: Arc<InFlightTracker>,
    /// Counter for unique task IDs
    task_counter: AtomicU64,
    /// Channel for sending results back (optional monitoring)
    result_tx: Option<mpsc::Sender<LiquidationOutcome>>,
}

impl LiquidationOrchestrator {
    pub fn new(config: ParallelConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
        let tracker = Arc::new(InFlightTracker::new(config.cooldown_duration));

        Self {
            config,
            semaphore,
            tracker,
            task_counter: AtomicU64::new(1),
            result_tx: None,
        }
    }

    /// Create with a result channel for monitoring outcomes
    pub fn with_result_channel(config: ParallelConfig) -> (Self, mpsc::Receiver<LiquidationOutcome>) {
        let (tx, rx) = mpsc::channel(config.queue_size);
        let mut orchestrator = Self::new(config);
        orchestrator.result_tx = Some(tx);
        (orchestrator, rx)
    }

    /// Generate a unique task ID
    fn next_task_id(&self) -> u64 {
        self.task_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Check if an obligation can be processed (without blocking)
    pub async fn can_process(&self, obligation_pubkey: &Pubkey) -> bool {
        // Check if we have capacity
        if self.semaphore.available_permits() == 0 {
            return false;
        }
        // Check if not already in-flight or cooldown
        self.tracker.can_process(obligation_pubkey).await
    }

    /// Submit a liquidation for parallel processing
    ///
    /// Returns the task ID if queued, None if skipped (duplicate/no capacity)
    pub async fn submit<F, Fut>(
        &self,
        obligation_pubkey: Pubkey,
        obligation: Obligation,
        market_pubkey: Pubkey,
        ltv: Fraction,
        unhealthy_ltv: Fraction,
        ltv_margin_pct: f64,
        slot: u64,
        liquidate_fn: F,
    ) -> Option<u64>
    where
        F: FnOnce(LiquidationRequest) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
    {
        // Try to acquire tracker slot
        if !self.tracker.try_acquire(&obligation_pubkey).await {
            info!(
                "[SKIP] {} already in-flight or cooldown",
                &obligation_pubkey.to_string()[..8]
            );
            return None;
        }

        // Try to acquire semaphore (non-blocking check first)
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                // No permits available, release tracker and skip
                self.tracker.release(&obligation_pubkey).await;
                info!(
                    "[SKIP] {} max concurrent reached ({})",
                    &obligation_pubkey.to_string()[..8],
                    self.config.max_concurrent
                );
                return None;
            }
        };

        let task_id = self.next_task_id();
        let request = LiquidationRequest {
            task_id,
            obligation_pubkey,
            obligation,
            market_pubkey,
            ltv,
            unhealthy_ltv,
            ltv_margin_pct,
            slot,
            created_at: Instant::now(),
        };

        let tracker = self.tracker.clone();
        let result_tx = self.result_tx.clone();
        let short_id = request.short_id();

        // Spawn the liquidation task
        tokio::spawn(
            async move {
                let start = Instant::now();
                let prefix = log_prefix(task_id, &obligation_pubkey);

                info!("{} Starting liquidation (LTV={:.4})", prefix, ltv_margin_pct);

                let result = liquidate_fn(request).await;
                let duration_ms = start.elapsed().as_millis();

                let outcome = match result {
                    Ok(()) => {
                        info!("{} ✓ Completed in {}ms", prefix, duration_ms);
                        LiquidationOutcome::Success {
                            task_id,
                            obligation: obligation_pubkey,
                            duration_ms,
                            profit_estimate: None, // TODO: capture from liquidate_fn
                        }
                    }
                    Err(e) => {
                        let error_str = format!("{:?}", e);
                        let classification = classify_error(&error_str);

                        match classification {
                            ErrorClassification::Permanent(reason) => {
                                info!("{} ✗ Permanent error ({}): {}", prefix, reason, duration_ms);
                                LiquidationOutcome::PermanentError {
                                    task_id,
                                    obligation: obligation_pubkey,
                                    error: error_str,
                                    duration_ms,
                                }
                            }
                            ErrorClassification::Retryable(reason) => {
                                warn!("{} ⟳ Retryable error ({}): {}ms", prefix, reason, duration_ms);
                                LiquidationOutcome::RetryableError {
                                    task_id,
                                    obligation: obligation_pubkey,
                                    error: error_str,
                                    duration_ms,
                                }
                            }
                            ErrorClassification::Unknown => {
                                warn!("{} ? Unknown error in {}ms: {}", prefix, duration_ms, &error_str[..100.min(error_str.len())]);
                                LiquidationOutcome::RetryableError {
                                    task_id,
                                    obligation: obligation_pubkey,
                                    error: error_str,
                                    duration_ms,
                                }
                            }
                        }
                    }
                };

                // Release tracker
                tracker.release(&obligation_pubkey).await;

                // Send outcome if channel configured
                if let Some(tx) = result_tx {
                    let _ = tx.send(outcome).await;
                }

                // Permit is dropped here, releasing semaphore slot
                drop(permit);
            }
            .instrument(info_span!("liquidation", task_id = task_id, obligation = %short_id))
        );

        Some(task_id)
    }

    /// Get current stats for monitoring
    pub async fn stats(&self) -> OrchestratorStats {
        let (in_flight, cooldown) = self.tracker.stats().await;
        let available_permits = self.semaphore.available_permits();

        OrchestratorStats {
            in_flight,
            cooldown,
            available_permits,
            max_concurrent: self.config.max_concurrent,
            total_submitted: self.task_counter.load(Ordering::SeqCst) - 1,
        }
    }

    // ========================================================================
    // Manual tracking interface (for when closures are impractical)
    // ========================================================================

    /// Try to start processing an obligation
    /// Returns (task_id, permit) if successful, None if blocked by dedup/cooldown/rate-limit
    /// Caller is responsible for calling `finish()` when done
    pub async fn try_start(&self, obligation_pubkey: &Pubkey) -> Option<(u64, tokio::sync::OwnedSemaphorePermit)> {
        // Try to acquire tracker slot
        if !self.tracker.try_acquire(obligation_pubkey).await {
            return None;
        }

        // Try to acquire semaphore
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                // No permits available, release tracker
                self.tracker.release(obligation_pubkey).await;
                return None;
            }
        };

        let task_id = self.next_task_id();
        Some((task_id, permit))
    }

    /// Mark an obligation as finished processing
    /// This releases the tracker slot and starts the cooldown period
    pub async fn finish(&self, obligation_pubkey: &Pubkey) {
        self.tracker.release(obligation_pubkey).await;
    }

    /// Get the tracker for direct access (advanced usage)
    pub fn tracker(&self) -> &Arc<InFlightTracker> {
        &self.tracker
    }
}

/// Statistics for monitoring
#[derive(Debug, Clone)]
pub struct OrchestratorStats {
    pub in_flight: usize,
    pub cooldown: usize,
    pub available_permits: usize,
    pub max_concurrent: usize,
    pub total_submitted: u64,
}

impl std::fmt::Display for OrchestratorStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Liquidations: {}/{} active, {} cooldown, {} total",
            self.max_concurrent - self.available_permits,
            self.max_concurrent,
            self.cooldown,
            self.total_submitted
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_skip_liquidation() {
        let mut in_flight = HashSet::new();
        let mut recent = HashSet::new();

        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        let pubkey3 = Pubkey::new_unique();

        // Not in any set - should not skip
        assert!(should_skip_liquidation(&pubkey1, &in_flight, &recent).is_none());

        // In in_flight - should skip
        in_flight.insert(pubkey2);
        assert_eq!(
            should_skip_liquidation(&pubkey2, &in_flight, &recent),
            Some("already in-flight".to_string())
        );

        // In recent - should skip
        recent.insert(pubkey3);
        assert_eq!(
            should_skip_liquidation(&pubkey3, &in_flight, &recent),
            Some("recently attempted (cooldown)".to_string())
        );
    }

    #[test]
    fn test_classify_error() {
        // Permanent errors
        assert_eq!(
            classify_error("ObligationHealthy"),
            ErrorClassification::Permanent("obligation became healthy")
        );
        assert_eq!(
            classify_error("TOKEN_NOT_TRADABLE"),
            ErrorClassification::Permanent("collateral is LP token (not tradable)")
        );

        // Retryable errors
        assert_eq!(
            classify_error("SlippageToleranceExceeded"),
            ErrorClassification::Retryable("slippage exceeded - price moved")
        );
        assert_eq!(
            classify_error("custom program error: 0x1788"),
            ErrorClassification::Retryable("slippage exceeded - price moved")
        );
        assert_eq!(
            classify_error("ReserveStale"),
            ErrorClassification::Retryable("stale state - needs refresh")
        );

        // Unknown
        assert_eq!(
            classify_error("some random error"),
            ErrorClassification::Unknown
        );
    }

    #[test]
    fn test_log_prefix() {
        let pubkey = Pubkey::new_unique();
        let prefix = log_prefix(42, &pubkey);
        assert!(prefix.starts_with("[T42:"));
        assert!(prefix.ends_with("]"));
    }

    #[tokio::test]
    async fn test_in_flight_tracker() {
        let tracker = InFlightTracker::new(Duration::from_millis(100));
        let pubkey = Pubkey::new_unique();

        // First acquire should succeed
        assert!(tracker.try_acquire(&pubkey).await);

        // Second acquire should fail (already in-flight)
        assert!(!tracker.try_acquire(&pubkey).await);

        // Release
        tracker.release(&pubkey).await;

        // Should still fail (in cooldown)
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!tracker.try_acquire(&pubkey).await);

        // After cooldown, should succeed
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(tracker.try_acquire(&pubkey).await);
    }

    #[tokio::test]
    async fn test_orchestrator_stats() {
        let config = ParallelConfig {
            max_concurrent: 5,
            ..Default::default()
        };
        let orchestrator = LiquidationOrchestrator::new(config);

        let stats = orchestrator.stats().await;
        assert_eq!(stats.max_concurrent, 5);
        assert_eq!(stats.available_permits, 5);
        assert_eq!(stats.in_flight, 0);
        assert_eq!(stats.total_submitted, 0);
    }
}
