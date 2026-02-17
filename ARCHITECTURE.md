# Kamino Terminator Architecture

A flash loan liquidator bot for the Kamino Lending protocol on Solana.

## Overview

The bot monitors unhealthy lending positions (obligations) on Kamino via Geyser streaming and liquidates them using flash loans — no upfront capital required, only SOL for transaction fees. Liquidations are submitted as Jito bundles for MEV protection.

## Key Concepts

- **Obligation**: A user's lending position containing deposits (collateral) and borrows. When the Loan-to-Value ratio exceeds the `unhealthy_ltv` threshold, it becomes liquidatable.
- **Reserve**: A single asset market (e.g., SOL, USDC) tracking liquidity, borrows, interest rates, oracle configs, and flash loan fees.
- **cTokens**: Interest-bearing tokens representing a share of a reserve's liquidity pool. The `liquidate_obligation_and_redeem_reserve_collateral` instruction atomically liquidates and redeems cTokens, so the liquidator receives the underlying token directly.

## Flash Loan Liquidation Flow

```
┌────────────────────────────────────────────────────────────┐
│                  Single Atomic Transaction                  │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  1. Flash Borrow                                           │
│     └─> Borrow debt token from Kamino reserve              │
│                                                            │
│  2. Refresh Reserves & Obligation                          │
│     └─> Update all reserve prices and obligation state     │
│                                                            │
│  3. Liquidate & Redeem                                     │
│     └─> Repay debt, receive collateral (auto-redeemed)     │
│                                                            │
│  4. Swap Collateral                                        │
│     └─> Swap collateral back to debt token via Metis       │
│                                                            │
│  5. Flash Repay                                            │
│     └─> Repay flash loan + fee, keep profit                │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### Transaction Structure

```
Index  Instruction
─────  ──────────────────────────────────────────
0      ComputeBudget::SetComputeUnitLimit
1      ComputeBudget::SetComputeUnitPrice
2      flash_borrow_reserve_liquidity        ← flash_borrow_index = 2
3      refresh_reserve (collateral)
4      refresh_reserve (debt)
5      refresh_obligation
6      liquidate_obligation_and_redeem_reserve_collateral
7-N    swap instructions (from Metis/Jupiter)
N+1    flash_repay_reserve_liquidity(borrow_index=2)
```

The `flash_repay` instruction references the `flash_borrow` by its index, allowing the protocol to verify the loan is repaid in the same transaction. All reserves referenced by the obligation must be refreshed in the same slot to ensure synchronized prices.

## Architecture Components

### 1. Geyser Streaming (`geyser.rs`)

Real-time account monitoring via Helius LaserStream (Yellowstone gRPC):
- Subscribes to obligation accounts (by program owner), oracle accounts, and reserve accounts (by pubkey)
- **OracleCache**: `Arc<RwLock<HashMap>>` with slot-based staleness protection, updated on each Geyser oracle update
- **ReserveCache**: `Arc<RwLock<HashMap>>` storing deserialized `Reserve` structs. Deserialization happens on the write path (off the critical liquidation path) so reads are instant
- Caches are seeded from loaded market state before streaming begins
- Auto-reconnect with exponential backoff (500ms to 60s max), stale connection detection (2min timeout)

### 2. Obligation Scanner (`scanner.rs`)

Evaluates obligation health from Geyser updates. Calculates current LTV, identifies liquidatable positions, and determines optimal liquidation amount.

### 3. Swap Router (`routing.rs`, `metis.rs`)

Gets optimal swap routes via Metis/Jupiter HTTP API:
- Single `get_quote_with_instructions` call for route + instructions + ALTs in one request
- Uses `only_direct_routes=true` to minimize accounts; falls back to non-direct routes when direct routes lack ALTs

### 4. Address Lookup Tables (`lookup_tables.rs`)

Per-market ALTs created/extended at startup only, cached in memory:
- Each market has its own ALT (~170 addresses): reserve pubkeys, liquidator ATAs, market authorities
- Reduces transaction size by replacing 32-byte addresses with 1-byte indices
- 256-address hard limit per ALT; reserves prioritized by borrow volume

### 5. Liquidation Orchestrator (`parallel.rs`)

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Geyser Stream  │────>│  LiquidationOrch.    │────>│  liquidate_fast │
│  (obligations)  │     │  (dedup + cooldown)  │     │  (sequential)   │
└─────────────────┘     └──────────────────────┘     └─────────────────┘
```

- Deduplication and cooldown tracking via `InFlightTracker`
- Semaphore-based concurrency control
- Error classification: Permanent (don't retry), Retryable (retry after cooldown), Unknown

Liquidations execute sequentially on the main thread because the Kamino lending library uses `Rc<RefCell<>>` internally (`!Send`). The orchestrator still prevents duplicate attempts on the same obligation.

### 6. Jito Submission

Transactions are wrapped in Jito bundles (tip + liquidation tx) and submitted to 4 EU endpoints in parallel via `select_ok` — first successful response wins.

## Transaction Size Constraints

Solana transactions have a **1232-byte** serialized limit. Size reduction strategies:
- Address Lookup Tables (per-market + swap ALTs from Metis)
- Direct swap routes to minimize accounts

| Collaterals | Fits in TX? | Notes |
|-------------|-------------|-------|
| 1-4 | Yes | 5-15% headroom |
| 5+ | No | Exceeds 1232 bytes even with all optimizations |

## Hot Path Optimizations

Speed is critical — positions can become healthy again within seconds. The bot minimizes latency on the critical path from Geyser update to Jito submission.

### Cached Data

- **MarketState**: Reserves, referrer token states, oracle data (refreshed every ~4 hours)
- **OracleCache**: Real-time oracle prices streamed via Geyser
- **ReserveCache**: Real-time reserve data streamed via Geyser
- **BlockhashCache**: Background task refreshes every 2s via `tokio::sync::watch` channel
- **Lookup tables**: Loaded once at startup, cached in memory

### Fast Liquidation Path (`liquidate_fast`)

| Data | Source | Latency |
|------|--------|---------|
| Obligation | Geyser update | 0ms |
| Reserves | Geyser ReserveCache | 0ms |
| Oracle prices | Geyser OracleCache | 0ms |
| Referrer states | MarketState cache | 0ms |
| Clock/slot | Geyser slot + buffer | 0ms |
| Blockhash | Background cache | 0ms |
| Lookup tables | Memory cache | 0ms |
| Farm user states | RPC (parallel with swap) | 0ms extra |
| Swap quotes | Metis HTTP API | ~200ms |

### Parallel Execution

```
Reserve cache read (0ms) ──┐
                           ├── Parallel if cache misses
get_slot fallback (40ms) ──┘

Farm prefetch (60ms) ──────┐
                           ├── tokio::join! (max of both, not sum)
Metis swap quote (200ms) ──┘

Build tx (cached blockhash, 0ms) ──> Jito submit (4 endpoints race, 80ms)
```

### RPC Calls Per Liquidation

With all caches warm (common case):

| Call | Latency |
|------|---------|
| `get_multiple_accounts` (farm states) | ~60ms (hidden under swap) |
| Metis API `/quote` + `/swap-instructions` | ~200ms |
| Jito `send_bundle` x 4 endpoints | ~80ms |

**Typical critical path: ~280ms** (Metis + Jito, farm hidden under Metis)

### What Cannot Be Cached

- **Swap quotes**: Real-time market data from Metis/Jupiter
- **Farm user states**: Per-obligation PDAs, fetched each time but hidden in parallel section
