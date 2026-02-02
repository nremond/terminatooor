# Kamino Terminator Architecture

A flash loan liquidator bot for the Kamino Lending protocol on Solana.

## Overview

This bot monitors unhealthy lending positions (obligations) on Kamino and liquidates them for profit. It uses **flash loans** to execute liquidations without requiring upfront capital - only SOL for transaction fees.

## Key Concepts

### Obligations
An **obligation** is a user's lending position on Kamino. It contains:
- **Deposits**: Collateral the user has deposited (stored as cTokens)
- **Borrows**: Tokens the user has borrowed

Each obligation has a **Loan-to-Value (LTV)** ratio. When LTV exceeds the `unhealthy_ltv` threshold, the position becomes liquidatable.

### Reserves
A **reserve** represents a single asset market (e.g., SOL, USDC). Each reserve tracks:
- Total liquidity available for borrowing
- Total borrowed amount
- Interest rates and fees
- Oracle price configurations
- Flash loan fee

### cTokens (Collateral Tokens)
When users deposit assets into Kamino, they receive **cTokens** in return. These are interest-bearing tokens that represent a share of the reserve's liquidity pool.

- Depositing 100 SOL might give you 95 kSOL (cTokens)
- As interest accrues, the exchange rate increases
- Later, those 95 kSOL might be redeemable for 105 SOL

**Exchange Rate Formula:**
```
exchange_rate = total_liquidity / ctoken_supply

where:
  total_liquidity = available_amount + borrowed_amount
  ctoken_supply = mint_total_supply of the cToken
```

## Flash Loan Liquidation Flow

The bot executes liquidations atomically using flash loans:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Single Atomic Transaction                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. Flash Borrow                                                     │
│     └─> Borrow debt token (e.g., USDC) from Kamino reserve          │
│                                                                      │
│  2. Refresh Reserves & Obligation                                    │
│     └─> Update all reserve prices and obligation state              │
│                                                                      │
│  3. Liquidate & Redeem                                               │
│     └─> Repay user's debt with borrowed tokens                      │
│     └─> Receive collateral (automatically redeemed from cTokens)    │
│     └─> e.g., Pay 100 USDC debt, receive 105 USDC worth of SOL      │
│                                                                      │
│  4. Swap Collateral                                                  │
│     └─> Swap received collateral back to debt token                 │
│     └─> e.g., Swap SOL → USDC via Titan aggregator                  │
│                                                                      │
│  5. Flash Repay                                                      │
│     └─> Repay flash loan + fee                                      │
│     └─> Keep remaining tokens as profit                             │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Why cTokens Don't Appear in Swaps

The Kamino instruction `liquidate_obligation_and_redeem_reserve_collateral` does two things atomically:
1. **Liquidates** the position (repay debt, seize collateral cTokens)
2. **Redeems** the cTokens for underlying tokens

So the liquidator receives the actual underlying token (SOL, not kSOL), which can be directly swapped on DEX aggregators.

### Profit Calculation

```
profit = collateral_received - flash_loan_repay - swap_slippage

where:
  collateral_received = liquidate_amount × (1 + liquidation_bonus)
  flash_loan_repay = liquidate_amount × (1 + flash_loan_fee)
  swap_slippage = collateral_received × slippage_bps / 10000
```

Typical values:
- Liquidation bonus: 5% (you get 5% more collateral than debt repaid)
- Flash loan fee: 0.09% (minimal cost)
- Swap slippage: 0.5-1%

## Architecture Components

### 1. Geyser Streaming (`geyser.rs`)
Real-time account monitoring via Triton/Helius WebSocket:
- Subscribes to all Kamino obligation accounts
- Receives instant updates when positions become unhealthy
- Much faster than polling RPC endpoints
- **Initialization order**: Geyser connection is deferred until AFTER market state loading to prevent channel overflow during startup

### 2. Obligation Scanner (`scanner.rs`)
Evaluates obligation health:
- Calculates current LTV from refreshed reserve prices
- Identifies liquidatable positions (LTV > unhealthy_ltv)
- Determines optimal liquidation amount

### 3. Price Oracle (`px.rs`)
Fetches token prices from multiple sources:
- Pyth Network
- Switchboard
- Scope (Kamino's price aggregator)

### 4. Swap Router (`titan.rs`, `routing.rs`)
Gets optimal swap routes via Titan aggregator:
- Connects via WebSocket for streaming quotes
- Supports all major Solana DEXes
- Returns transaction instructions for the best route
- **Early exit on empty quotes**: If Titan returns 3 consecutive empty quote responses (no liquidity for the pair), exits early instead of waiting for the 30-second timeout
- **Single quote call**: Uses `get_quote_with_instructions` to get route AND instructions in one call, ensuring ALTs match the actual swap instructions
- **Non-direct route fallback**: When direct routes have no ALTs (causing large transactions), automatically tries non-direct routes which may use DEXes that provide lookup tables

### 5. Liquidator (`liquidator.rs`)
Manages the liquidator wallet:
- Creates Associated Token Accounts (ATAs) for all tokens
- Handles both SPL Token and Token-2022 programs
- Tracks token holdings and balances

### 6. Transaction Builder (`client.rs`, `instructions.rs`)
Constructs and sends liquidation transactions:
- Builds flash loan borrow/repay instructions
- Adds compute budget for complex transactions
- Handles transaction confirmation and retries

### 7. Address Lookup Tables (`lookup_tables.rs`)
Per-market lookup tables to reduce transaction size:
- Each lending market has its own Address Lookup Table (ALT)
- ALTs are created/extended **only at startup**, stored in `liquidator_lookup_tables.json`
- Contains keys for that market only: reserve pubkeys, liquidator ATAs for market tokens, market authorities
- Reduces transaction size by replacing 32-byte addresses with 1-byte indices
- **Cached in memory** after initial load - no RPC calls during liquidation
- Periodic refresh skips ALT extension to avoid blocking the hot path

**256 Address Limit**: Solana ALTs have a hard limit of 256 addresses. For markets with many reserves, the bot prioritizes reserves by borrow volume (most active first). If a market has more than ~22 reserves, low-volume reserves won't fit in the LUT, and liquidations involving those reserves may fail due to transaction size. See warning log: `Lookup table for market X is full (256 addresses)`

### 8. Liquidation Orchestrator (`parallel.rs`)
Manages liquidation attempts with deduplication and cooldown:
- **Deduplication**: Prevents attempting same obligation twice simultaneously
- **Cooldown tracking**: Enforces wait period between attempts on same obligation
- **Rate limiting**: Semaphore-based concurrency control
- **Structured logging**: Unique task IDs for filtering logs (`[T42:8xBnR5kd]`)
- **Error classification**: Categorizes errors as Permanent/Retryable/Unknown
- **Pure functions**: Core logic testable without I/O

## Reserve Refresh Requirements

Before liquidating, ALL reserves referenced by an obligation must be refreshed in the same slot:

```rust
// An obligation with SOL collateral and USDC + BONK borrows
// requires refreshing ALL THREE reserves:
refresh_reserve(SOL_RESERVE);   // collateral
refresh_reserve(USDC_RESERVE);  // borrow 1
refresh_reserve(BONK_RESERVE);  // borrow 2
refresh_obligation(...);
liquidate(...);
```

This ensures prices are synchronized and the protocol can accurately calculate the obligation's health.

## Transaction Size Constraints

Solana transactions have a hard limit of **1232 bytes** (serialized). Flash loan liquidations are complex transactions that can exceed this limit.

### Size Reduction Strategies

1. **Address Lookup Tables (ALTs)**: Replace 32-byte addresses with 1-byte indices
2. **Direct swap routes**: Use `only_direct_routes=true` to avoid multi-hop swaps that add accounts
3. **Per-market ALTs**: Each market has its own ALT with ~170 addresses
4. **Swap ALTs**: Titan returns ALTs for swap instructions, fetched and included in transaction

### Known Limitations

| Collaterals | Fits in TX? | Notes |
|-------------|-------------|-------|
| 1-2 | ✅ Yes | ~10-15% headroom |
| 3-4 | ✅ Yes | ~5% headroom |
| 5+ | ❌ No | Exceeds 1232 bytes even with all optimizations |

Positions with 5+ collateral types cannot be liquidated in a single transaction. Future options:
- Jito bundles (but flash loans require borrow+repay in same TX)
- Multiple partial liquidations
- Custom on-chain helper program

### Lookup Table Overflow

Markets with many reserves (>22) exceed the 256-address ALT limit:

| Symptom | Cause | Impact |
|---------|-------|--------|
| `Lookup table is full (256 addresses). X keys not in LUT` | Market has more reserves than ALT can hold | Liquidations involving low-volume reserves may exceed tx size |

**Current behavior**: Reserves are prioritized by borrow volume. High-volume reserves are always in the LUT. Low-volume reserves may be excluded.

**Future options**:
- Multiple LUTs per market (overflow tables)
- Per-liquidation check to skip positions involving excluded reserves
- On-demand LUT creation for specific liquidations

## Token-2022 Support

Some Solana tokens use the new Token-2022 program instead of the original SPL Token program. The bot handles this by:

1. **Detecting token program**: Check the mint account's owner
2. **Creating correct ATAs**: Use `get_associated_token_address_with_program_id`
3. **Parsing accounts**: Token-2022 accounts may have extensions (>165 bytes) but the base account structure is compatible

## Transaction Structure

A typical flash loan liquidation transaction:

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
7-N    swap instructions (from Titan)
N+1    flash_repay_reserve_liquidity(borrow_index=2)
```

The `flash_repay` instruction references the `flash_borrow` by its index, allowing the protocol to verify the loan is repaid in the same transaction.

## Hot Path Optimizations

Speed is critical for liquidations - positions can become healthy again within seconds. The bot uses several optimizations to minimize latency.

### Cached Data (MarketState)

At startup and during periodic refresh, the bot caches:
- **Reserves**: All reserve data for each market
- **Referrer token states**: Fee distribution accounts
- **Oracle accounts**: Pyth, Switchboard, Scope price data
- **Lookup tables**: Address lookup tables loaded once, then cached in memory

### Fast Liquidation Path (`liquidate_fast`)

When a liquidatable position is detected via Geyser, the bot uses cached data instead of making redundant RPC calls:

| Data | Slow Path | Fast Path |
|------|-----------|-----------|
| Obligation | RPC fetch (~140ms) | From Geyser update |
| Reserves | RPC fetch (~75ms) | From MarketState cache |
| Referrer states | RPC fetch (~50ms) | From MarketState cache |
| Clock/slot | RPC fetch (~50ms) | From Geyser update |
| Lookup tables | RPC fetch × 26 (~1100ms) | Memory cache |
| Oracle prices | RPC fetch (~100ms) | RPC fetch (required) |

**Total savings: ~300-400ms per liquidation attempt**

### What Cannot Be Cached

- **Oracle prices**: Must be fresh for accurate LTV simulation
- **Swap quotes**: Real-time from Titan (~550ms)

### Periodic State Refresh

Market state is refreshed periodically (default: every 4 hours) to catch:
- New reserves added to markets
- Configuration changes
- New oracle accounts

Configurable via `--state-refresh-hours` or `STATE_REFRESH_HOURS` env var.

## Liquidation Orchestrator (`parallel.rs`)

The bot includes an orchestrator module for managing liquidation attempts with deduplication, cooldown tracking, and structured logging.

### Architecture

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Geyser Stream  │────>│  LiquidationOrch.    │────>│  liquidate_fast │
│  (obligations)  │     │  (dedup + cooldown)  │     │  (sequential)   │
└─────────────────┘     └──────────────────────┘     └─────────────────┘
                                  │
                                  v
                        ┌──────────────────────┐
                        │  InFlightTracker     │
                        │  (prevents duplicates)│
                        └──────────────────────┘
```

### Features

1. **Deduplication**: Prevents attempting the same obligation twice simultaneously
2. **Cooldown Tracking**: Enforces a wait period after each attempt (default: 5 seconds)
3. **Rate Limiting**: Semaphore-based limit on concurrent attempts (default: 5)
4. **Structured Logging**: Each attempt gets a unique task ID for filtering logs
5. **Error Classification**: Categorizes errors for appropriate handling
6. **Pure Functions**: Core logic in testable pure functions

### Data Types

**LiquidationRequest** - Input data for a liquidation attempt:
```rust
struct LiquidationRequest {
    task_id: u64,              // Unique identifier for logging
    obligation_pubkey: Pubkey, // The obligation to liquidate
    obligation: Obligation,    // Deserialized obligation data
    market_pubkey: Pubkey,     // Which market this belongs to
    ltv: Fraction,             // Current LTV ratio
    unhealthy_ltv: Fraction,   // Threshold for liquidation
    ltv_margin_pct: f64,       // How far over threshold (e.g., 0.05 = 5%)
    slot: u64,                 // Slot when observed
    created_at: Instant,       // For latency tracking
}
```

**LiquidationOutcome** - Result of an attempt:
```rust
enum LiquidationOutcome {
    Success { task_id, obligation, duration_ms, profit_estimate },
    RetryableError { task_id, obligation, error, duration_ms },
    PermanentError { task_id, obligation, error, duration_ms },
    Skipped { task_id, obligation, reason },
}
```

### InFlightTracker

Tracks which obligations are currently being processed:

```rust
struct InFlightTracker {
    in_flight: RwLock<HashSet<Pubkey>>,  // Currently processing
    recent: Arc<RwLock<HashSet<Pubkey>>>, // In cooldown period
    cooldown: Duration,                   // How long to wait after attempt
}
```

**Methods:**
- `try_acquire(pubkey)` - Returns true if slot acquired, false if already in-flight or cooldown
- `release(pubkey)` - Releases slot and starts cooldown timer
- `can_process(pubkey)` - Check without acquiring (for pre-filtering)

### Pure Functions (Testable)

The module uses pure functions for core logic to enable unit testing:

```rust
// Check if obligation should be skipped
fn should_skip_liquidation(
    pubkey: &Pubkey,
    in_flight: &HashSet<Pubkey>,
    recent: &HashSet<Pubkey>,
) -> Option<String>  // Returns skip reason or None

// Classify error for retry decisions
fn classify_error(error: &str) -> ErrorClassification

// Format log prefix for consistent logging
fn log_prefix(task_id: u64, obligation: &Pubkey) -> String
```

### Task ID Format

Logs use the format `[T{task_id}:{obligation_short}]` for easy grep/filtering:
```
[T42:8xBnR5kd] Starting liquidation (LTV margin=2.50%)
[T42:8xBnR5kd] ✓ Liquidation completed
```

Filter logs for a specific task: `grep "\[T42:" logs.txt`

### Error Classification

Errors are classified to determine retry behavior:

| Classification | Examples | Behavior |
|----------------|----------|----------|
| **Permanent** | `ObligationHealthy`, `TOKEN_NOT_TRADABLE`, `InsufficientLiquidity` | Don't retry |
| **Retryable** | `SlippageToleranceExceeded` (0x1788), `ReserveStale`, `BlockhashNotFound`, `timeout` | Can retry after cooldown |
| **Unknown** | Other errors | Logged for investigation, treated as retryable once |

### Log Symbols

| Symbol | Meaning |
|--------|---------|
| `✓` | Success |
| `✗` | Permanent error (won't retry) |
| `⟳` | Retryable error (may retry after cooldown) |
| `?` | Unknown error (logged for investigation) |
| `⚠` | Panic caught (klend library issue) |

### Configuration

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `--max-concurrent` | `MAX_CONCURRENT_LIQUIDATIONS` | 5 | Max simultaneous liquidation attempts |
| `--cooldown-secs` | `LIQUIDATION_COOLDOWN_SECS` | 5 | Seconds to wait before retrying same obligation |

### Orchestrator Flow

```
1. Geyser detects liquidatable obligation
2. orchestrator.try_start(pubkey) called
   ├─ If in_flight or cooldown → return None (skip)
   └─ If available → acquire slot, return (task_id, permit)
3. Execute liquidate_fast()
4. Log result with task_id prefix
5. orchestrator.finish(pubkey) releases slot, starts cooldown
6. After cooldown_secs, obligation can be attempted again
```

### Statistics

The orchestrator provides monitoring stats:

```rust
struct OrchestratorStats {
    in_flight: usize,        // Currently processing
    cooldown: usize,         // In cooldown period
    available_permits: usize, // Remaining capacity
    max_concurrent: usize,   // Configured limit
    total_submitted: u64,    // Lifetime counter
}
```

Display format: `Liquidations: 2/5 active, 3 cooldown, 47 total`

### Why Not True Parallelism?

The Kamino lending library uses `Rc<RefCell<>>` for internal state management, which is not thread-safe (`!Send`). This means liquidation futures cannot be spawned across threads with `tokio::spawn`.

**What this means:**
- Liquidations execute sequentially on the main thread
- The orchestrator still provides deduplication and cooldown tracking
- Multiple liquidatable obligations discovered simultaneously won't cause duplicate attempts

**What would be needed for true parallelism:**
- Kamino library would need to use `Arc<Mutex<>>` instead of `Rc<RefCell<>>`
- This is outside our control as it's in the external `kamino_lending` crate

## Configuration

Key environment variables:
- `RPC_ENDPOINT`: Solana RPC URL (Helius/Triton recommended)
- `GEYSER_ENDPOINT`: Geyser WebSocket URL for streaming
- `GEYSER_API_KEY`: API key for Geyser authentication
- `TITAN_WS_URL`: Titan aggregator WebSocket URL
- `WALLET_PATH`: Path to liquidator keypair JSON
- `STATE_REFRESH_HOURS`: How often to refresh market state (default: 4)
- `MAX_CONCURRENT_LIQUIDATIONS`: Max concurrent liquidation attempts (default: 5)
- `LIQUIDATION_COOLDOWN_SECS`: Cooldown between attempts on same obligation (default: 5)
- `LIQUIDATOR_LOOKUP_TABLE_FILE`: Path to lookup tables JSON (default: `liquidator_lookup_tables.json`)

## Non-Tradable Collateral (LP Tokens)

Some Kamino reserves use **LP tokens** as collateral (e.g., kUXDUSDCOrca, Orca LP shares). These tokens cannot be swapped on DEX aggregators like Jupiter/Metis because they represent shares in a liquidity pool, not fungible tokens.

### Example Error
```
Metis quote failed: 400 Bad Request - {"error":"The token 4G9USgnbg6fDTQ5AUfpCjM89zqbzWj32xfqvsaAu66DM is not tradable","errorCode":"TOKEN_NOT_TRADABLE"}
```

### Current Behavior
Flash loan liquidations are **skipped** when the collateral token is not tradable. The liquidator logs a warning and moves on.

### Why This Happens
1. LP tokens represent pool shares, not a simple token balance
2. Converting them requires **withdrawing from the LP position** first
3. DEX aggregators don't support this multi-step conversion

### Known Non-Tradable Token Types
| Token Type | Example | Reason |
|------------|---------|--------|
| Kamino kTokens | kUXDUSDCOrca | Vault share tokens |
| Orca LP tokens | Various Orca pools | Concentrated liquidity positions |
| Raydium LP tokens | Various Raydium pools | AMM pool shares |

### Future Improvements
To liquidate positions with LP collateral, the bot would need to:
1. Flash borrow the debt token
2. Liquidate and receive LP tokens
3. **Withdraw from the LP** to get underlying tokens
4. Swap underlying tokens to debt token
5. Repay flash loan

This requires custom logic for each LP type (Orca, Raydium, Kamino vaults, etc.).

### Alternative: Non-Flash-Loan Liquidation
If the liquidator already holds the debt token, it can use the `LiquidateAndRedeem` strategy:
- No swap needed - keep the LP tokens as profit
- Manually withdraw from LP later
- Useful for accumulating LP positions at a discount

## Error Handling

Common errors and their causes:

| Error | Cause | Solution |
|-------|-------|----------|
| `ReserveStale` | Not all obligation reserves refreshed | Refresh ALL deposit and borrow reserves |
| `InvalidFlashRepay` | Wrong borrow instruction index | Account for ComputeBudget instructions (index = base + 2) |
| `NoValidRoute` | Titan can't find swap path or empty quotes | Skip liquidation - no liquidity for this pair |
| `TOKEN_NOT_TRADABLE` | Collateral is LP token (kToken, Orca LP, etc.) | Skip - LP tokens can't be swapped directly |
| `IncorrectProgramId` | Token-2022 mint with SPL Token ATA | Detect token program and use correct one |
| `Timeout` | Titan WebSocket timeout | Service may be down, retry later |
| `TransactionTooLarge` | TX exceeds 1232 bytes | Position has too many collaterals (5+) |

## Running the Bot

```bash
# Build
cargo build --release

# Run (simulation mode)
SHOULD_SEND=false cargo run --release

# Run (live mode)
SHOULD_SEND=true cargo run --release
```

The bot will:
1. Load all Kamino reserves and markets
2. Create/extend Address Lookup Tables for each market
3. Create ATAs for all token mints
4. Connect to Geyser and subscribe to obligations (after init to prevent channel overflow)
5. Monitor for unhealthy positions
6. Execute flash loan liquidations when profitable
