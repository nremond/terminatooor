//! Swap routing module using Metis/Jupiter API
//!
//! This module provides swap functionality via Triton's Metis API,
//! which returns address lookup tables for transaction compression.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use anchor_lang::prelude::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::address_lookup_table::{state::AddressLookupTable, AddressLookupTableAccount};
use solana_sdk::instruction::Instruction;
use tracing::{debug, info, warn};

use crate::consts::{
    EXTRA_ACCOUNTS_BUFFER, MAX_ACCOUNTS_PER_TRANSACTION, MAX_EXTRA_ACCOUNTS_BUFFER,
};
use crate::metis;

/// Known Jupiter/Metis ALTs to prewarm at startup.
/// Add addresses here as they appear in logs ("Loaded swap lookup table X").
const KNOWN_SWAP_ALTS: &[&str] = &[
    "3bDh8FpfSZezpiwfKZZJXFKHyoCBqu4LfQaf8Xo3kvJX",
];

/// Cache for swap Address Lookup Tables to avoid repeated RPC fetches.
/// Jupiter/Metis ALTs are long-lived and rarely change within a session.
pub struct SwapAltCache {
    inner: RwLock<HashMap<Pubkey, AddressLookupTableAccount>>,
}

impl SwapAltCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Prewarm the cache by fetching known ALTs from RPC at startup.
    pub async fn prewarm(&self, rpc_client: &RpcClient) {
        let addresses: Vec<Pubkey> = KNOWN_SWAP_ALTS
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        if addresses.is_empty() {
            return;
        }

        info!("Prewarming swap ALT cache with {} known addresses", addresses.len());
        let mut loaded = 0;
        for address in &addresses {
            match rpc_client.get_account(address).await {
                Ok(account) => {
                    match AddressLookupTable::deserialize(&account.data) {
                        Ok(table) => {
                            let alt = AddressLookupTableAccount {
                                key: *address,
                                addresses: table.addresses.to_vec(),
                            };
                            info!("Prewarmed swap ALT {} ({} addresses)", address, alt.addresses.len());
                            self.inner.write().unwrap().insert(*address, alt);
                            loaded += 1;
                        }
                        Err(e) => warn!("Failed to deserialize swap ALT {}: {:?}", address, e),
                    }
                }
                Err(e) => warn!("Failed to fetch swap ALT {}: {:?}", address, e),
            }
        }
        info!("Swap ALT cache prewarmed: {}/{} loaded", loaded, addresses.len());
    }
}

/// Error type for routing operations
#[derive(Debug)]
pub enum RoutingError {
    NoValidRoute,
    PriceImpactTooHigh(f32),
    ApiError(String),
}

impl std::fmt::Display for RoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingError::NoValidRoute => write!(f, "No valid route found"),
            RoutingError::PriceImpactTooHigh(pct) => write!(f, "Price impact too high: {}%", pct),
            RoutingError::ApiError(msg) => write!(f, "API error: {}", msg),
        }
    }
}

impl std::error::Error for RoutingError {}

pub type RoutingResult<T> = std::result::Result<T, RoutingError>;

/// Decompiled transaction with instructions and lookup tables
#[derive(Debug)]
pub struct DecompiledVersionedTx {
    pub instructions: Vec<Instruction>,
    pub lookup_tables: Option<Vec<AddressLookupTableAccount>>,
}

/// Swap route information
#[derive(Debug, Clone)]
pub struct SwapRoute {
    pub in_amount: u64,
    pub out_amount: u64,
    pub slippage_bps: u16,
    pub price_impact_pct: String,
    pub address_lookup_tables: Vec<Pubkey>,
}

/// Swap result containing both quote info and instructions
#[derive(Debug)]
pub struct SwapResult {
    pub route: SwapRoute,
    pub tx: DecompiledVersionedTx,
}

#[allow(clippy::too_many_arguments)]
/// Get the swap instructions for the best route matching parameters
/// Returns both the route (with quote info) and the transaction instructions
pub async fn get_best_swap_instructions(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    only_direct_routes: bool,
    slippage_bps: Option<u16>,
    price_impact_limit: Option<f32>,
    user_public_key: Pubkey,
    rpc_client: &RpcClient,
    accounts: Option<&Vec<&Pubkey>>,
    accounts_count_buffer: Option<usize>,
    swap_alt_cache: &SwapAltCache,
) -> RoutingResult<SwapResult> {
    // Try with the requested route type first
    let result = get_best_swap_instructions_inner(
        input_mint,
        output_mint,
        amount,
        only_direct_routes,
        slippage_bps,
        price_impact_limit,
        user_public_key,
        rpc_client,
        accounts,
        accounts_count_buffer,
        swap_alt_cache,
    )
    .await;

    // If direct route succeeded but has no ALTs, try non-direct as fallback
    // Non-direct routes might use DEXes that provide lookup tables
    if only_direct_routes {
        if let Ok(ref swap_result) = result {
            if swap_result.route.address_lookup_tables.is_empty() {
                info!("Direct route has no ALTs, trying non-direct route as fallback...");
                let non_direct_result = get_best_swap_instructions_inner(
                    input_mint,
                    output_mint,
                    amount,
                    false, // try non-direct
                    slippage_bps,
                    price_impact_limit,
                    user_public_key,
                    rpc_client,
                    accounts,
                    accounts_count_buffer,
                    swap_alt_cache,
                )
                .await;

                if let Ok(non_direct_swap) = non_direct_result {
                    if !non_direct_swap.route.address_lookup_tables.is_empty() {
                        info!(
                            "Non-direct route has {} ALTs, using it instead",
                            non_direct_swap.route.address_lookup_tables.len()
                        );
                        return Ok(non_direct_swap);
                    }
                }
                // Fall through to return original direct route result
            }
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn get_best_swap_instructions_inner(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    only_direct_routes: bool,
    slippage_bps: Option<u16>,
    price_impact_limit: Option<f32>,
    user_public_key: Pubkey,
    rpc_client: &RpcClient,
    accounts: Option<&Vec<&Pubkey>>,
    accounts_count_buffer: Option<usize>,
    swap_alt_cache: &SwapAltCache,
) -> RoutingResult<SwapResult> {
    let accounts_count_buffer = accounts_count_buffer.unwrap_or(0);
    let mut extra_accounts_buffer = EXTRA_ACCOUNTS_BUFFER;

    let mut accounts_distinct: HashSet<&Pubkey> = HashSet::new();
    if let Some(accounts) = accounts {
        accounts_distinct.extend(accounts.iter());
    }
    accounts_distinct.insert(&user_public_key);

    let accounts_distinct_count = accounts_distinct.len();

    while extra_accounts_buffer < MAX_EXTRA_ACCOUNTS_BUFFER {
        let max_accounts = MAX_ACCOUNTS_PER_TRANSACTION
            .saturating_sub(accounts_distinct_count)
            .saturating_sub(extra_accounts_buffer)
            .saturating_sub(accounts_count_buffer);

        info!(
            "Trying swap route with max_accounts={} (buffer={}) direct_only={}",
            max_accounts, extra_accounts_buffer, only_direct_routes
        );

        // Get quote and swap instructions from Metis
        let result = metis::get_quote_and_swap_instructions(
            input_mint,
            output_mint,
            amount,
            slippage_bps,
            if only_direct_routes { Some(true) } else { None },
            Some(max_accounts.try_into().unwrap_or(64)),
            &user_public_key,
        )
        .await;

        match result {
            Ok((quote, swap_ixs)) => {
                // Validate price impact
                let route_price_impact_pct = if quote.price_impact_pct.is_empty() {
                    0.0
                } else {
                    quote.price_impact_pct.parse::<f32>().unwrap_or(0.0)
                };
                if let Some(limit) = price_impact_limit {
                    if route_price_impact_pct > limit {
                        info!("Price impact {} exceeds limit {}", route_price_impact_pct, limit);
                        extra_accounts_buffer += 2;
                        continue;
                    }
                }

                info!("Got route: in={} out={}", quote.in_amount, quote.out_amount);

                // Convert Metis instructions to Solana instructions
                let mut instructions: Vec<Instruction> = Vec::new();

                // Add compute budget instructions (Metis handles this for us)
                for ix_data in &swap_ixs.compute_budget_instructions {
                    match ix_data.to_instruction() {
                        Ok(ix) => instructions.push(ix),
                        Err(e) => warn!("Failed to parse compute budget instruction: {}", e),
                    }
                }

                // Add setup instructions (token account creation)
                for ix_data in &swap_ixs.setup_instructions {
                    match ix_data.to_instruction() {
                        Ok(ix) => instructions.push(ix),
                        Err(e) => warn!("Failed to parse setup instruction: {}", e),
                    }
                }

                // Add main swap instruction
                let swap_ix = swap_ixs.swap_instruction.to_instruction()
                    .map_err(|e| RoutingError::ApiError(format!("Failed to parse swap instruction: {}", e)))?;
                instructions.push(swap_ix);

                // Add cleanup instruction if present
                if let Some(cleanup_data) = &swap_ixs.cleanup_instruction {
                    match cleanup_data.to_instruction() {
                        Ok(ix) => instructions.push(ix),
                        Err(e) => warn!("Failed to parse cleanup instruction: {}", e),
                    }
                }

                info!("Got {} swap instructions", instructions.len());

                // Count total accounts
                let total_accounts = instructions
                    .iter()
                    .flat_map(|ix| ix.accounts.iter().map(|a| &a.pubkey))
                    .chain(accounts_distinct.iter().copied())
                    .collect::<HashSet<_>>();

                if total_accounts.len() <= MAX_ACCOUNTS_PER_TRANSACTION {
                    info!("Total accounts: {} (max {})", total_accounts.len(), MAX_ACCOUNTS_PER_TRANSACTION);

                    // Parse ALT addresses from response
                    let alt_addresses: Vec<Pubkey> = swap_ixs.address_lookup_table_addresses
                        .iter()
                        .filter_map(|s| s.parse().ok())
                        .collect();

                    info!("Metis returned {} lookup table addresses", alt_addresses.len());

                    // Fetch lookup tables (from cache or RPC)
                    let lookup_tables = if !alt_addresses.is_empty() {
                        let tables = fetch_lookup_tables(rpc_client, &alt_addresses, swap_alt_cache).await;
                        if tables.is_empty() { None } else { Some(tables) }
                    } else {
                        None
                    };

                    let route = SwapRoute {
                        in_amount: quote.in_amount_u64(),
                        out_amount: quote.out_amount_u64(),
                        slippage_bps: quote.slippage_bps,
                        price_impact_pct: quote.price_impact_pct.clone(),
                        address_lookup_tables: alt_addresses,
                    };

                    let tx = DecompiledVersionedTx {
                        instructions,
                        lookup_tables,
                    };

                    return Ok(SwapResult { route, tx });
                }
                info!("Too many accounts: {} > {}", total_accounts.len(), MAX_ACCOUNTS_PER_TRANSACTION);
            }
            Err(e) => {
                info!("No route found for {} {} -> {}: {:?}", amount, input_mint, output_mint, e);
                return Err(RoutingError::ApiError(e.to_string()));
            }
        }

        extra_accounts_buffer += 2;
    }

    Err(RoutingError::NoValidRoute)
}

/// Fetch lookup table accounts, serving from cache when possible.
async fn fetch_lookup_tables(
    rpc_client: &RpcClient,
    addresses: &[Pubkey],
    cache: &SwapAltCache,
) -> Vec<AddressLookupTableAccount> {
    let mut tables = Vec::new();
    let mut missing = Vec::new();

    // Check cache first
    {
        let cache_read = cache.inner.read().unwrap();
        for address in addresses {
            if let Some(table) = cache_read.get(address) {
                debug!("Swap ALT cache hit: {} ({} addresses)", address, table.addresses.len());
                tables.push(table.clone());
            } else {
                missing.push(*address);
            }
        }
    }

    if missing.is_empty() {
        return tables;
    }

    // Fetch missing from RPC
    info!("Fetching {} swap lookup tables from RPC: {:?}", missing.len(), missing);
    for address in &missing {
        match rpc_client.get_account(address).await {
            Ok(account) => {
                match AddressLookupTable::deserialize(&account.data) {
                    Ok(table) => {
                        let alt = AddressLookupTableAccount {
                            key: *address,
                            addresses: table.addresses.to_vec(),
                        };
                        info!("Loaded swap lookup table {} with {} addresses", address, alt.addresses.len());
                        cache.inner.write().unwrap().insert(*address, alt.clone());
                        tables.push(alt);
                    }
                    Err(e) => {
                        warn!("Failed to deserialize lookup table {}: {:?}", address, e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch lookup table {}: {:?}", address, e);
            }
        }
    }
    tables
}
