use std::collections::HashSet;

use anchor_lang::prelude::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::address_lookup_table::{state::AddressLookupTable, AddressLookupTableAccount};
use tracing::{info, warn};

use crate::consts::{
    EXTRA_ACCOUNTS_BUFFER, MAX_ACCOUNTS_PER_TRANSACTION, MAX_EXTRA_ACCOUNTS_BUFFER,
};
use crate::titan::{self, DecompiledVersionedTx, Error as TitanError, SwapRoute, TitanResult};

/// Swap result containing both quote info and instructions
#[derive(Debug)]
pub struct SwapResult {
    pub route: SwapRoute,
    pub tx: DecompiledVersionedTx,
}

pub async fn get_best_swap_route(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    only_direct_routes: bool,
    slippage_bps: Option<u16>,
    price_impact_limit: Option<f32>,
    max_accounts: Option<u8>,
    user_public_key: Pubkey,
) -> TitanResult<SwapRoute> {
    let best_route = titan::get_quote(
        input_mint,
        output_mint,
        amount,
        only_direct_routes,
        slippage_bps,
        max_accounts,
        user_public_key,
    )
    .await?;

    // Handle empty or invalid price_impact_pct (Titan sometimes returns empty string)
    let route_price_impact_pct = if best_route.price_impact_pct.is_empty() {
        0.0
    } else {
        best_route
            .price_impact_pct
            .parse::<f32>()
            .unwrap_or(0.0)
    };
    if let Some(price_impact_limit) = price_impact_limit {
        if route_price_impact_pct > price_impact_limit {
            return Err(TitanError::PriceImpactTooHigh(route_price_impact_pct));
        }
    }
    Ok(best_route)
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
) -> TitanResult<SwapResult> {
    // when we use swap + Kamino's swap rewards in a single transaction, the total number of unique accounts that we can lock (include) in the tx is 64. We need to count how many accounts the Kamino ix will use and require the API to give us a route that uses less than MAX_ACCOUNTS_PER_TRANSACTION - the amount of accounts that Kamino will use.
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
            "Trying swap route with max_accounts={} (buffer={})",
            max_accounts, extra_accounts_buffer
        );

        // First check if we can get a valid quote with these constraints
        let best_route = match get_best_swap_route(
            input_mint,
            output_mint,
            amount,
            only_direct_routes,
            slippage_bps,
            price_impact_limit,
            Some(max_accounts.try_into().unwrap()),
            user_public_key,
        )
        .await
        {
            Ok(res) => {
                info!("Got route: in={} out={}", res.in_amount, res.out_amount);
                Some(res)
            }
            Err(e) => {
                info!("No route found: {:?}", e);
                None
            }
        };

        if let Some(route) = best_route {
            info!("Getting swap instructions...");
            // Get the actual swap instructions
            let instructions_result = titan::get_swap_instructions(
                input_mint,
                output_mint,
                amount,
                slippage_bps,
                user_public_key,
            )
            .await;

            if let Ok(mut decompiled_tx) = instructions_result {
                info!("Got {} swap instructions", decompiled_tx.instructions.len());
                let total_accounts = decompiled_tx
                    .instructions
                    .iter()
                    .flat_map(|ix| ix.accounts.iter().map(|a| &a.pubkey))
                    .chain(accounts_distinct.iter().copied())
                    .collect::<HashSet<_>>();
                if total_accounts.len() <= MAX_ACCOUNTS_PER_TRANSACTION {
                    info!("max accounts {}", max_accounts);

                    // Fetch lookup tables from Titan's response
                    if !route.address_lookup_tables.is_empty() {
                        let swap_luts = fetch_lookup_tables(rpc_client, &route.address_lookup_tables).await;
                        if !swap_luts.is_empty() {
                            decompiled_tx.lookup_tables = Some(swap_luts);
                        }
                    }

                    return Ok(SwapResult { route, tx: decompiled_tx });
                }
            }
        } else {
            warn!("cannot find route from {input_mint} to {output_mint} for max_accounts {max_accounts}");
            return Err(TitanError::NoValidRoute);
        }

        extra_accounts_buffer += 2;
    }

    Err(TitanError::NoValidRoute)
}

/// Fetch lookup table accounts from RPC given their addresses
async fn fetch_lookup_tables(
    rpc_client: &RpcClient,
    addresses: &[Pubkey],
) -> Vec<AddressLookupTableAccount> {
    let mut tables = Vec::new();
    for address in addresses {
        match rpc_client.get_account(address).await {
            Ok(account) => {
                match AddressLookupTable::deserialize(&account.data) {
                    Ok(table) => {
                        tables.push(AddressLookupTableAccount {
                            key: *address,
                            addresses: table.addresses.to_vec(),
                        });
                        info!("Loaded swap lookup table {} with {} addresses", address, table.addresses.len());
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
