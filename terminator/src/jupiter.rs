use std::collections::HashSet;

use anchor_lang::prelude::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tracing::warn;

use crate::consts::{
    EXTRA_ACCOUNTS_BUFFER, MAX_ACCOUNTS_PER_TRANSACTION, MAX_EXTRA_ACCOUNTS_BUFFER,
};
use crate::titan::{self, DecompiledVersionedTx, Error as TitanError, SwapRoute, TitanResult};

pub async fn get_best_swap_route(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    only_direct_routes: bool,
    slippage_bps: Option<u16>,
    price_impact_limit: Option<f32>,
    max_accounts: Option<u8>,
) -> TitanResult<SwapRoute> {
    let best_route = titan::get_quote(
        input_mint,
        output_mint,
        amount,
        only_direct_routes,
        slippage_bps,
        max_accounts,
    )
    .await?;

    let route_price_impact_pct = best_route
        .price_impact_pct
        .parse::<f32>()
        .map_err(|_| TitanError::ResponseTypeConversionError)?;
    if let Some(price_impact_limit) = price_impact_limit {
        if route_price_impact_pct > price_impact_limit {
            return Err(TitanError::PriceImpactTooHigh(route_price_impact_pct));
        }
    }
    Ok(best_route)
}

#[allow(clippy::too_many_arguments)]
/// Get the swap instructions for the best route matching parameters
pub async fn get_best_swap_instructions(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    only_direct_routes: bool,
    slippage_bps: Option<u16>,
    price_impact_limit: Option<f32>,
    user_public_key: Pubkey,
    _rpc_client: &RpcClient,
    accounts: Option<&Vec<&Pubkey>>,
    accounts_count_buffer: Option<usize>,
) -> TitanResult<DecompiledVersionedTx> {
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

        // First check if we can get a valid quote with these constraints
        let best_route = match get_best_swap_route(
            input_mint,
            output_mint,
            amount,
            only_direct_routes,
            slippage_bps,
            price_impact_limit,
            Some(max_accounts.try_into().unwrap()),
        )
        .await
        {
            Ok(res) => Some(res),
            Err(_) => None,
        };

        if best_route.is_some() {
            // Get the actual swap instructions
            let instructions_result = titan::get_swap_instructions(
                input_mint,
                output_mint,
                amount,
                slippage_bps,
                user_public_key,
            )
            .await;

            if let Ok(decompiled_tx) = instructions_result {
                let total_accounts = decompiled_tx
                    .instructions
                    .iter()
                    .flat_map(|ix| ix.accounts.iter().map(|a| &a.pubkey))
                    .chain(accounts_distinct.iter().copied())
                    .collect::<HashSet<_>>();
                if total_accounts.len() <= MAX_ACCOUNTS_PER_TRANSACTION {
                    println!("max accounts {}", max_accounts);
                    return Ok(decompiled_tx);
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
