use std::collections::{HashMap, HashSet};

use anchor_lang::prelude::Pubkey;
use kamino_lending::{LendingMarket, Reserve, ReserveFarmKind};

use crate::liquidator::Liquidator;

/// Maximum addresses in a lookup table
const MAX_LUT_ADDRESSES: usize = 256;

/// Collect keys for the lookup table, prioritizing reserves with the most borrows.
///
/// We include keys relevant to THIS market:
/// 1. Reserve pubkeys and their vaults/mints
/// 2. Liquidator's ATAs for mints in this market
/// 3. Lending market authority
/// 4. Farm state accounts
/// 5. Oracle accounts (only scope - most commonly used)
///
/// To stay under the 256 key limit, we prioritize reserves by borrow amount.
pub fn collect_keys(
    reserves: &HashMap<Pubkey, Reserve>,
    liquidator: &Liquidator,
    _lending_market: &LendingMarket,
) -> HashSet<Pubkey> {
    let mut lending_markets = HashSet::new();
    let mut keys = HashSet::new();

    // Add common program IDs and sysvars first (always needed)
    keys.insert(anchor_spl::token::ID);
    keys.insert(anchor_spl::token_2022::ID);
    keys.insert(solana_sdk::system_program::ID);
    keys.insert(solana_sdk::sysvar::instructions::ID);
    keys.insert(solana_sdk::sysvar::rent::ID);
    keys.insert(farms::ID);

    // Sort reserves by borrow amount (descending) to prioritize active reserves
    let mut sorted_reserves: Vec<_> = reserves.iter().collect();
    sorted_reserves.sort_by(|a, b| {
        let a_borrows: u64 = a.1.liquidity.borrowed_amount_sf.try_into().unwrap_or(0);
        let b_borrows: u64 = b.1.liquidity.borrowed_amount_sf.try_into().unwrap_or(0);
        b_borrows.cmp(&a_borrows)
    });

    // Collect liquidator ATAs for quick lookup
    let atas = liquidator.atas.read().unwrap();

    // Add reserve accounts, stopping when we approach the limit
    for (pubkey, reserve) in sorted_reserves {
        // Estimate keys this reserve will add (reserve + 5 vaults/mints + ~2 farms + ~1 oracle + ~2 ATAs)
        let estimated_new_keys = 11;
        if keys.len() + estimated_new_keys > MAX_LUT_ADDRESSES {
            break;
        }

        keys.insert(*pubkey);
        lending_markets.insert(reserve.lending_market);

        // Reserve vaults and mints (used in flash loan and liquidation)
        keys.insert(reserve.liquidity.supply_vault);
        keys.insert(reserve.liquidity.fee_vault);
        keys.insert(reserve.liquidity.mint_pubkey);
        keys.insert(reserve.collateral.supply_vault);
        keys.insert(reserve.collateral.mint_pubkey);

        // Farm state accounts (used in refresh_obligation_farms)
        let debt_farm = reserve.get_farm(ReserveFarmKind::Debt);
        let coll_farm = reserve.get_farm(ReserveFarmKind::Collateral);
        if debt_farm != Pubkey::default() {
            keys.insert(debt_farm);
        }
        if coll_farm != Pubkey::default() {
            keys.insert(coll_farm);
        }

        // Oracle accounts - only add scope (most commonly used) to save space
        // Pyth and Switchboard are less commonly the primary oracle
        if reserve.config.token_info.scope_configuration.price_feed != Pubkey::default() {
            keys.insert(reserve.config.token_info.scope_configuration.price_feed);
        }

        // Add liquidator ATAs for this reserve's mints
        if let Some(ata) = atas.get(&reserve.liquidity.mint_pubkey) {
            keys.insert(*ata);
        }
        if let Some(ata) = atas.get(&reserve.collateral.mint_pubkey) {
            keys.insert(*ata);
        }
    }

    // Add lending market authorities (essential for transactions)
    for lending_market in lending_markets.iter() {
        let lending_market_authority =
            kamino_lending::utils::seeds::pda::lending_market_auth(lending_market);
        keys.insert(*lending_market);
        keys.insert(lending_market_authority);
    }

    // Note: lending_market_owner and risk_council are NOT added - they're not used in liquidations

    keys
}
