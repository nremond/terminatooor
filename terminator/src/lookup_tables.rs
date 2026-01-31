use std::collections::{HashMap, HashSet};

use anchor_lang::prelude::Pubkey;
use kamino_lending::{LendingMarket, Reserve, ReserveFarmKind};

use crate::liquidator::Liquidator;

/// Collect keys for the lookup table.
///
/// We include keys relevant to THIS market:
/// 1. Reserve pubkeys and their vaults/mints
/// 2. Liquidator's ATAs for mints in this market
/// 3. Lending market info
/// 4. Farm state accounts
/// 5. Oracle accounts
///
/// We stay under the 256 key limit by only including this market's reserves.
pub fn collect_keys(
    reserves: &HashMap<Pubkey, Reserve>,
    liquidator: &Liquidator,
    lending_market: &LendingMarket,
) -> HashSet<Pubkey> {
    let mut lending_markets = HashSet::new();
    let mut keys = HashSet::new();

    // Add all reserve pubkeys and their related accounts
    for (pubkey, reserve) in reserves {
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

        // Oracle accounts (used in refresh_reserve)
        if reserve.config.token_info.pyth_configuration.price != Pubkey::default() {
            keys.insert(reserve.config.token_info.pyth_configuration.price);
        }
        if reserve.config.token_info.switchboard_configuration.price_aggregator != Pubkey::default() {
            keys.insert(reserve.config.token_info.switchboard_configuration.price_aggregator);
        }
        if reserve.config.token_info.switchboard_configuration.twap_aggregator != Pubkey::default() {
            keys.insert(reserve.config.token_info.switchboard_configuration.twap_aggregator);
        }
        if reserve.config.token_info.scope_configuration.price_feed != Pubkey::default() {
            keys.insert(reserve.config.token_info.scope_configuration.price_feed);
        }
    }

    // Add liquidator ATAs for mints in THIS market's reserves
    {
        let atas = liquidator.atas.read().unwrap();
        for (_pubkey, reserve) in reserves {
            if let Some(ata) = atas.get(&reserve.liquidity.mint_pubkey) {
                keys.insert(*ata);
            }
            if let Some(ata) = atas.get(&reserve.collateral.mint_pubkey) {
                keys.insert(*ata);
            }
        }
    }

    // Add lending market info
    keys.insert(lending_market.lending_market_owner);
    keys.insert(lending_market.risk_council);

    for lending_market in lending_markets.iter() {
        let lending_market_authority =
            kamino_lending::utils::seeds::pda::lending_market_auth(lending_market);
        keys.insert(*lending_market);
        keys.insert(lending_market_authority);
    }

    // Add common program IDs and sysvars (these compress well in ALTs)
    keys.insert(anchor_spl::token::ID);
    keys.insert(anchor_spl::token_2022::ID);
    keys.insert(solana_sdk::system_program::ID);
    keys.insert(solana_sdk::sysvar::instructions::ID);
    keys.insert(solana_sdk::sysvar::rent::ID);
    keys.insert(farms::ID);

    keys
}
