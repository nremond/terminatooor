use std::collections::{HashMap, HashSet};

use anchor_lang::prelude::Pubkey;
use kamino_lending::{LendingMarket, Reserve};

use crate::liquidator::Liquidator;

/// Collect keys for the lookup table.
///
/// We include only keys relevant to THIS market:
/// 1. Reserve pubkeys (needed for refresh instructions)
/// 2. Liquidator's ATAs for mints in this market only
/// 3. Lending market info
///
/// By only including ATAs for this market's reserves (not all 167 ATAs),
/// we keep the key count well under 256.
pub fn collect_keys(
    reserves: &HashMap<Pubkey, Reserve>,
    liquidator: &Liquidator,
    lending_market: &LendingMarket,
) -> HashSet<Pubkey> {
    let mut lending_markets = HashSet::new();
    let mut keys = HashSet::new();

    // Add all reserve pubkeys (needed for refresh instructions)
    for (pubkey, reserve) in reserves {
        keys.insert(*pubkey);
        lending_markets.insert(reserve.lending_market);
    }

    // Add liquidator ATAs only for mints in THIS market's reserves
    // This is ~2 ATAs per reserve (liquidity + collateral mint)
    {
        let atas = liquidator.atas.read().unwrap();
        for (_pubkey, reserve) in reserves {
            // Add ATA for liquidity mint (the actual token)
            if let Some(ata) = atas.get(&reserve.liquidity.mint_pubkey) {
                keys.insert(*ata);
            }
            // Add ATA for collateral mint (cToken) - needed for some operations
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

    // Total: reserves + (2 ATAs per reserve) + market info = much less than 256
    keys
}
