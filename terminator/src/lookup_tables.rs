use std::collections::{HashMap, HashSet};

use anchor_lang::prelude::Pubkey;
use kamino_lending::{LendingMarket, Reserve};

use crate::liquidator::Liquidator;

/// Collect keys for the lookup table.
///
/// We prioritize accounts that are most commonly used:
/// 1. All reserve pubkeys (needed for refresh instructions)
/// 2. All reserve vaults and mints (needed for token operations)
/// 3. Liquidator's ATAs (needed for transfers)
/// 4. Lending market info
///
/// With 55 reserves * 6 accounts + 110 ATAs + overhead = 440+ keys,
/// we exceed the 256 limit. So we only include reserve pubkeys and vaults
/// for the most important reserves, plus all liquidator ATAs.
pub fn collect_keys(
    reserves: &HashMap<Pubkey, Reserve>,
    liquidator: &Liquidator,
    lending_market: &LendingMarket,
) -> HashSet<Pubkey> {
    let mut lending_markets = HashSet::new();
    let mut keys = HashSet::new();

    // Add all reserve pubkeys (needed for refresh instructions) - ~55 keys
    for (pubkey, reserve) in reserves {
        keys.insert(*pubkey);
        lending_markets.insert(reserve.lending_market);
    }

    // Add liquidator ATAs only (not mints - they're included in reserve data)
    // This is ~110 keys for the ATAs
    {
        let atas = liquidator.atas.read().unwrap();
        for (_mint, ata) in atas.iter() {
            keys.insert(*ata);
        }
    }

    // Add lending market info - ~5 keys
    keys.insert(lending_market.lending_market_owner);
    keys.insert(lending_market.risk_council);

    for lending_market in lending_markets.iter() {
        let lending_market_authority =
            kamino_lending::utils::seeds::pda::lending_market_auth(lending_market);
        keys.insert(*lending_market);
        keys.insert(lending_market_authority);
    }

    // Total should be around 170 keys, well under 256
    keys
}
