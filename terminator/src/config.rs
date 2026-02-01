use std::{path::PathBuf, str::FromStr, time::Duration};

use anchor_lang::prelude::Pubkey;
use anyhow::{anyhow, Result};
use orbit_link::OrbitLink;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{read_keypair_file, Keypair},
};

use crate::{
    client::{KlendClient, RebalanceConfig},
    Actions, Args, RebalanceArgs,
};

pub async fn get_lending_markets(client: &crate::client::KlendClient) -> Result<Vec<Pubkey>> {
    let env = std::env::var("MARKETS").ok();
    let markets = if let Some(markets) = env {
        let markets: Vec<Pubkey> = markets
            .split(',')
            .map(|s| Pubkey::from_str(s).unwrap())
            .collect();
        markets
    } else {
        // Fetch all markets dynamically from the program
        client.fetch_all_markets().await?
    };
    Ok(markets)
}

pub fn get_client_for_action(args: &Args) -> Result<KlendClient> {
    let (payer, placeholder) = get_keypair_for_action(&args.keypair)?;
    let commitment = CommitmentConfig::processed();
    let rpc = RpcClient::new_with_timeout_and_commitment(
        args.cluster.url().to_string(),
        Duration::from_secs(300),
        commitment,
    );
    let orbit_link: OrbitLink<RpcClient, Keypair> =
        OrbitLink::new(rpc, payer, None, commitment, placeholder)?;
    let rebalance_config = get_rebalance_config_for_action(&args.action);
    let klend_client = KlendClient::init(
        orbit_link,
        args.klend_program_id.unwrap_or(kamino_lending::id()),
        rebalance_config,
    )?;
    Ok(klend_client)
}

pub fn get_keypair_for_action(
    keypair: &Option<PathBuf>,
) -> Result<(Option<Keypair>, Option<Pubkey>)> {
    let (keypair, pubkey) = client_keypair_and_pubkey(keypair)?;
    validate_keypair_for_action(&keypair)?;
    Ok((keypair, pubkey))
}

pub fn client_keypair_and_pubkey(
    keypair: &Option<PathBuf>,
) -> Result<(Option<Keypair>, Option<Pubkey>)> {
    Ok(if let Some(key) = keypair {
        (
            Some(
                read_keypair_file(key.clone())
                    .map_err(|e| anyhow!("Keypair file {:?} not found or invalid {:?}", key, e))?,
            ),
            None,
        )
    } else {
        (
            None,
            Some(Pubkey::from_str(
                "K1endProducer111111111111111111111111111111",
            )?),
        )
    })
}

fn validate_keypair_for_action(keypair: &Option<Keypair>) -> Result<()> {
    if keypair.is_none() {
        return Err(anyhow::anyhow!("Keypair is required for this action"));
    }

    Ok(())
}

pub fn get_rebalance_config_for_action(action: &Actions) -> Option<RebalanceConfig> {
    match action {
        Actions::Liquidate { rebalance_args, .. }
        | Actions::Crank { rebalance_args, .. }
        | Actions::CrankStream { rebalance_args, .. }
        | Actions::Rebalance { rebalance_args, .. }
        | Actions::Swap { rebalance_args, .. }
        | Actions::TestFlashLiquidation { rebalance_args, .. } => Some(parse_rebalance_args(rebalance_args)),
    }
}

fn parse_rebalance_args(args: &RebalanceArgs) -> RebalanceConfig {
    RebalanceConfig {
        base_token: args.base_currency,
        min_sol_balance: args.min_sol_balance,
        usdc_mint: args.usdc_mint,
        rebalance_slippage_pct: args.rebalance_slippage_pct,
        non_swappable_dust_usd_value: args.non_swappable_dust_usd_value,
    }
}
