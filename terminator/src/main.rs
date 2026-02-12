use std::{collections::HashMap, panic::AssertUnwindSafe, path::PathBuf, sync::Arc, time::Duration};
use futures::FutureExt;

use anchor_client::{solana_sdk::pubkey::Pubkey, Cluster};
use anchor_lang::AccountDeserialize;
use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::Colorize;
use consts::WRAPPED_SOL_MINT;
use itertools::Itertools;
use crate::routing::DecompiledVersionedTx;
use bytemuck::try_from_bytes;
use kamino_lending::{Obligation, Reserve, ReserveFarmKind};
use solana_sdk::{
    compute_budget::{self},
    signer::Signer,
};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use tracing_subscriber::filter::EnvFilter;

use crate::{
    accounts::{map_accounts_and_create_infos, oracle_accounts, OracleAccounts},
    client::{KlendClient, RebalanceConfig},
    config::get_lending_markets,
    geyser::{GeyserConfig, GeyserStream},
    routing::{get_best_swap_instructions, SwapResult},
    liquidator::{Holding, Holdings},
    math::{LiquidationStrategy, Fraction},
    model::StateWithKey,
    operations::{
        obligation_reserves, referrer_token_states_of_obligation, split_obligations,
        ObligationReserves, SplitObligations,
    },
    px::fetch_prices,
    utils::get_all_reserve_mints,
};

pub mod accounts;
pub mod client;
mod config;
pub mod consts;
pub mod geyser;
pub mod instructions;
pub mod routing;
pub mod liquidator;
pub mod lookup_tables;
pub mod macros;
pub mod math;
pub mod metis;
mod model;
pub mod operations;
mod px;
pub mod sysvars;
pub mod titan;  // Keep for now, will remove after Metis is confirmed working
mod utils;
pub mod parallel;

const USDC_MINT_STR: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Klend program id
    /// Default is mainnet: KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD
    /// If compiled with staging profile, default is: SLendK7ySfcEzyaFqy93gDnD3RtrpXJcnRwb6zFHJSh
    #[clap(long, env, parse(try_from_str))]
    klend_program_id: Option<Pubkey>,

    /// Connect to solana validator
    #[clap(long, env, parse(try_from_str), default_value = "localnet")]
    cluster: Cluster,

    /// Account keypair to pay for the transactions
    #[clap(long, env, parse(from_os_str))]
    keypair: Option<PathBuf>,

    /// Markets to be considered (CLI only, use MARKETS env var for comma-separated list)
    /// Defaults to using MARKETS env var or fetching dynamically
    #[clap(long, parse(try_from_str))]
    markets: Option<Vec<Pubkey>>,

    /// Set flag to activate json log output
    #[clap(long, env = "JSON_LOGS")]
    json: bool,

    /// Print timestamps in logs (not needed on grafana)
    #[clap(long, env, default_value = "true")]
    log_timestamps: bool,

    /// Run with embedded webserver (default false)
    #[clap(short, env, long)]
    server: bool,

    /// Embedded webserver port
    /// Only valid if --server is also used
    #[clap(long, env, default_value = "8080")]
    server_port: u16,

    /// Helius LaserStream/Geyser endpoint URL
    /// Example: https://laserstream-mainnet-ewr.helius-rpc.com
    #[clap(long, env = "GEYSER_ENDPOINT")]
    geyser_endpoint: Option<String>,

    /// API key for Geyser/LaserStream authentication (Helius, Triton, etc.)
    #[clap(long, env = "GEYSER_API_KEY")]
    geyser_api_key: Option<String>,

    /// Subcommand to execute
    #[clap(subcommand)]
    action: Actions,
}

#[derive(Parser, Debug)]
pub struct RebalanceArgs {
    /// What to hold the balance in
    #[clap(long, env, parse(try_from_str), default_value = USDC_MINT_STR)]
    base_currency: Pubkey,

    /// Necessary for fees
    #[clap(long, env, parse(try_from_str), default_value = "0.5")]
    min_sol_balance: f64,

    /// Used for jup quote pxs etc.
    #[clap(long, env, parse(try_from_str), default_value = USDC_MINT_STR)]
    usdc_mint: Pubkey,

    /// From token
    #[clap(long, env, parse(try_from_str), default_value = "0.35")]
    rebalance_slippage_pct: f64,

    /// Threshold value to trigger a rebalance
    #[clap(long, env, parse(try_from_str), default_value = "5.0")]
    non_swappable_dust_usd_value: f64,
}

#[derive(Debug, Subcommand)]
pub enum Actions {
    /// Automatically refresh prices using RPC polling (legacy mode)
    #[clap()]
    Crank {
        /// Obligation to be cranked
        #[clap(long, env, parse(try_from_str))]
        obligation: Option<Pubkey>,

        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },
    /// Stream obligation updates via Geyser/LaserStream for real-time monitoring
    /// Requires --geyser-endpoint and --helius-api-key to be set
    #[clap()]
    CrankStream {
        #[clap(flatten)]
        rebalance_args: RebalanceArgs,

        /// How often to refresh market state (hours). Default: 4 hours.
        /// This is a fallback safety check; Geyser provides real-time updates.
        #[clap(long, env = "STATE_REFRESH_HOURS", default_value = "4")]
        state_refresh_hours: u64,

        /// Maximum number of concurrent liquidations. Default: 5.
        #[clap(long, env = "MAX_CONCURRENT_LIQUIDATIONS", default_value = "5")]
        max_concurrent: usize,

        /// Cooldown period (seconds) before retrying the same obligation. Default: 5.
        #[clap(long, env = "LIQUIDATION_COOLDOWN_SECS", default_value = "5")]
        cooldown_secs: u64,
    },
    #[clap()]
    Swap {
        /// From token
        #[clap(long, env, parse(try_from_str))]
        from: Pubkey,

        /// From token
        #[clap(long, env, parse(try_from_str))]
        to: Pubkey,

        /// From token
        #[clap(long, env, parse(try_from_str))]
        amount: f64,

        /// From token
        #[clap(long, env, parse(try_from_str))]
        slippage_pct: f64,

        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },

    #[clap()]
    Rebalance {
        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },

    /// Test flash loan liquidation by simulating a transaction for a specific obligation
    /// This is useful for debugging error 2502 without waiting for real opportunities
    #[clap()]
    TestFlashLiquidation {
        /// Obligation to test liquidation against (optional - will fetch one if not provided)
        #[clap(long, env, parse(try_from_str))]
        obligation: Option<Pubkey>,

        /// Market pubkey (optional, will be derived from obligation if not provided)
        #[clap(long, env, parse(try_from_str))]
        market: Option<Pubkey>,

        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },

    /// Profile instruction building latency for liquidations
    /// Runs multiple iterations to measure wrap_obligation_instruction_with_farms timing
    #[clap()]
    ProfileIxBuilding {
        /// Obligation to profile (must be a valid obligation with deposits and borrows)
        #[clap(long, env, parse(try_from_str))]
        obligation: Pubkey,

        /// Number of iterations to run (default: 3)
        #[clap(long, default_value = "3")]
        iterations: usize,

        #[clap(flatten)]
        rebalance_args: RebalanceArgs,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Ok(e) = std::env::var("ENV") {
        dotenvy::from_filename(e)?;
    } else if PathBuf::from(".env").exists() {
        dotenvy::from_filename(".env")?;
    };
    let args: Args = Args::parse();

    let env_filter = EnvFilter::from_default_env();
    let env_filter = env_filter.add_directive("kamino_lending=warn".parse()?);
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .compact()
        .init();

    info!("Starting with {:#?}", args);

    info!("Initializing client..");
    let klend_client = config::get_client_for_action(&args)?;
    let klend_client = Arc::new(klend_client);

    it_event!("klend_terminator::started");

    info!("Executing action..");
    match args.action {
        Actions::Crank {
            obligation: obligation_filter,
            rebalance_args: _,
        } => crank(&klend_client, obligation_filter).await,
        Actions::CrankStream { rebalance_args: _, state_refresh_hours, max_concurrent, cooldown_secs } => {
            let geyser_endpoint = args
                .geyser_endpoint
                .ok_or_else(|| anyhow::anyhow!("--geyser-endpoint is required for CrankStream"))?;
            let geyser_api_key = args
                .geyser_api_key
                .ok_or_else(|| anyhow::anyhow!("--geyser-api-key is required for CrankStream"))?;
            let parallel_config = parallel::ParallelConfig {
                max_concurrent,
                cooldown_duration: Duration::from_secs(cooldown_secs),
                ..Default::default()
            };
            crank_stream(klend_client.clone(), geyser_endpoint, geyser_api_key, state_refresh_hours, parallel_config).await
        }
        Actions::Swap {
            from,
            to,
            amount,
            slippage_pct,
            rebalance_args: _,
        } => swap::swap_action(&klend_client, from, to, amount, slippage_pct).await,
        Actions::Rebalance { rebalance_args: _ } => rebalance(&klend_client).await,
        Actions::TestFlashLiquidation {
            obligation,
            market,
            rebalance_args: _,
        } => test_flash_liquidation(&klend_client, obligation.as_ref(), market.as_ref()).await,
        Actions::ProfileIxBuilding {
            obligation,
            iterations,
            rebalance_args: _,
        } => profile_ix_building(&klend_client, &obligation, iterations).await,
    }
}

/// Test flash loan liquidation by building and simulating a transaction
/// This helps debug error 2502 without waiting for real liquidation opportunities
async fn test_flash_liquidation(
    klend_client: &Arc<KlendClient>,
    obligation_pubkey: Option<&Pubkey>,
    market_pubkey: Option<&Pubkey>,
) -> Result<()> {
    use base64::engine::general_purpose::STANDARD as BS64;
    use base64::Engine;

    info!("=== Testing Flash Loan Liquidation ===");

    // If no obligation provided, fetch markets and find one with deposits and borrows
    let (obligation_pubkey, obligation, market_key) = if let Some(ob_key) = obligation_pubkey {
        info!("Obligation: {}", ob_key);
        let obligation: Obligation = klend_client
            .client
            .get_anchor_account(ob_key)
            .await?;
        let market_key = market_pubkey.copied().unwrap_or(obligation.lending_market);
        (*ob_key, obligation, market_key)
    } else {
        info!("No obligation provided, searching for one with deposits and borrows...");

        // Fetch all markets
        let markets = klend_client.fetch_all_markets().await?;
        info!("Found {} markets", markets.len());

        let mut found_obligation = None;
        for market in markets.iter().take(3) {  // Check first 3 markets
            info!("Checking market {}...", market);
            match klend_client.fetch_obligations(market).await {
                Ok(obligations) => {
                    info!("Found {} obligations in market", obligations.len());
                    // Find obligation with both deposits and borrows
                    for (pubkey, ob) in obligations.iter().take(100) {  // Check first 100
                        let has_deposits = ob.deposits.iter().any(|d| d.deposit_reserve != Pubkey::default() && d.deposited_amount > 0);
                        let has_borrows = ob.borrows.iter().any(|b| b.borrow_reserve != Pubkey::default() && b.borrowed_amount_sf > 0);
                        if has_deposits && has_borrows {
                            info!("Found suitable obligation: {}", pubkey);
                            found_obligation = Some((*pubkey, ob.clone(), *market));
                            break;
                        }
                    }
                    if found_obligation.is_some() {
                        break;
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch obligations for market {}: {:?}", market, e);
                }
            }
        }

        found_obligation.ok_or_else(|| anyhow::anyhow!("No suitable obligation found"))?
    };

    info!("Using obligation: {}", obligation_pubkey);
    info!("Market: {}", market_key);

    // Load market and reserves
    info!("Loading market and reserves...");
    let market_accounts = klend_client.fetch_market_and_reserves(&market_key).await?;
    let lending_market = StateWithKey::new(market_accounts.lending_market, market_key);
    let reserves = &market_accounts.reserves;
    info!("Found {} reserves", reserves.len());

    // Load lookup table for this market (required for transaction size reduction)
    info!("Loading lookup table for market...");
    klend_client.load_lookup_table(&market_key, &market_accounts).await?;

    // Find deposits and borrows
    let deposits: Vec<_> = obligation
        .deposits
        .iter()
        .filter(|d| d.deposit_reserve != Pubkey::default() && d.deposited_amount > 0)
        .collect();
    let borrows: Vec<_> = obligation
        .borrows
        .iter()
        .filter(|b| b.borrow_reserve != Pubkey::default() && b.borrowed_amount_sf > 0)
        .collect();

    info!("Obligation has {} deposits and {} borrows", deposits.len(), borrows.len());

    if deposits.is_empty() || borrows.is_empty() {
        info!("No deposits or borrows found, cannot simulate liquidation");
        return Ok(());
    }

    // Print deposit/borrow details
    for deposit in &deposits {
        info!("  Deposit: reserve={} amount={}", deposit.deposit_reserve, deposit.deposited_amount);
    }
    for borrow in &borrows {
        let borrowed: u64 = (borrow.borrowed_amount_sf >> 60) as u64; // Approximate conversion from sf
        info!("  Borrow: reserve={} amount_sf={} (~{})", borrow.borrow_reserve, borrow.borrowed_amount_sf, borrowed);
    }

    // Use first deposit as collateral and first borrow as debt
    let coll_reserve_key = deposits[0].deposit_reserve;
    let debt_reserve_key = borrows[0].borrow_reserve;

    let coll_reserve_data: Reserve = reserves
        .get(&coll_reserve_key)
        .ok_or_else(|| anyhow::anyhow!("Collateral reserve not found"))?
        .clone();
    let debt_reserve_data: Reserve = reserves
        .get(&debt_reserve_key)
        .ok_or_else(|| anyhow::anyhow!("Debt reserve not found"))?
        .clone();

    let coll_reserve = StateWithKey::new(coll_reserve_data.clone(), coll_reserve_key);
    let debt_reserve = StateWithKey::new(debt_reserve_data.clone(), debt_reserve_key);

    info!("Using collateral reserve: {}", coll_reserve_key);
    info!("Using debt reserve: {}", debt_reserve_key);

    // Check farm configuration
    let coll_farm = coll_reserve_data.get_farm(ReserveFarmKind::Collateral);
    let debt_farm = debt_reserve_data.get_farm(ReserveFarmKind::Debt);
    info!("Collateral reserve farm (Collateral mode): {} (has_farm={})", coll_farm, coll_farm != Pubkey::default());
    info!("Debt reserve farm (Debt mode): {} (has_farm={})", debt_farm, debt_farm != Pubkey::default());

    // Ensure ATAs exist for all reserve mints before building instructions
    info!("Ensuring ATAs for reserves...");
    klend_client
        .liquidator
        .ensure_atas_for_reserves(klend_client, reserves)
        .await?;

    // Calculate a small test liquidation amount (1% of borrowed amount or 1 token)
    let borrowed_sf = borrows[0].borrowed_amount_sf;
    let borrowed_amount: u64 = (borrowed_sf >> 60) as u64;
    let liquidate_amount = std::cmp::max(borrowed_amount / 100, 1000); // At least 1000 base units
    info!("Test liquidation amount: {}", liquidate_amount);

    // Get debt token ATA
    let debt_mint = debt_reserve_data.liquidity.mint_pubkey;
    let coll_mint = coll_reserve_data.liquidity.mint_pubkey;
    info!("Debt mint: {}", debt_mint);
    info!("Collateral mint: {}", coll_mint);

    let debt_token_ata = {
        let atas = klend_client.liquidator.atas.read().unwrap();
        match atas.get(&debt_mint) {
            Some(ata) => *ata,
            None => {
                info!("No ATA for debt token, using derived address");
                spl_associated_token_account::get_associated_token_address(
                    &klend_client.liquidator.wallet.pubkey(),
                    &debt_mint,
                )
            }
        }
    };

    // Build flash loan liquidation transaction
    info!("\n=== Building Flash Loan Liquidation Transaction ===");

    let mut ixns = vec![];
    let mut luts = vec![];

    // 1. Flash borrow
    let debt_token_program = klend_client.liquidator.token_program_for_mint(&debt_mint);
    let flash_borrow_ix = instructions::flash_borrow_reserve_liquidity_ix(
        &klend_client.program_id,
        &lending_market.key,
        &debt_reserve,
        &debt_token_ata,
        &klend_client.liquidator,
        liquidate_amount,
        None,
        None,
        debt_token_program,
    );
    ixns.push(flash_borrow_ix.instruction);
    let flash_borrow_index = instructions::flash_borrow_instruction_index(0);

    // 2. Liquidation instructions
    let obligation_for_ix = StateWithKey::new(obligation.clone(), obligation_pubkey);
    info!("Building liquidation instructions with skip_post_farm_refresh=false...");
    let liquidate_ixns = klend_client
        .liquidate_obligation_and_redeem_reserve_collateral_ixns(
            lending_market.clone(),
            debt_reserve.clone(),
            coll_reserve.clone(),
            obligation_for_ix.clone(),
            liquidate_amount,
            0, // min_acceptable_received_coll_amount
            None, // max_allowed_ltv_override
            false, // skip_post_farm_refresh = false (include post-farm refresh)
            Some(reserves), // Pass cached reserves to avoid RPC calls
            None, // No farm cache - will fetch via RPC
        )
        .await?;

    info!("Liquidation instructions count: {}", liquidate_ixns.len());
    ixns.extend_from_slice(&liquidate_ixns);

    // 3. Swap instructions (if collateral != debt)
    // Skip swap for this test if METIS_ENDPOINT is not configured
    let _expected_collateral = liquidate_amount; // Simplified estimate
    if coll_mint != debt_mint && std::env::var("METIS_ENDPOINT").is_ok() {
        info!("Getting swap instructions: {} -> {}", coll_mint, debt_mint);
        let user = klend_client.liquidator.wallet.pubkey();
        let swap_result = get_best_swap_instructions(
            &coll_mint,
            &debt_mint,
            _expected_collateral,
            true, // only_direct_routes
            Some(100), // slippage_bps
            Some(5.0), // price_impact_limit
            user,
            &klend_client.client.client,
            None,
            None,
        )
        .await;

        match swap_result {
            Ok(result) => {
                let SwapResult { route: _, tx: swap_tx } = result;
                let DecompiledVersionedTx {
                    lookup_tables,
                    instructions: swap_ixs,
                } = swap_tx;

                // Filter compute budget ixns
                let swap_ixs: Vec<_> = swap_ixs
                    .into_iter()
                    .filter(|ix| ix.program_id != compute_budget::id())
                    .collect();

                info!("Swap instructions count: {}", swap_ixs.len());
                ixns.extend_from_slice(&swap_ixs);

                if let Some(lookup_tables) = lookup_tables {
                    for table in lookup_tables.into_iter() {
                        luts.push(table);
                    }
                }
            }
            Err(e) => {
                warn!("No swap route found: {:?}, proceeding without swap", e);
            }
        }
    } else if coll_mint != debt_mint {
        warn!("Skipping swap (METIS_ENDPOINT not configured) - this test will show instruction order but won't be executable");
    }

    // 4. Flash repay
    let flash_repay_ix = instructions::flash_repay_reserve_liquidity_ix(
        &klend_client.program_id,
        &lending_market.key,
        &debt_reserve,
        &debt_token_ata,
        &klend_client.liquidator,
        liquidate_amount,
        flash_borrow_index,
        None,
        None,
        debt_token_program,
    );
    ixns.push(flash_repay_ix.instruction);

    // Print instruction order
    info!("\n=== Transaction Instruction Order (before ComputeBudget prepend) ===");
    for (i, ix) in ixns.iter().enumerate() {
        let program_name = if ix.program_id == klend_client.program_id {
            "Kamino".to_string()
        } else if ix.program_id == solana_sdk::system_program::id() {
            "System".to_string()
        } else if ix.program_id == anchor_spl::token::ID {
            "Token".to_string()
        } else if ix.program_id == anchor_spl::token_2022::ID {
            "Token2022".to_string()
        } else if ix.program_id == spl_associated_token_account::ID {
            "ATA".to_string()
        } else if ix.program_id == compute_budget::id() {
            "ComputeBudget".to_string()
        } else {
            format!("{}...", &ix.program_id.to_string()[..8])
        };
        info!("  [{}] {} ({}) - {} accounts", i, program_name, ix.program_id, ix.accounts.len());
    }

    // Identify key positions
    let flash_borrow_pos = 0;
    let _liquidate_pos = liquidate_ixns.iter().position(|ix| {
        // Liquidate instruction has a specific discriminator
        ix.data.len() >= 8
    }).map(|p| p + 1); // +1 for flash_borrow
    info!("\nKey positions (before ComputeBudget):");
    info!("  flash_borrow: {}", flash_borrow_pos);
    info!("  liquidate_ixns start: 1");
    info!("  liquidate_ixns count: {}", liquidate_ixns.len());

    // Build transaction
    info!("\n=== Building Transaction ===");
    let mut txn = klend_client.client.tx_builder().add_ixs(ixns.clone());

    // Add lookup tables
    if let Some(liquidator_lut) = klend_client.get_lookup_table(&lending_market.key) {
        info!("Adding liquidator lookup table with {} addresses", liquidator_lut.addresses.len());
        txn = txn.add_lookup_table(liquidator_lut);
    }
    for lut in luts {
        txn = txn.add_lookup_table(lut);
    }

    let txn = txn.build_with_budget_and_fee(&[]).await?;

    // Check transaction size
    let txn_bytes = bincode::serialize(&txn).unwrap_or_default();
    let txn_size = txn_bytes.len();
    info!("Transaction size: {} bytes ({:.1}% of 1232 max)", txn_size, (txn_size as f64 / 1232.0) * 100.0);

    let txn_b64 = BS64.encode(&txn_bytes);
    info!("Simulation URL: https://explorer.solana.com/tx/inspector?message={}", urlencoding::encode(&txn_b64));

    // Simulate
    info!("\n=== Simulating Transaction ===");
    let res = klend_client.client.client.simulate_transaction(&txn).await;

    match res {
        Ok(sim_result) => {
            if let Some(err) = sim_result.value.err {
                error!("Simulation FAILED: {:?}", err);
                if let Some(logs) = sim_result.value.logs {
                    info!("Simulation logs:");
                    for log in logs {
                        info!("  {}", log);
                    }
                }
            } else {
                info!("Simulation SUCCEEDED!");
                info!("Compute units used: {:?}", sim_result.value.units_consumed);
            }
        }
        Err(e) => {
            error!("Simulation request failed: {:?}", e);
        }
    }

    info!("\n=== Test Complete ===");
    Ok(())
}

/// Profile instruction building latency
/// Runs multiple iterations of wrap_obligation_instruction_with_farms to measure timing
async fn profile_ix_building(
    klend_client: &Arc<KlendClient>,
    obligation_pubkey: &Pubkey,
    iterations: usize,
) -> Result<()> {
    info!("=== Profiling Instruction Building ===");
    info!("Obligation: {}", obligation_pubkey);
    info!("Iterations: {}", iterations);

    // Fetch obligation
    let obligation: Obligation = klend_client
        .client
        .get_anchor_account(obligation_pubkey)
        .await?;
    let market_key = obligation.lending_market;
    info!("Market: {}", market_key);

    // Load market and reserves
    let market_accounts = klend_client.fetch_market_and_reserves(&market_key).await?;
    let lending_market = StateWithKey::new(market_accounts.lending_market, market_key);
    let reserves = &market_accounts.reserves;
    info!("Found {} reserves", reserves.len());

    // Find deposits and borrows
    let deposits: Vec<_> = obligation
        .deposits
        .iter()
        .filter(|d| d.deposit_reserve != Pubkey::default() && d.deposited_amount > 0)
        .collect();
    let borrows: Vec<_> = obligation
        .borrows
        .iter()
        .filter(|b| b.borrow_reserve != Pubkey::default() && b.borrowed_amount_sf > 0)
        .collect();

    if deposits.is_empty() || borrows.is_empty() {
        return Err(anyhow::anyhow!("Obligation has no deposits or borrows"));
    }

    // Use first deposit as collateral and first borrow as debt
    let coll_reserve_key = deposits[0].deposit_reserve;
    let debt_reserve_key = borrows[0].borrow_reserve;

    let coll_reserve_data = reserves.get(&coll_reserve_key).ok_or_else(|| anyhow::anyhow!("Collateral reserve not found"))?.clone();
    let debt_reserve_data = reserves.get(&debt_reserve_key).ok_or_else(|| anyhow::anyhow!("Debt reserve not found"))?.clone();

    let coll_reserve = StateWithKey::new(coll_reserve_data, coll_reserve_key);
    let debt_reserve = StateWithKey::new(debt_reserve_data, debt_reserve_key);
    let obligation_state = StateWithKey::new(obligation.clone(), *obligation_pubkey);

    info!("Collateral reserve: {}", coll_reserve_key);
    info!("Debt reserve: {}", debt_reserve_key);

    // Ensure ATAs exist for all reserve mints before building instructions
    info!("Ensuring ATAs for reserves...");
    klend_client
        .liquidator
        .ensure_atas_for_reserves(klend_client, reserves)
        .await?;

    // Calculate test liquidation amount
    let borrowed_sf = borrows[0].borrowed_amount_sf;
    let borrowed_amount: u64 = (borrowed_sf >> 60) as u64;
    let liquidate_amount = std::cmp::max(borrowed_amount / 100, 1000);
    info!("Test liquidation amount: {}", liquidate_amount);

    // Pre-fetch farm user states once (simulating what liquidate_fast does)
    info!("Pre-fetching farm user states...");
    let prefetch_start = std::time::Instant::now();
    let debt_res_ref = debt_reserve.state.borrow();
    let coll_res_ref = coll_reserve.state.borrow();
    let reserves_for_prefetch: Vec<(&Pubkey, &Reserve)> = vec![
        (&debt_reserve_key, &*debt_res_ref),
        (&coll_reserve_key, &*coll_res_ref),
    ];
    let farm_cache = client::prefetch_farm_user_states(
        &klend_client.client.client,
        obligation_pubkey,
        &reserves_for_prefetch,
    ).await;
    drop(debt_res_ref);
    drop(coll_res_ref);
    info!("Pre-fetched {} farm user states in {}ms", farm_cache.len(), prefetch_start.elapsed().as_millis());

    info!("\n--- Running {} iterations (with farm cache) ---", iterations);
    let mut times = Vec::with_capacity(iterations);

    for i in 0..iterations {
        info!("\n=== Iteration {} ===", i + 1);
        let start = std::time::Instant::now();

        // This is what we're profiling - it calls wrap_obligation_instruction_with_farms internally
        let _liquidate_ixns = klend_client
            .liquidate_obligation_and_redeem_reserve_collateral_ixns(
                lending_market.clone(),
                debt_reserve.clone(),
                coll_reserve.clone(),
                obligation_state.clone(),
                liquidate_amount,
                1, // min_acceptable_received_coll_amount
                Some(10), // max_allowed_ltv_override_pct
                false, // skip_post_farm_refresh
                Some(reserves), // Pass cached reserves to avoid RPC calls
                Some(&farm_cache), // Pass pre-fetched farm user states
            )
            .await?;

        let elapsed = start.elapsed();
        info!("Iteration {} total: {}ms", i + 1, elapsed.as_millis());
        times.push(elapsed.as_millis() as u64);
    }

    // Print summary statistics
    info!("\n=== Summary (with all caches) ===");
    let avg = times.iter().sum::<u64>() / times.len() as u64;
    let min = times.iter().min().unwrap();
    let max = times.iter().max().unwrap();
    info!("Times: {:?}ms", times);
    info!("Average: {}ms", avg);
    info!("Min: {}ms, Max: {}ms", min, max);

    Ok(())
}

async fn rebalance(klend_client: &Arc<KlendClient>) -> Result<()> {
    let lending_markets = get_lending_markets(klend_client).await?;

    info!("Rebalancing...");
    let rebalance_config = match &klend_client.rebalance_config {
        None => Err(anyhow::anyhow!("Rebalance settings not found")),
        Some(c) => Ok(c),
    }?;
    let RebalanceConfig {
        base_token,
        min_sol_balance,
        rebalance_slippage_pct: slippage,
        ..
    } = rebalance_config;
    info!(
        "Loading markets and reserves for {} markets..",
        lending_markets.len()
    );
    let markets =
        crate::client::utils::fetch_markets_and_reserves(klend_client, &lending_markets).await?;
    let (all_reserves, _ctoken_mints, liquidity_mints) = get_all_reserve_mints(&markets);
    info!("Loading prices via Titan..");
    let amount = 100.0;
    let pxs = fetch_prices(&liquidity_mints, &rebalance_config.usdc_mint, amount).await?;
    info!("Loading holdings..");
    let mut holdings = klend_client
        .liquidator
        .fetch_holdings(&klend_client.client.client, &all_reserves, &pxs)
        .await?;

    let base = holdings.holding_of(base_token).unwrap();
    info!(
        "Base {:?} {} holding {}",
        base.mint, base.label, base.ui_balance
    );
    let sol_holding = &holdings.sol;
    info!(
        "SOL holding {}, Min sol holding {}",
        sol_holding.ui_balance, min_sol_balance
    );

    // Rules:
    // - if sol_balance < min_sol -> base token swaps into min_sol balance at least
    // - if sol_balance > min_sol * 2 -> swap the diff from current_sol - min_sol * 2 -> base token
    // - every non base token goes into base token if > $1 -> swap it partially at most $20k at a time

    const SOL_BUFFER_FACTOR: f64 = 2.0;
    let sol_balance = sol_holding.ui_balance;

    if sol_balance < *min_sol_balance {
        // Swap base token into SOL to reach min sol balance
        let target = min_sol_balance * SOL_BUFFER_FACTOR;
        let missing = target - sol_balance;

        let px_sol_to_base = pxs.a_to_b(&WRAPPED_SOL_MINT, base_token);
        let base_to_swap = missing * px_sol_to_base * (1.0 + slippage / 100.0);

        info!("Sol balance {} is below min_balance {} so we are topping up to {}, therefore acquiring {} more SOL, sol_price_to_base {}, swapping base {}",
            sol_balance,
            min_sol_balance,
            target,
            missing,
            px_sol_to_base,
            base_to_swap
        );

        // TODO: make these ixns go together
        swap::swap(
            klend_client,
            &holdings,
            base_token,
            &sol_holding.mint,
            base_to_swap,
            *slippage,
        )
        .await?;

        let _ = accounts::unwrap_wsol_ata(klend_client).await?;

        // Reload holdings
        tokio::time::sleep(Duration::from_secs(5)).await;
        holdings = klend_client
            .liquidator
            .fetch_holdings(&klend_client.client.client, &all_reserves, &pxs)
            .await?;
    }

    // TODO: If we have too much wsol and it's not the base asset
    // then just unwrap it
    // accounts::unwrap_wsol_ata(klend_client).await;
    if rebalance_config.base_token != WRAPPED_SOL_MINT {
        let wsol_holding = holdings.holding_of(&WRAPPED_SOL_MINT).unwrap();
        if wsol_holding.usd_value > 1.0 {
            info!("Unwrapping {} WSOL", wsol_holding.ui_balance);
            let _ = accounts::unwrap_wsol_ata(klend_client).await?;

            // Reload holdings
            tokio::time::sleep(Duration::from_secs(5)).await;
            klend_client
                .liquidator
                .fetch_holdings(&klend_client.client.client, &all_reserves, &pxs)
                .await?;
        }
    }

    // Now swap the remaining
    for Holding {
        mint,
        ui_balance,
        usd_value,
        label,
        ..
    } in holdings.holdings.clone().into_iter()
    {
        if &mint == base_token {
            continue;
        }

        if usd_value < rebalance_config.non_swappable_dust_usd_value {
            // We don't swap it, too small
            continue;
        }

        // Swap the whole thing
        let px = pxs.a_to_b(&mint, base_token);
        let estimated_base = ui_balance * px * (1.0 + slippage / 100.0);
        info!(
            "Swapping non-base token {} amount: {} expecting back {} base",
            label, ui_balance, estimated_base
        );

        swap::swap(
            klend_client,
            &holdings,
            &mint,
            base_token,
            ui_balance,
            *slippage,
        )
        .await?;
    }
    Ok(())
}

pub mod swap {
    use super::*;

    pub async fn swap_action(
        klend_client: &Arc<KlendClient>,
        from: Pubkey,
        to: Pubkey,
        amount: f64,
        slippage_pct: f64,
    ) -> Result<()> {
        let rebalance_config = match &klend_client.rebalance_config {
            None => Err(anyhow::anyhow!("Rebalance settings not found")),
            Some(c) => Ok(c),
        }?;

        let lending_markets = get_lending_markets(klend_client).await?;
        let markets =
            client::utils::fetch_markets_and_reserves(klend_client, &lending_markets).await?;
        let (reserves, _, l_mints) = get_all_reserve_mints(&markets);
        let pxs = fetch_prices(&l_mints, &rebalance_config.usdc_mint, amount as f32).await?;
        let holdings = klend_client
            .liquidator
            .fetch_holdings(&klend_client.client.client, &reserves, &pxs)
            .await?;
        swap(klend_client, &holdings, &from, &to, amount, slippage_pct).await
    }

    pub async fn swap(
        klend_client: &KlendClient,
        holdings: &Holdings,
        from: &Pubkey,
        to: &Pubkey,
        amount: f64,
        slippage_pct: f64,
    ) -> Result<()> {
        // Titan swap API quote request

        let from_token = holdings.holding_of(from)?;
        let to_token = holdings.holding_of(to)?;
        let user = klend_client.liquidator.wallet.pubkey();

        info!(
            "Swapping {} {} for {} with slippage {}%",
            amount,
            from_token.label.to_string().green(),
            to_token.label.to_string().green(),
            slippage_pct
        );

        let amount_to_swap = (amount * 10f64.powf(from_token.decimals as f64)).floor() as u64;
        let slippage_bps = (slippage_pct * 100f64).floor() as u16;

        let swap_result = get_best_swap_instructions(
            from,
            to,
            amount_to_swap,
            false,
            Some(slippage_bps),
            None,
            user,
            &klend_client.client.client,
            None,
            None,
        )
        .await?;

        info!(
            "Swap quote: in={} out={} (slippage={}bps)",
            swap_result.route.in_amount,
            swap_result.route.out_amount,
            swap_result.route.slippage_bps
        );

        let DecompiledVersionedTx {
            lookup_tables,
            instructions: jup_ixs,
        } = swap_result.tx;

        let mut builder = klend_client.client.tx_builder().add_ixs(jup_ixs);

        if let Some(lookup_tables) = lookup_tables {
            for table in lookup_tables.into_iter() {
                builder = builder.add_lookup_table(table);
            }
        }

        let tx = builder.build(&[]).await?;

        info!("Sending transaction...");
        let (sig, _) = klend_client
            .client
            .send_retry_and_confirm_transaction(tx, None, false)
            .await?;
        info!("Executed transaction: {:?}", sig);

        Ok(())
    }
}

/// Fast liquidation path for streaming mode - uses cached data to minimize RPC calls
/// Saves ~250ms by avoiding redundant fetches of obligation, reserves, referrer_token_states
/// Uses streamed oracle prices from Geyser to avoid additional RPC latency
///
/// `log_prefix` is used for structured logging (e.g., "[T1:8xBnR5kd]")
async fn liquidate_fast(
    klend_client: &KlendClient,
    obligation_pubkey: &Pubkey,
    obligation: Obligation,
    market_state: &MarketState,
    slot: u64,
    _ltv_margin_pct: f64,
    log_prefix: &str,
    oracle_cache: &geyser::OracleCache,
) -> Result<()> {
    info!("{} Liquidating obligation (fast path)", log_prefix);
    let rebalance_config = match &klend_client.rebalance_config {
        None => return Err(anyhow::anyhow!("Rebalance settings not found")),
        Some(c) => c,
    };

    // Start with cached reserves
    let mut ob = obligation;
    let mut reserves = market_state.reserves.clone();
    let market = &market_state.lending_market;
    let rts = &market_state.referrer_token_states;

    // Collect reserve keys referenced by this obligation (deposits + borrows)
    let mut obligation_reserve_keys: Vec<Pubkey> = Vec::new();
    for deposit in ob.deposits.iter() {
        if deposit.deposit_reserve != Pubkey::default() && !obligation_reserve_keys.contains(&deposit.deposit_reserve) {
            obligation_reserve_keys.push(deposit.deposit_reserve);
        }
    }
    for borrow in ob.borrows.iter() {
        if borrow.borrow_reserve != Pubkey::default() && !obligation_reserve_keys.contains(&borrow.borrow_reserve) {
            obligation_reserve_keys.push(borrow.borrow_reserve);
        }
    }

    // Fetch fresh reserves in a single batch RPC call to avoid stale cumulative_borrow_rate
    // This prevents NegativeInterestRate errors when obligation was refreshed more recently than cached reserves
    info!("{} Fetching fresh {} reserves (batch)", log_prefix, obligation_reserve_keys.len());
    let reserve_accounts = klend_client
        .client
        .client
        .get_multiple_accounts(&obligation_reserve_keys)
        .await
        .unwrap_or_default();
    for (i, account_opt) in reserve_accounts.iter().enumerate() {
        let reserve_key = &obligation_reserve_keys[i];
        if let Some(account) = account_opt {
            match Reserve::try_deserialize(&mut account.data.as_slice()) {
                Ok(fresh_reserve) => {
                    reserves.insert(*reserve_key, fresh_reserve);
                }
                Err(e) => {
                    warn!("{} Failed to deserialize reserve {}: {:?}, using cached", log_prefix, reserve_key, e);
                }
            }
        } else {
            warn!("{} Reserve {} not found, using cached", log_prefix, reserve_key);
        }
    }
    debug!("{} Using {} reserves for market {}", log_prefix, reserves.len(), ob.lending_market);

    // Use the maximum slot from all sources plus a buffer to ensure clock is not behind any data.
    // RPC slot can lag behind Geyser or fetched data due to different endpoints/caching.
    // The buffer accounts for this lag and prevents "reserve stale" errors.
    let rpc_slot = klend_client.client.client.get_slot().await.unwrap_or(slot);
    let clock_slot = calculate_clock_slot(rpc_slot, slot);
    let clock = solana_sdk::clock::Clock {
        slot: clock_slot,
        epoch_start_timestamp: 0,
        epoch: 0,
        leader_schedule_epoch: 0,
        unix_timestamp: 0,
    };

    // Pick debt and coll reserves to liquidate
    let debt_res_key = ob.borrows
        .iter()
        .find(|b| b.borrow_reserve != Pubkey::default())
        .ok_or_else(|| anyhow::anyhow!("No valid borrow reserves in obligation"))?
        .borrow_reserve;
    let coll_res_key = ob.deposits
        .iter()
        .find(|d| d.deposit_reserve != Pubkey::default())
        .ok_or_else(|| anyhow::anyhow!("No valid deposit reserves in obligation"))?
        .deposit_reserve;
    debug!("{} debt_reserve={} coll_reserve={}", log_prefix, debt_res_key, coll_res_key);

    // Refresh reserves and obligation using cached oracle data (no RPC calls!)
    operations::refresh_reserves_and_obligation_with_cache(
        obligation_pubkey,
        &mut ob,
        &mut reserves,
        rts,
        market,
        &clock,
        oracle_cache,
    )?;

    // Now it's all fully refreshed and up to date
    let debt_reserve_state = *reserves.get(&debt_res_key).ok_or_else(|| {
        anyhow::anyhow!("Debt reserve {} not found in reserves map", debt_res_key)
    })?;
    let coll_reserve_state = *reserves.get(&coll_res_key).ok_or_else(|| {
        anyhow::anyhow!("Collateral reserve {} not found in reserves map", coll_res_key)
    })?;
    let debt_mint = debt_reserve_state.liquidity.mint_pubkey;
    let debt_reserve = StateWithKey::new(debt_reserve_state, debt_res_key);
    let coll_reserve = StateWithKey::new(coll_reserve_state, coll_res_key);
    let lending_market = StateWithKey::new(*market, ob.lending_market);
    let obligation_state = StateWithKey::new(ob, *obligation_pubkey);
    // Skip slow price fetch - holdings aren't used in decide_liquidation_strategy for flash loans
    let holdings = Holdings::default();

    let deposit_reserves: Vec<StateWithKey<Reserve>> = ob
        .deposits
        .iter()
        .filter(|coll| coll.deposit_reserve != Pubkey::default())
        .map(|coll| {
            StateWithKey::new(
                *reserves.get(&coll.deposit_reserve).unwrap(),
                coll.deposit_reserve,
            )
        })
        .collect();

    let max_allowed_ltv_override_pct_opt = Some(10);
    let liquidation_swap_slippage_pct = 0.5;
    let min_acceptable_received_collateral_amount = 1;
    let liquidation_strategy = math::decide_liquidation_strategy(
        &rebalance_config.base_token,
        &obligation_state,
        &lending_market,
        &coll_reserve,
        &debt_reserve,
        &clock,
        max_allowed_ltv_override_pct_opt,
        liquidation_swap_slippage_pct,
        holdings,
    )?;

    let (liquidate_amount, expected_collateral) = match liquidation_strategy {
        Some(LiquidationStrategy::FlashLoanLiquidate(liquidate_amount, expected_collateral)) => {
            (liquidate_amount, expected_collateral)
        }
        Some(LiquidationStrategy::LiquidateAndRedeem(liquidate_amount)) => (liquidate_amount, 0),
        Some(LiquidationStrategy::SwapThenLiquidate(_, liquidate_amount)) => (liquidate_amount, 0),
        None => {
            info!("{} No liquidation strategy available", log_prefix);
            return Ok(());
        }
    };

    // Save original obligation for building instructions (simulation will mutate it)
    let original_obligation = *obligation_state.state.borrow();

    // Simulate liquidation (this mutates obligation_state)
    let res = kamino_lending::lending_market::lending_operations::liquidate_and_redeem(
        &lending_market.state.borrow(),
        &debt_reserve,
        &coll_reserve,
        &mut obligation_state.state.borrow_mut(),
        &clock,
        liquidate_amount,
        min_acceptable_received_collateral_amount,
        max_allowed_ltv_override_pct_opt,
        deposit_reserves.into_iter(),
    );

    debug!("{} Simulating the liquidation {:#?}", log_prefix, res);

    // Use original obligation for instructions (before simulation mutated it)
    let obligation_for_ix = StateWithKey::new(original_obligation, *obligation_pubkey);

    if res.is_ok() {
        let user = klend_client.liquidator.wallet.pubkey();
        let coll_mint = coll_reserve.state.borrow().liquidity.mint_pubkey;

        let mut ixns = vec![];
        let mut luts = vec![];

        // Get user's ATA for debt token
        let debt_token_ata = {
            let atas = klend_client.liquidator.atas.read().unwrap();
            *atas.get(&debt_mint).ok_or_else(|| anyhow::anyhow!("No ATA for debt mint {}", debt_mint))?
        };

        // Calculate flash loan fee and repay amount
        let flash_loan_fee_sf = debt_reserve.state.borrow().config.fees.flash_loan_fee_sf;
        let fee = (liquidate_amount as u128 * flash_loan_fee_sf as u128 / 1_000_000_000_000_000_000) as u64;
        let repay_amount = liquidate_amount + fee + 1; // +1 for rounding

        // 1. Flash borrow instruction
        let debt_token_program = klend_client.liquidator.token_program_for_mint(&debt_mint);
        let flash_borrow_ix = instructions::flash_borrow_reserve_liquidity_ix(
            &klend_client.program_id,
            &lending_market.key,
            &debt_reserve,
            &debt_token_ata,
            &klend_client.liquidator,
            liquidate_amount,
            None,
            None,
            debt_token_program,
        );

        ixns.push(flash_borrow_ix.instruction);
        // Flash borrow is at index 0 in our list, but build_with_budget_and_fee prepends ComputeBudget ixs
        let flash_borrow_index = instructions::flash_borrow_instruction_index(0);

        // 2. Build liquidation instructions AND get swap quote in PARALLEL
        // This saves ~200-350ms by overlapping the instruction building RPC calls with the Metis API call
        let parallel_start = std::time::Instant::now();

        // Pre-fetch farm user states to eliminate ~60ms RPC latency during instruction building
        let debt_res_ref = debt_reserve.state.borrow();
        let coll_res_ref = coll_reserve.state.borrow();
        let reserves_for_prefetch: Vec<(&Pubkey, &Reserve)> = vec![
            (&debt_res_key, &*debt_res_ref),
            (&coll_res_key, &*coll_res_ref),
        ];
        let farm_cache = client::prefetch_farm_user_states(
            &klend_client.client.client,
            obligation_pubkey,
            &reserves_for_prefetch,
        ).await;
        drop(debt_res_ref);
        drop(coll_res_ref);
        debug!("{} Pre-fetched {} farm user states in {}ms", log_prefix, farm_cache.len(), parallel_start.elapsed().as_millis());

        let log_prefix_owned = log_prefix.to_string();
        let reserves_for_cache = reserves.clone();
        let liquidate_ixns_future = async {
            let start = std::time::Instant::now();
            let result = klend_client.liquidate_obligation_and_redeem_reserve_collateral_ixns(
                lending_market.clone(),
                debt_reserve.clone(),
                coll_reserve.clone(),
                obligation_for_ix.clone(),
                liquidate_amount,
                min_acceptable_received_collateral_amount,
                max_allowed_ltv_override_pct_opt,
                false, // Must include post-farm refresh for on-chain validation
                Some(&reserves_for_cache), // Pass cached reserves to avoid RPC calls
                Some(&farm_cache), // Pass pre-fetched farm user states to avoid RPC calls
            ).await;
            debug!("{} Liquidation ixns built in {}ms", log_prefix_owned, start.elapsed().as_millis());
            result
        };

        // Only fetch swap if collateral != debt (need to swap back)
        let needs_swap = coll_mint != debt_mint;
        let log_prefix_owned2 = log_prefix.to_string();
        let swap_future = async {
            if needs_swap {
                let start = std::time::Instant::now();
                let result = get_best_swap_instructions(
                    &coll_mint,
                    &debt_mint,
                    expected_collateral,
                    true, // only_direct_routes
                    Some(100), // slippage_bps
                    Some(5.0), // price_impact_limit
                    user,
                    &klend_client.client.client,
                    None,
                    None,
                ).await;
                debug!("{} Swap quote fetched in {}ms", log_prefix_owned2, start.elapsed().as_millis());
                Some(result)
            } else {
                None
            }
        };

        // Run both in parallel
        let (liquidate_ixns_result, swap_result_opt) = tokio::join!(liquidate_ixns_future, swap_future);
        info!("{} Parallel build completed in {}ms", log_prefix, parallel_start.elapsed().as_millis());

        // Handle liquidation instructions result
        let liquidate_ixns = liquidate_ixns_result?;
        ixns.extend_from_slice(&liquidate_ixns);

        // 3. Handle swap result (if we needed a swap)
        if needs_swap {
            let swap_result = match swap_result_opt.unwrap() {
                Ok(result) => result,
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    if err_str.contains("TOKEN_NOT_TRADABLE") {
                        warn!(
                            "{} Collateral {} is an LP/kToken (not tradable on DEX), skipping flash loan liquidation",
                            log_prefix, coll_mint
                        );
                    } else {
                        warn!(
                            "{} No swap route found from {} to {}: {:?}, skipping liquidation",
                            log_prefix, coll_mint, debt_mint, e
                        );
                    }
                    return Ok(());
                }
            };

            // PROFITABILITY CHECK
            let swap_out_amount = swap_result.route.out_amount;
            info!(
                "{} Swap quote: in={} out={} (need {} to repay flash loan)",
                log_prefix, swap_result.route.in_amount, swap_out_amount, repay_amount
            );

            if swap_out_amount < repay_amount {
                let loss = repay_amount - swap_out_amount;
                warn!(
                    "{} UNPROFITABLE: Swap output {} < repay amount {}. Would lose {} debt tokens. Skipping.",
                    log_prefix, swap_out_amount, repay_amount, loss
                );
                return Ok(());
            }

            let profit = swap_out_amount - repay_amount;
            info!(
                "{} PROFITABLE: Expected profit = {} debt tokens (swap_out={} - repay={})",
                log_prefix, profit, swap_out_amount, repay_amount
            );

            let SwapResult { route: _, tx: swap_tx } = swap_result;
            let DecompiledVersionedTx {
                lookup_tables,
                instructions: swap_ixs,
            } = swap_tx;

            // Filter compute budget ixns
            let swap_ixs = swap_ixs
                .into_iter()
                .filter(|ix| ix.program_id != compute_budget::id())
                .collect_vec();

            ixns.extend_from_slice(&swap_ixs);

            if let Some(lookup_tables) = lookup_tables {
                for table in lookup_tables.into_iter() {
                    luts.push(table);
                }
            }
        }

        // 4. Flash repay (instruction data must match borrow amount exactly; fee is handled by token transfer)
        let flash_repay_ix = instructions::flash_repay_reserve_liquidity_ix(
            &klend_client.program_id,
            &lending_market.key,
            &debt_reserve,
            &debt_token_ata,
            &klend_client.liquidator,
            liquidate_amount, // Must match borrow amount - fee is added automatically
            flash_borrow_index,
            None,
            None,
            debt_token_program,
        );
        ixns.push(flash_repay_ix.instruction);

        info!(
            "{} Flash loan liquidation: borrow={}, repay={} (fee={}), expected_collateral={}",
            log_prefix,
            liquidate_amount,
            repay_amount,
            repay_amount - liquidate_amount,
            expected_collateral
        );

        // Debug: print instruction program IDs to diagnose error 2502
        debug!("{} Transaction instruction order (before ComputeBudget prepend):", log_prefix);
        for (i, ix) in ixns.iter().enumerate() {
            let program_name = if ix.program_id == klend_client.program_id {
                "Kamino".to_string()
            } else if ix.program_id == solana_sdk::system_program::id() {
                "System".to_string()
            } else if ix.program_id == anchor_spl::token::ID {
                "Token".to_string()
            } else if ix.program_id == anchor_spl::token_2022::ID {
                "Token2022".to_string()
            } else if ix.program_id == spl_associated_token_account::ID {
                "ATA".to_string()
            } else {
                format!("{}...", &ix.program_id.to_string()[..8])
            };
            debug!("{}   [{}] {} ({})", log_prefix, i, program_name, ix.program_id);
        }
        debug!("{}   Note: liquidate_ixns count = {}", log_prefix, liquidate_ixns.len());

        // Build transaction with lookup tables
        let swap_luts_count = luts.len();
        let mut txn = klend_client.client.tx_builder().add_ixs(ixns.clone());

        // Add liquidator lookup table
        let mut total_luts = 0;
        if let Some(liquidator_lut) = klend_client.get_lookup_table(&lending_market.key) {
            debug!("{} Adding liquidator lookup table with {} addresses", log_prefix, liquidator_lut.addresses.len());
            txn = txn.add_lookup_table(liquidator_lut);
            total_luts += 1;
        }

        // Add swap lookup tables
        for lut in luts {
            txn = txn.add_lookup_table(lut);
            total_luts += 1;
        }

        // Build the versioned transaction with lookup tables first
        let txn = match txn.build_with_budget_and_fee(&[]).await {
            Ok(t) => t,
            Err(e) => {
                warn!("{} Failed to build transaction: {:?}", log_prefix, e);
                return Ok(());
            }
        };

        // Check transaction size using the actual serialized versioned transaction
        let txn_bytes = bincode::serialize(&txn).unwrap_or_default();
        let txn_size = txn_bytes.len();
        const MAX_TX_SIZE: usize = 1232; // Solana raw transaction limit

        if txn_size > MAX_TX_SIZE {
            warn!(
                "{} Transaction too large: {} bytes (max {}). Skipping liquidation.",
                log_prefix, txn_size, MAX_TX_SIZE
            );
            debug!(
                "{} Debug: {} instructions, {} lookup tables ({} liquidator, {} swap)",
                log_prefix,
                ixns.len(),
                total_luts,
                if klend_client.get_lookup_table(&lending_market.key).is_some() { 1 } else { 0 },
                swap_luts_count
            );
            return Ok(());
        }

        debug!(
            "{} Transaction size: {} bytes ({:.1}% of max)",
            log_prefix,
            txn_size,
            (txn_size as f64 / MAX_TX_SIZE as f64) * 100.0
        );

        // Skip simulation and submit directly for faster execution
        debug!("{} Skipping simulation for faster execution", log_prefix);

        let should_send = true;

        if should_send {
            match klend_client
                .client
                .send_retry_and_confirm_transaction(txn, None, false)
                .await
            {
                Ok(sig) => {
                    info!("{} ✓ Liquidation tx sent: {:?}", log_prefix, sig.0);
                    debug!("{} Liquidation tx res: {:?}", log_prefix, sig.1);
                }
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    // Parse common Kamino error codes for better logging
                    if err_str.contains("Custom(6016)") {
                        info!("{} ✗ Lost race: obligation already liquidated (6016)", log_prefix);
                    } else if err_str.contains("Custom(6009)") {
                        warn!("{} ✗ Reserve stale - needs refresh (6009)", log_prefix);
                    } else if err_str.contains("Custom(6023)") {
                        warn!("{} ✗ Obligation stale - needs refresh (6023)", log_prefix);
                    } else if err_str.contains("Custom(6015)") {
                        info!("{} ✗ Liquidation amount too small (6015)", log_prefix);
                    } else {
                        warn!("{} ✗ Liquidation tx failed: {:?}", log_prefix, e);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn crank(klend_client: &KlendClient, obligation_filter: Option<Pubkey>) -> Result<()> {
    let sleep_duration = Duration::from_secs(10);
    let (markets, ob) = match obligation_filter {
        None => {
            let lending_markets = get_lending_markets(klend_client).await?;
            info!("Cranking all markets {lending_markets:?}..");
            (lending_markets, None)
        }
        Some(filter) => {
            let ob = klend_client.fetch_obligation(&filter).await?;
            let market = ob.lending_market;
            (vec![market], Some(ob))
        }
    };

    loop {
        for market in &markets {
            info!("{} cranking market", market.to_string().green());
            let st = std::time::Instant::now();

            let start = std::time::Instant::now();

            // Reload accounts
            let obligations = match ob {
                None => {
                    let obs = klend_client.fetch_obligations(market).await?;
                    info!(
                        "Fetched {} obligations in {}s",
                        obs.len(),
                        start.elapsed().as_secs()
                    );
                    obs
                }
                Some(o) => vec![(obligation_filter.unwrap(), o)],
            };
            let market_accs = klend_client.fetch_market_and_reserves(market).await?;
            let rts = klend_client.fetch_referrer_token_states().await?;
            info!("Market accounts fetched in {}s", start.elapsed().as_secs());

            let mut reserves = market_accs.reserves.clone();
            let lending_market = market_accs.lending_market;
            // let obligations = obligations.clone();

            let OracleAccounts {
                mut pyth_accounts,
                mut switchboard_accounts,
                mut scope_price_accounts,
            } = oracle_accounts(&klend_client.client, &reserves)
                .await
                .unwrap();

            let pyth_account_infos = map_accounts_and_create_infos(&mut pyth_accounts);
            let switchboard_feed_infos = map_accounts_and_create_infos(&mut switchboard_accounts);
            let scope_price_infos = map_accounts_and_create_infos(&mut scope_price_accounts);

            let clock = sysvars::get_clock(&klend_client.client.client)
                .await
                .unwrap();

            // Refresh all reserves first
            for (key, reserve) in reserves.iter_mut() {
                info!(
                    "Refreshing reserve {} token {} with status {}",
                    key.to_string().green(),
                    reserve.config.token_info.symbol().purple(),
                    reserve.config.status
                );
                // if reserve.config.status != ReserveStatus::Active as u8 {
                //     continue;
                // }
                let ignore_tokens = ["EURC", "CHAI"];
                if ignore_tokens.contains(&reserve.config.token_info.symbol()) {
                    continue;
                }
                if let Err(e) = reserve.last_update.slots_elapsed(clock.slot) {
                    warn!(err = ?e,
                        "RESERVE {:?} last updated slot is already ahead of the clock, skipping refresh",
                        key,
                    );
                } else {
                    operations::refresh_reserve(
                        key,
                        reserve,
                        &lending_market,
                        &clock,
                        &pyth_account_infos,
                        &switchboard_feed_infos,
                        &scope_price_infos,
                    )?;
                }
            }

            // Refresh all obligations second
            let SplitObligations {
                zero_debt,
                mut risky,
            } = split_obligations(&obligations);
            let num_obligations = risky.len();

            info!("Total obligations: {}", risky.len() + zero_debt.len());
            info!("Zero debt obligations: {}", zero_debt.len());
            info!("Risky obligations: {}", risky.len());

            let mut healthy_obligations = 0;
            let mut unhealthy_obligations = 0;
            for (i, (address, obligation)) in risky.iter_mut().enumerate() {
                // Apply the filter
                if let Some(obligation_filter) = obligation_filter {
                    if *address != obligation_filter {
                        continue;
                    }
                }
                info!("Processing obligation {:?}", address);

                // Refresh the obligation
                let ObligationReserves {
                    deposit_reserves,
                    borrow_reserves,
                } = obligation_reserves(obligation, &reserves)?;
                let referrer_states = referrer_token_states_of_obligation(
                    address,
                    obligation,
                    &borrow_reserves,
                    &rts,
                )?;
                kamino_lending::lending_market::lending_operations::refresh_obligation(
                    obligation,
                    &lending_market,
                    clock.slot,
                    deposit_reserves.into_iter(),
                    borrow_reserves.into_iter(),
                    referrer_states.into_iter(),
                )?;

                info!("Refreshed obligation: {}", address.to_string().green());
                let obligation_stats = math::obligation_info(address, obligation);
                math::print_obligation_stats(&obligation_stats, address, i, num_obligations);

                if obligation_stats.ltv > obligation_stats.unhealthy_ltv {
                    unhealthy_obligations += 1;
                } else {
                    healthy_obligations += 1;
                }
            }

            let en = st.elapsed().as_secs_f64();
            info!(
                "{} evaluated {} total obligations {} with debt, {} healthy, {} unhealthy. Sleeping for {:?}, duration {:?}", market.to_string().green(), risky.len() + zero_debt.len(), num_obligations, healthy_obligations, unhealthy_obligations, sleep_duration, en
            );
        }
        sleep(sleep_duration).await;
    }
}

/// Stream-based crank using Geyser/LaserStream for real-time obligation monitoring.
/// This is significantly faster than RPC polling as it receives updates within ~50-100ms
/// of on-chain state changes.
async fn crank_stream(
    klend_client: Arc<KlendClient>,
    geyser_endpoint: String,
    geyser_api_key: String,
    state_refresh_hours: u64,
    parallel_config: parallel::ParallelConfig,
) -> Result<()> {
    info!(
        "Starting stream-based crank with Geyser/LaserStream (state refresh every {} hours, max {} concurrent)",
        state_refresh_hours, parallel_config.max_concurrent
    );

    // Get lending markets to monitor
    let lending_markets = get_lending_markets(&klend_client).await?;
    info!("Monitoring {} markets: {:?}", lending_markets.len(), lending_markets);

    // Load initial state for all markets BEFORE connecting to Geyser
    // This prevents the Geyser channel from filling up during initialization
    let mut market_states: HashMap<Pubkey, MarketState> = HashMap::new();
    for market in &lending_markets {
        info!("Loading initial state for market {}", market.to_string().green());
        match load_market_state(&klend_client, market, true).await {
            Ok(state) => {
                // Ensure ATAs exist for all reserve mints
                if let Err(e) = klend_client
                    .liquidator
                    .ensure_atas_for_reserves(&klend_client, &state.reserves)
                    .await
                {
                    warn!("Failed to ensure ATAs for market {}: {:?}", market, e);
                }
                market_states.insert(*market, state);
            }
            Err(e) => {
                warn!("Failed to load market state for {}: {:?}", market, e);
            }
        }
    }

    // Collect all oracle account pubkeys from market states for streaming
    let mut oracle_accounts: std::collections::HashSet<Pubkey> = std::collections::HashSet::new();
    for state in market_states.values() {
        oracle_accounts.extend(state.pyth_accounts.keys());
        oracle_accounts.extend(state.switchboard_accounts.keys());
        oracle_accounts.extend(state.scope_accounts.keys());
    }
    info!("Collected {} oracle accounts for streaming", oracle_accounts.len());

    // Connect to Geyser AFTER initialization is complete
    let geyser_config = GeyserConfig {
        endpoint: geyser_endpoint,
        api_key: geyser_api_key,
        program_id: klend_client.program_id,
        oracle_accounts,
    };

    let mut geyser_stream = GeyserStream::connect(geyser_config).await?;
    info!("Connected to Geyser stream");

    // Initialize oracle cache with existing data from market states
    for state in market_states.values() {
        geyser_stream.oracle_cache().init_from_market_state(
            &state.pyth_accounts,
            &state.switchboard_accounts,
            &state.scope_accounts,
        );
    }
    info!("Oracle cache initialized with {} accounts", geyser_stream.oracle_cache().len());

    // Create the parallel liquidation orchestrator (wrapped in Arc for sharing across tasks)
    let orchestrator = Arc::new(parallel::LiquidationOrchestrator::new(parallel_config.clone()));
    info!("Parallel liquidation orchestrator initialized (max_concurrent={})", parallel_config.max_concurrent);

    // Track obligations we've seen and their LTVs
    let mut obligation_ltvs: HashMap<Pubkey, Fraction> = HashMap::new();
    let mut last_state_refresh = std::time::Instant::now();
    let state_refresh_interval = Duration::from_secs(state_refresh_hours * 3600);

    info!("Starting to process obligation updates...");

    loop {
        // Check if we should refresh the full market state (periodic safety check)
        if last_state_refresh.elapsed() > state_refresh_interval {
            info!("Refreshing market states (periodic refresh)");
            for market in &lending_markets {
                // Skip ALT updates during periodic refresh to avoid blocking
                if let Ok(state) = load_market_state(&klend_client, market, false).await {
                    // Ensure ATAs exist for any new reserve mints
                    if let Err(e) = klend_client
                        .liquidator
                        .ensure_atas_for_reserves(&klend_client, &state.reserves)
                        .await
                    {
                        warn!("Failed to ensure ATAs for market {}: {:?}", market, e);
                    }
                    market_states.insert(*market, state);
                }
            }
            last_state_refresh = std::time::Instant::now();
        }

        // Process obligation updates from Geyser
        // Use a timeout so we can do periodic tasks
        let update = tokio::time::timeout(
            Duration::from_secs(5),
            geyser_stream.recv()
        ).await;

        match update {
            Ok(Some(obligation_update)) => {
                let start = std::time::Instant::now();

                // Try to deserialize the obligation
                if let Some(obligation) = deserialize_obligation(&obligation_update.data) {
                    // Find which market this obligation belongs to
                    let market_pubkey = obligation.lending_market;

                    // Check if we're monitoring this market
                    if !lending_markets.contains(&market_pubkey) {
                        continue;
                    }

                    // Get the market state
                    let market_state = match market_states.get(&market_pubkey) {
                        Some(state) => state,
                        None => {
                            warn!("No market state for {}, skipping", market_pubkey);
                            continue;
                        }
                    };

                    // Evaluate the obligation
                    match evaluate_obligation_streaming(
                        &klend_client,
                        &obligation_update.pubkey,
                        &obligation,
                        market_state,
                    ).await {
                        Ok(Some(ltv_info)) => {
                            let prev_ltv = obligation_ltvs.get(&obligation_update.pubkey);
                            let ltv_changed = prev_ltv.map(|p| *p != ltv_info.ltv).unwrap_or(true);
                            obligation_ltvs.insert(obligation_update.pubkey, ltv_info.ltv);

                            if ltv_info.is_liquidatable {
                                info!(
                                    "{} LIQUIDATABLE: {} LTV={:?} (unhealthy={:?}) latency={}ms",
                                    "!!!".red().bold(),
                                    obligation_update.pubkey.to_string().red(),
                                    ltv_info.ltv,
                                    ltv_info.unhealthy_ltv,
                                    start.elapsed().as_millis()
                                );

                                // Calculate LTV margin (how far over threshold)
                                let ltv_margin_pct = if ltv_info.unhealthy_ltv > Fraction::ZERO {
                                    let ltv_f64: f64 = ltv_info.ltv.to_num();
                                    let unhealthy_f64: f64 = ltv_info.unhealthy_ltv.to_num();
                                    (ltv_f64 - unhealthy_f64) / unhealthy_f64
                                } else {
                                    0.0
                                };

                                // Trigger liquidation with orchestrator for deduplication/cooldown tracking
                                // Note: true parallelism not possible due to Kamino library using Rc<RefCell>
                                // but we get: deduplication, cooldown tracking, and structured logging
                                let obligation_key = obligation_update.pubkey;
                                let slot = obligation_update.slot;
                                let short_id = &obligation_key.to_string()[..8];

                                // Check orchestrator for deduplication
                                match orchestrator.try_start(&obligation_key).await {
                                    Some((task_id, permit)) => {
                                        let prefix = format!("[T{}:{}]", task_id, short_id);
                                        info!("{} Starting liquidation (LTV margin={:.2}%)", prefix, ltv_margin_pct * 100.0);

                                        // Wrap with catch_unwind to handle panics from klend library
                                        let liquidation_result = AssertUnwindSafe(liquidate_fast(
                                            &klend_client,
                                            &obligation_key,
                                            obligation.clone(),
                                            market_state,
                                            slot,
                                            ltv_margin_pct,
                                            &prefix,
                                            geyser_stream.oracle_cache(),
                                        )).catch_unwind().await;

                                        match liquidation_result {
                                            Ok(Ok(())) => {
                                                // Note: Ok(()) means the attempt finished without error,
                                                // but the liquidation may have been skipped (e.g., obligation became healthy)
                                                // The actual outcome is logged by liquidate_fast itself
                                                debug!("{} Attempt finished", prefix);
                                            }
                                            Ok(Err(e)) => {
                                                let err_str = format!("{:?}", e);
                                                let classification = parallel::classify_error(&err_str);
                                                match classification {
                                                    parallel::ErrorClassification::Permanent(reason) => {
                                                        info!("{} ✗ Permanent error ({})", prefix, reason);
                                                    }
                                                    parallel::ErrorClassification::Retryable(reason) => {
                                                        warn!("{} ⟳ Retryable error ({})", prefix, reason);
                                                    }
                                                    parallel::ErrorClassification::Unknown => {
                                                        warn!("{} ? Error: {}", prefix, &err_str[..100.min(err_str.len())]);
                                                    }
                                                }
                                            }
                                            Err(panic_info) => {
                                                let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                                                    s.to_string()
                                                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                                                    s.clone()
                                                } else {
                                                    "Unknown panic".to_string()
                                                };
                                                warn!("{} ⚠ Panic: {}", prefix, panic_msg);
                                            }
                                        }

                                        // Release orchestrator slot (cooldown starts)
                                        orchestrator.finish(&obligation_key).await;
                                        drop(permit);
                                    }
                                    None => {
                                        // Blocked by dedup or rate limit
                                        info!("[SKIP] {} already in-flight or cooldown", short_id);
                                    }
                                }
                            } else if ltv_changed && ltv_info.ltv > Fraction::ZERO {
                                // Log obligations with non-zero LTV for awareness
                                info!(
                                    "Obligation {} LTV={:?} (unhealthy={:?}) slot={} latency={}ms",
                                    obligation_update.pubkey.to_string().yellow(),
                                    ltv_info.ltv,
                                    ltv_info.unhealthy_ltv,
                                    obligation_update.slot,
                                    start.elapsed().as_millis()
                                );
                            }
                        }
                        Ok(None) => {
                            // Zero debt obligation, skip
                        }
                        Err(e) => {
                            warn!("Failed to evaluate obligation {}: {:?}", obligation_update.pubkey, e);
                        }
                    }
                }
            }
            Ok(None) => {
                // Stream closed, this shouldn't happen with reconnection logic
                warn!("Geyser stream closed unexpectedly");
                return Err(anyhow::anyhow!("Geyser stream closed"));
            }
            Err(_) => {
                // Timeout, continue to allow periodic tasks
                continue;
            }
        }
    }
}

/// Cached market state for fast obligation evaluation
#[derive(Clone)]
#[allow(dead_code)]
struct MarketState {
    lending_market: kamino_lending::LendingMarket,
    reserves: HashMap<Pubkey, Reserve>,
    referrer_token_states: HashMap<Pubkey, kamino_lending::ReferrerTokenState>,
    pyth_accounts: HashMap<Pubkey, Vec<u8>>,
    switchboard_accounts: HashMap<Pubkey, Vec<u8>>,
    scope_accounts: HashMap<Pubkey, Vec<u8>>,
}

/// Load the full state for a market
/// If `update_lookup_table` is true, will create/extend the lookup table (slow, only do at startup)
async fn load_market_state(klend_client: &KlendClient, market: &Pubkey, update_lookup_table: bool) -> Result<MarketState> {
    let market_accs = klend_client.fetch_market_and_reserves(market).await?;
    let rts = klend_client.fetch_referrer_token_states().await?;

    // Load/update the liquidator lookup table for this market (reduces transaction size)
    // Only extend at startup - periodic refresh should not block on ALT updates
    if update_lookup_table {
        if let Err(e) = klend_client.load_lookup_table(market, &market_accs).await {
            warn!("Failed to load lookup table for market {}: {:?}", market, e);
        }
    }

    let OracleAccounts {
        pyth_accounts,
        switchboard_accounts,
        scope_price_accounts,
    } = oracle_accounts(&klend_client.client, &market_accs.reserves)
        .await?;

    // Convert account data to raw bytes for storage
    // The tuples are (Pubkey, bool, Account)
    let pyth_map: HashMap<Pubkey, Vec<u8>> = pyth_accounts
        .into_iter()
        .map(|(k, _valid, acc)| (k, acc.data))
        .collect();
    let switchboard_map: HashMap<Pubkey, Vec<u8>> = switchboard_accounts
        .into_iter()
        .map(|(k, _valid, acc)| (k, acc.data))
        .collect();
    let scope_map: HashMap<Pubkey, Vec<u8>> = scope_price_accounts
        .into_iter()
        .map(|(k, _valid, acc)| (k, acc.data))
        .collect();

    Ok(MarketState {
        lending_market: market_accs.lending_market,
        reserves: market_accs.reserves,
        referrer_token_states: rts,
        pyth_accounts: pyth_map,
        switchboard_accounts: switchboard_map,
        scope_accounts: scope_map,
    })
}

/// LTV information for an obligation
struct LtvInfo {
    ltv: Fraction,
    unhealthy_ltv: Fraction,
    is_liquidatable: bool,
}

/// Evaluate an obligation for liquidation using cached market state
async fn evaluate_obligation_streaming(
    _klend_client: &KlendClient,
    obligation_pubkey: &Pubkey,
    obligation: &Obligation,
    _market_state: &MarketState,
) -> Result<Option<LtvInfo>> {
    // Skip zero-debt obligations
    if obligation.num_of_obsolete_reserves == 0
        && obligation.deposits_empty()
        && obligation.borrows_empty() {
        return Ok(None);
    }

    // Check if this has any borrows (debt)
    let has_debt = !obligation.borrows_empty();
    if !has_debt {
        return Ok(None);
    }

    // Get obligation info using the math module
    let stats = math::obligation_info(obligation_pubkey, obligation);

    let is_liquidatable = stats.ltv > stats.unhealthy_ltv;

    Ok(Some(LtvInfo {
        ltv: stats.ltv,
        unhealthy_ltv: stats.unhealthy_ltv,
        is_liquidatable,
    }))
}

/// Deserialize raw account data into an Obligation
fn deserialize_obligation(data: &[u8]) -> Option<Obligation> {
    // Anchor accounts have an 8-byte discriminator prefix
    if data.len() < 8 + std::mem::size_of::<Obligation>() {
        return None;
    }

    // Check discriminator (Kamino Obligation)
    const OBLIGATION_DISCRIMINATOR: [u8; 8] = [168, 206, 141, 106, 88, 76, 172, 167];
    if data[..8] != OBLIGATION_DISCRIMINATOR {
        return None;
    }

    // Try to deserialize using bytemuck (zero-copy)
    let obligation_data = &data[8..];
    match try_from_bytes::<Obligation>(obligation_data) {
        Ok(obligation) => Some(*obligation),
        Err(_) => None,
    }
}

/// Buffer added to clock slot to account for RPC lag.
/// RPC get_slot() can return a slot that's behind the slot of data we fetched,
/// causing "reserve stale" errors when the reserve's last_update.slot is ahead.
const CLOCK_SLOT_BUFFER: u64 = 100;

/// Calculate the clock slot to use for liquidation operations.
/// Takes the maximum of RPC slot and Geyser slot, then adds a buffer to ensure
/// the clock is ahead of any fetched reserve data.
///
/// This fixes a race condition where:
/// 1. We fetch reserve data from RPC (at slot X)
/// 2. We call get_slot() which returns slot Y where Y < X
/// 3. Reserve refresh fails because reserve.last_update.slot > clock.slot
fn calculate_clock_slot(rpc_slot: u64, geyser_slot: u64) -> u64 {
    rpc_slot.max(geyser_slot) + CLOCK_SLOT_BUFFER
}

#[cfg(test)]
mod clock_slot_tests {
    use super::*;

    #[test]
    fn test_clock_slot_uses_max_of_rpc_and_geyser() {
        // When RPC slot is ahead
        assert_eq!(calculate_clock_slot(1000, 900), 1000 + CLOCK_SLOT_BUFFER);

        // When Geyser slot is ahead
        assert_eq!(calculate_clock_slot(900, 1000), 1000 + CLOCK_SLOT_BUFFER);

        // When both are equal
        assert_eq!(calculate_clock_slot(1000, 1000), 1000 + CLOCK_SLOT_BUFFER);
    }

    #[test]
    fn test_clock_slot_handles_rpc_lag_scenario() {
        // Real scenario from logs:
        // - Reserve data fetched at slot 397173192
        // - RPC get_slot() returned 397173108 (84 slots behind)
        // - Geyser slot was also stale
        let rpc_slot = 397173108;
        let geyser_slot = 397173100; // Even more stale
        let reserve_slot = 397173192; // This is what the fetched reserve had

        let clock_slot = calculate_clock_slot(rpc_slot, geyser_slot);

        // The clock slot should be ahead of the reserve slot with the buffer
        // In this case: max(397173108, 397173100) + 100 = 397173208
        // Which is > 397173192 (reserve slot)
        assert!(clock_slot > reserve_slot,
            "clock_slot {} should be > reserve_slot {}", clock_slot, reserve_slot);
    }

    #[test]
    fn test_clock_slot_buffer_prevents_stale_error() {
        // If reserve is only slightly ahead of RPC (within buffer), we should still be safe
        let rpc_slot = 1000;
        let geyser_slot = 950;

        // Reserve fetched could be up to ~100 slots ahead of RPC slot
        // (typical RPC lag is 50-100 slots based on observations)
        for reserve_ahead_by in [10, 50, 99] {
            let reserve_slot = rpc_slot + reserve_ahead_by;
            let clock_slot = calculate_clock_slot(rpc_slot, geyser_slot);

            assert!(clock_slot > reserve_slot,
                "With reserve {} slots ahead, clock_slot {} should be > reserve_slot {}",
                reserve_ahead_by, clock_slot, reserve_slot);
        }
    }

    #[test]
    fn test_clock_slot_buffer_value() {
        // Verify the buffer is reasonable (not too small, not too large)
        // Too small: won't cover typical RPC lag
        // Too large: might cause issues with slot-dependent calculations
        assert!(CLOCK_SLOT_BUFFER >= 50, "Buffer should be at least 50 slots");
        assert!(CLOCK_SLOT_BUFFER <= 200, "Buffer should not exceed 200 slots");
    }

    #[test]
    fn test_multiple_reserves_ahead_scenario() {
        // From logs: clock was 397173075, multiple reserves were ahead
        // This caused MathOverflow when slots_elapsed was called
        let rpc_slot = 397173000; // RPC returned this
        let geyser_slot = 397173075; // Geyser had this

        let clock_slot = calculate_clock_slot(rpc_slot, geyser_slot);

        // Reserves that were ahead in the logs (estimated ~50-100 slots ahead)
        // d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q
        // febGYTnFX4GbSGoFHFeJXUHgNaK53fB23uDins9Jp1E
        // D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59
        // H3t6qZ1JkguCNTi9uzVKqQ7dvt2cum4XiXWom6Gn5e5S
        let reserve_slots = [
            geyser_slot + 50,  // 50 ahead
            geyser_slot + 75,  // 75 ahead
            geyser_slot + 90,  // 90 ahead
            geyser_slot + 95,  // 95 ahead
        ];

        for reserve_slot in reserve_slots {
            assert!(clock_slot > reserve_slot,
                "clock_slot {} should be > reserve_slot {} (was {} slots ahead)",
                clock_slot, reserve_slot, reserve_slot - geyser_slot);
        }
    }
}

// Integration tests moved to titan.rs for now due to complex dependencies
// TODO: Add reserve lookup test when we figure out the KlendClient initialization
