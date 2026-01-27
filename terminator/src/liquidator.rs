use std::{collections::HashMap, sync::{Arc, RwLock}, time::Duration};

use anchor_client::solana_client::nonblocking::rpc_client::RpcClient;
use anchor_lang::{prelude::Pubkey, solana_program::program_pack::Pack, AccountDeserialize, Id};
use anchor_spl::token::{Mint, Token};
use anyhow::{anyhow, Result};
use kamino_lending::Reserve;
use solana_sdk::{signature::Keypair, signer::Signer};
use spl_associated_token_account::{
    get_associated_token_address, instruction::create_associated_token_account,
};
use spl_token::state::Account as TokenAccount;
use tracing::{debug, info, warn};

use crate::{accounts::find_account, client::KlendClient, consts::WRAPPED_SOL_MINT, px::Prices};

#[derive(Debug, Clone, Default)]
pub struct Holdings {
    pub holdings: Vec<Holding>,
    pub sol: Holding,
}

#[derive(Debug, Clone, Default)]
pub struct Holding {
    pub mint: Pubkey,
    pub ata: Pubkey,
    pub decimals: u8,
    pub balance: u64,
    pub ui_balance: f64,
    pub label: String,
    pub usd_value: f64,
}

impl Holdings {
    pub fn holding_of(&self, mint: &Pubkey) -> Result<Holding> {
        for holding in self.holdings.iter() {
            if holding.mint == *mint {
                return Ok(holding.clone());
            }
        }
        Err(anyhow!("Holding not found for mint {}", mint))
    }
}

#[derive(Debug)]
pub struct Liquidator {
    pub wallet: Arc<Keypair>,
    pub atas: RwLock<HashMap<Pubkey, Pubkey>>,
}

fn label_of(mint: &Pubkey, reserves: &HashMap<Pubkey, Reserve>) -> String {
    for (_, reserve) in reserves.iter() {
        if &reserve.liquidity.mint_pubkey == mint {
            let symbol = reserve.config.token_info.symbol().to_string();
            if symbol == "SOL" {
                return "WSOL".to_string();
            } else {
                return symbol;
            }
        }
    }
    mint.to_string()
}

impl Liquidator {
    pub async fn init(
        client: &KlendClient,
        reserves: &HashMap<Pubkey, Reserve>,
    ) -> Result<Liquidator> {
        // Load reserves mints
        let mints: Vec<Pubkey> = reserves
            .iter()
            .flat_map(|(_, r)| [r.liquidity.mint_pubkey, r.collateral.mint_pubkey])
            .collect();
        // Load wallet
        let mut atas = HashMap::new();
        let wallet = match { client.client.payer().ok() } {
            Some(wallet) => {
                // Load or create atas
                info!("Loading atas...");
                let wallet = Arc::new(wallet.insecure_clone());
                let get_or_create_atas_futures = mints
                    .iter()
                    .map(|mint| get_or_create_ata(client, &wallet, mint));
                let get_or_create_atas =
                    futures::future::join_all(get_or_create_atas_futures).await;
                for (i, ata) in get_or_create_atas.into_iter().enumerate() {
                    atas.insert(*mints.get(i).unwrap(), ata?);
                }
                info!(
                    "Loaded liquidator {} with {} tokens",
                    wallet.pubkey(),
                    atas.len(),
                );
                wallet
            }
            None => Arc::new(Keypair::new()),
        };

        let liquidator = Liquidator {
            wallet,
            atas: RwLock::new(atas),
        };

        Ok(liquidator)
    }

    /// Ensure ATAs exist for all mints in the given reserves
    pub async fn ensure_atas_for_reserves(
        &self,
        client: &KlendClient,
        reserves: &HashMap<Pubkey, Reserve>,
    ) -> Result<()> {
        let mints: Vec<Pubkey> = {
            let existing_atas = self.atas.read().unwrap();
            reserves
                .iter()
                .flat_map(|(_, r)| [r.liquidity.mint_pubkey, r.collateral.mint_pubkey])
                .filter(|mint| !existing_atas.contains_key(mint))
                .collect()
        };

        if mints.is_empty() {
            return Ok(());
        }

        info!("Creating ATAs for {} new mints...", mints.len());

        // Process in batches to avoid rate limiting
        const BATCH_SIZE: usize = 10;
        let mut all_results = Vec::with_capacity(mints.len());

        for (batch_idx, chunk) in mints.chunks(BATCH_SIZE).enumerate() {
            if batch_idx > 0 {
                // Add delay between batches to avoid rate limiting
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            let futures = chunk
                .iter()
                .map(|mint| get_or_create_ata(client, &self.wallet, mint));
            let batch_results = futures::future::join_all(futures).await;
            all_results.extend(batch_results);
        }

        let mut atas = self.atas.write().unwrap();
        for (i, result) in all_results.into_iter().enumerate() {
            let mint = mints[i];
            match result {
                Ok(ata) => {
                    atas.insert(mint, ata);
                }
                Err(e) => {
                    warn!("Failed to create ATA for mint {}: {:?}", mint, e);
                }
            }
        }

        info!("Liquidator now has {} token ATAs", atas.len());
        Ok(())
    }

    pub async fn fetch_holdings(
        &self,
        client: &RpcClient,
        reserves: &HashMap<Pubkey, Reserve>,
        prices: &Prices,
    ) -> Result<Holdings> {
        let mut holdings = Vec::new();

        // Get a snapshot of atas
        let atas_snapshot: Vec<(Pubkey, Pubkey)> = {
            let atas = self.atas.read().unwrap();
            atas.iter().map(|(m, a)| (*m, *a)).collect()
        };

        // Batch fetch all mint and ATA accounts in single RPC calls
        let mint_pubkeys: Vec<Pubkey> = atas_snapshot.iter().map(|(m, _)| *m).collect();
        let ata_pubkeys: Vec<Pubkey> = atas_snapshot.iter().map(|(_, a)| *a).collect();

        let mint_accounts = client.get_multiple_accounts(&mint_pubkeys).await?;
        let ata_accounts = client.get_multiple_accounts(&ata_pubkeys).await?;

        for (i, (mint, ata)) in atas_snapshot.iter().enumerate() {
            let mint_account = mint_accounts.get(i).and_then(|a| a.as_ref());
            let ata_account = ata_accounts.get(i).and_then(|a| a.as_ref());

            match (mint_account, ata_account) {
                (Some(mint_acc), Some(ata_acc)) => {
                    let token_account = match TokenAccount::unpack(&ata_acc.data) {
                        Ok(acc) => acc,
                        Err(e) => {
                            warn!("Error unpacking token account {:?}: {:?}", ata, e);
                            continue;
                        }
                    };
                    let mint_data = match Mint::try_deserialize_unchecked(&mut mint_acc.data.as_ref()) {
                        Ok(m) => m,
                        Err(e) => {
                            warn!("Error deserializing mint {:?}: {:?}", mint, e);
                            continue;
                        }
                    };
                    let balance = token_account.amount;
                    let decimals = mint_data.decimals;
                    let ui_balance = balance as f64 / 10u64.pow(decimals as u32) as f64;
                    holdings.push(Holding {
                        mint: *mint,
                        ata: *ata,
                        decimals,
                        balance,
                        ui_balance,
                        label: label_of(mint, reserves),
                        usd_value: if balance > 0 {
                            prices
                                .prices
                                .get(mint)
                                .map_or(0.0, |price| ui_balance * price)
                        } else {
                            0.0
                        },
                    });
                }
                _ => {
                    // ATA doesn't exist yet, skip silently
                }
            }
        }

        // Load SOL balance
        let balance = client.get_balance(&self.wallet.pubkey()).await?;
        let ui_balance = balance as f64 / 10u64.pow(9) as f64;
        let sol_price = prices.prices.get(&WRAPPED_SOL_MINT).copied().unwrap_or(0.0);
        let sol_holding = Holding {
            mint: Pubkey::default(), // No mint, this is the native balance
            ata: Pubkey::default(),  // Holding in the native account, not in the ata
            decimals: 9,
            balance,
            ui_balance,
            label: "SOL".to_string(),
            usd_value: ui_balance * sol_price,
        };
        info!("Holding {} SOL", sol_holding.ui_balance);

        for holding in holdings.iter() {
            if holding.balance > 0 {
                info!("Holding {} {}", holding.ui_balance, holding.label);
            }
        }

        let holding = Holdings {
            holdings,
            sol: sol_holding,
        };
        Ok(holding)
    }
}

async fn get_or_create_ata(
    client: &KlendClient,
    owner: &Arc<Keypair>,
    mint: &Pubkey,
) -> Result<Pubkey> {
    let owner_pubkey = &owner.pubkey();
    let ata = get_associated_token_address(owner_pubkey, mint);
    if !matches!(find_account(&client.client.client, ata).await, Ok(None)) {
        debug!("Liquidator ATA for mint {} exists: {}", mint, ata);
        Ok(ata)
    } else {
        debug!(
            "Liquidator ATA for mint {} does not exist, creating...",
            mint
        );
        let ix = create_associated_token_account(owner_pubkey, owner_pubkey, mint, &Token::id());
        let tx = client
            .client
            .tx_builder()
            .add_ix(ix)
            .build(&[])
            .await?;

        let (sig, _) = client.send_and_confirm_transaction(tx).await?;

        debug!(
            "Created ata for liquidator: {}, mint: {}, ata: {}, sig: {:?}",
            owner.pubkey(),
            mint,
            ata,
            sig
        );

        Ok(ata)
    }
}

/// get the balance of a token account
pub async fn get_token_balance(
    client: &RpcClient,
    mint: &Pubkey,
    token_account: &Pubkey,
) -> Result<(u64, u8)> {
    let mint_account = client.get_account(mint).await?;
    let token_account = client.get_account(token_account).await?;
    let token_account = TokenAccount::unpack(&token_account.data)?;
    let mint_account = Mint::try_deserialize_unchecked(&mut mint_account.data.as_ref())?;
    let amount = token_account.amount;
    let decimals = mint_account.decimals;
    Ok((amount, decimals))
}
