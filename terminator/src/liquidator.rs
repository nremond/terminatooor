use std::{collections::HashMap, sync::{Arc, RwLock}, time::Duration};

use anchor_client::solana_client::nonblocking::rpc_client::RpcClient;
use anchor_lang::{prelude::Pubkey, solana_program::program_pack::Pack, AccountDeserialize, Id};
use anchor_spl::token::{Mint, Token};
use anyhow::Result;
use kamino_lending::Reserve;
use solana_sdk::{signature::Keypair, signer::Signer};
use spl_associated_token_account::{
    get_associated_token_address, get_associated_token_address_with_program_id,
    instruction::create_associated_token_account,
};
use spl_token::state::Account as TokenAccount;
use tracing::{debug, info, warn};

use crate::{accounts::find_account, client::KlendClient};

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

#[derive(Debug)]
pub struct Liquidator {
    pub wallet: Arc<Keypair>,
    pub atas: RwLock<HashMap<Pubkey, Pubkey>>,
    /// Maps mint -> token program (SPL Token or Token2022)
    pub mint_token_programs: RwLock<HashMap<Pubkey, Pubkey>>,
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

        // Fetch token programs for all mints
        let mut mint_token_programs = HashMap::new();
        for chunk in mints.chunks(100) {
            let accounts = client.client.client.get_multiple_accounts(chunk).await?;
            for (i, account) in accounts.iter().enumerate() {
                if let Some(acc) = account {
                    mint_token_programs.insert(chunk[i], acc.owner);
                }
            }
        }

        let liquidator = Liquidator {
            wallet,
            atas: RwLock::new(atas),
            mint_token_programs: RwLock::new(mint_token_programs),
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

        info!("Ensuring ATAs for {} new mints...", mints.len());

        let owner_pubkey = self.wallet.pubkey();

        // Fetch mint accounts to determine which token program each uses
        let mut mint_token_programs: HashMap<Pubkey, Pubkey> = HashMap::new();
        for chunk in mints.chunks(100) {
            let accounts = client.client.client.get_multiple_accounts(chunk).await?;
            for (i, account) in accounts.iter().enumerate() {
                if let Some(acc) = account {
                    // The owner of the mint account tells us which token program it uses
                    mint_token_programs.insert(chunk[i], acc.owner);
                }
            }
        }

        // Calculate all ATA addresses using the correct token program for each mint
        let ata_addresses: Vec<Pubkey> = mints
            .iter()
            .map(|mint| {
                let token_program = mint_token_programs.get(mint).copied().unwrap_or(Token::id());
                get_associated_token_address_with_program_id(&owner_pubkey, mint, &token_program)
            })
            .collect();

        // Batch check which ATAs already exist (100 per RPC call)
        let mut existing_atas = std::collections::HashSet::new();
        for chunk in ata_addresses.chunks(100) {
            let accounts = client.client.client.get_multiple_accounts(chunk).await?;
            for (i, account) in accounts.iter().enumerate() {
                if account.is_some() {
                    existing_atas.insert(chunk[i]);
                }
            }
        }

        // Find mints that need ATA creation (with their token program)
        let mints_needing_ata: Vec<(&Pubkey, Pubkey)> = mints
            .iter()
            .enumerate()
            .filter(|(i, _)| !existing_atas.contains(&ata_addresses[*i]))
            .map(|(_, mint)| {
                let token_program = mint_token_programs.get(mint).copied().unwrap_or(Token::id());
                (mint, token_program)
            })
            .collect();

        info!(
            "Found {} existing ATAs, need to create {} new ones",
            existing_atas.len(),
            mints_needing_ata.len()
        );

        // Batch create ATAs (up to 5 per transaction to fit in tx size limit of 1232 bytes)
        const CREATE_BATCH_SIZE: usize = 5;
        for (batch_idx, chunk) in mints_needing_ata.chunks(CREATE_BATCH_SIZE).enumerate() {
            if chunk.is_empty() {
                continue;
            }

            let ixs: Vec<_> = chunk
                .iter()
                .map(|(mint, token_program)| {
                    create_associated_token_account(&owner_pubkey, &owner_pubkey, mint, token_program)
                })
                .collect();

            info!(
                "Creating ATAs batch {}: {} accounts",
                batch_idx + 1,
                ixs.len()
            );

            let tx = client.client.create_tx(&ixs, &[]).await?;

            match client.send_and_confirm_transaction(tx).await {
                Ok((sig, _)) => {
                    debug!("Created {} ATAs in tx: {:?}", chunk.len(), sig);
                }
                Err(e) => {
                    warn!("Failed to create ATA batch {}: {:?}", batch_idx + 1, e);
                }
            }

            // Small delay between batches
            if batch_idx > 0 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        // Store all ATAs (both existing and newly created)
        {
            let mut atas = self.atas.write().unwrap();
            for (i, mint) in mints.iter().enumerate() {
                atas.insert(*mint, ata_addresses[i]);
            }
            info!("Liquidator now has {} token ATAs", atas.len());
        }

        // Store token programs
        {
            let mut programs = self.mint_token_programs.write().unwrap();
            for (mint, token_program) in mint_token_programs {
                programs.insert(mint, token_program);
            }
        }

        Ok(())
    }

    /// Get the token program for a mint (SPL Token or Token2022)
    /// Returns SPL Token as default if not found
    pub fn token_program_for_mint(&self, mint: &Pubkey) -> Pubkey {
        self.mint_token_programs
            .read()
            .unwrap()
            .get(mint)
            .copied()
            .unwrap_or(Token::id())
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
