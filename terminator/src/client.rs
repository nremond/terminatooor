use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Write},
    str::FromStr,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

type TransactionResult = std::result::Result<(), TransactionError>;

use anchor_client::{
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    },
    solana_sdk::pubkey::Pubkey,
};
use anyhow::{anyhow, Result};
use kamino_lending::{
    utils::seeds, LendingMarket, Obligation, ReferrerTokenState, Reserve, ReserveFarmKind,
};
use orbit_link::OrbitLink;
use solana_sdk::{
    address_lookup_table::{state::AddressLookupTable, AddressLookupTableAccount},
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    signature::{Keypair, Signature},
    transaction::{TransactionError, VersionedTransaction},
};
use tokio::task;
use tracing::{info, warn};

use crate::{
    accounts::{find_account, market_and_reserve_accounts, MarketAccounts},
    consts::{NULL_PUBKEY, WRAPPED_SOL_MINT},
    instructions::{self, InstructionBlocks},
    liquidator::Liquidator,
    lookup_tables::collect_keys,
    model::StateWithKey,
    px,
    px::Prices,
};

pub struct KlendClient {
    pub program_id: Pubkey,

    pub client: OrbitLink<RpcClient, Keypair>,

    // Per-market lookup tables for transaction size reduction
    // Key: market pubkey, Value: lookup table account
    pub lookup_tables: RwLock<HashMap<Pubkey, AddressLookupTableAccount>>,

    // Rebalance settings
    pub rebalance_config: Option<RebalanceConfig>,

    // Liquidator
    // TODO: move all the fields of the liquidator out of this struct and flatten it
    pub liquidator: Liquidator,
}

#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    pub base_token: Pubkey,
    pub min_sol_balance: f64,
    pub usdc_mint: Pubkey,
    pub rebalance_slippage_pct: f64,
    pub non_swappable_dust_usd_value: f64,
}

impl KlendClient {
    pub fn init(
        client: OrbitLink<RpcClient, Keypair>,
        program_id: Pubkey,
        rebalance_config: Option<RebalanceConfig>,
    ) -> Result<Self> {
        // Use the payer keypair from OrbitLink, or create a dummy one for read-only operations
        let wallet = client
            .payer()
            .ok()
            .map(|k| Arc::new(k.insecure_clone()))
            .unwrap_or_else(|| Arc::new(Keypair::new()));

        let liquidator = Liquidator {
            wallet,
            atas: RwLock::new(HashMap::new()),
            mint_token_programs: RwLock::new(HashMap::new()),
        };

        Ok(Self {
            program_id,
            client,
            lookup_tables: RwLock::new(HashMap::new()),
            liquidator,
            rebalance_config,
        })
    }

    pub async fn fetch_market_and_reserves(&self, market: &Pubkey) -> Result<MarketAccounts> {
        market_and_reserve_accounts(self, market).await
    }

    /// Fetch top lending markets by borrow volume
    pub async fn fetch_all_markets(&self) -> Result<Vec<Pubkey>> {
        info!("Fetching active lending markets for program: {}", self.program_id);

        // Max number of markets to monitor (default: 30)
        let max_markets: usize = std::env::var("MAX_MARKETS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        // Fetch all reserves
        let reserves: Vec<(Pubkey, Reserve)> =
            rpc::get_zero_copy_pa(&self.client, &self.program_id, &[]).await?;

        // Sum borrowed amounts per market
        let mut market_borrows: std::collections::HashMap<Pubkey, u128> =
            std::collections::HashMap::new();
        for (_, reserve) in &reserves {
            *market_borrows.entry(reserve.lending_market).or_insert(0) +=
                reserve.liquidity.borrowed_amount_sf;
        }

        let total_markets = market_borrows.len();

        // Sort by borrow volume (descending) and take top N
        let mut markets_sorted: Vec<(Pubkey, u128)> = market_borrows
            .into_iter()
            .filter(|(_, borrows)| *borrows > 0) // Must have some borrows
            .collect();
        markets_sorted.sort_by(|a, b| b.1.cmp(&a.1));

        let pubkeys: Vec<Pubkey> = markets_sorted
            .iter()
            .take(max_markets)
            .map(|(pk, _)| *pk)
            .collect();

        info!(
            "Selected top {} markets by borrow volume (from {} total with reserves)",
            pubkeys.len(),
            total_markets
        );
        Ok(pubkeys)
    }

    pub async fn fetch_obligations(&self, market: &Pubkey) -> Result<Vec<(Pubkey, Obligation)>> {
        info!("Fetching obligations for market: {}", market);
        let filter = RpcFilterType::Memcmp(Memcmp::new(
            32,
            MemcmpEncodedBytes::Bytes(market.to_bytes().to_vec()),
        ));
        let filters = vec![filter];
        let obligations = rpc::get_zero_copy_pa(&self.client, &self.program_id, &filters).await?;
        Ok(obligations)
    }

    pub async fn fetch_obligation(&self, obligation_address: &Pubkey) -> Result<Obligation> {
        info!("Fetching obligation: {}", obligation_address);
        let obligation = self
            .client
            .get_anchor_account::<Obligation>(obligation_address)
            .await?;
        Ok(obligation)
    }

    pub async fn fetch_referrer_token_states(&self) -> Result<HashMap<Pubkey, ReferrerTokenState>> {
        let states = self
            .client
            .get_all_zero_copy_accounts::<ReferrerTokenState>()
            .await?;
        let map = states.into_iter().collect();
        Ok(map)
    }

    /// Load or create the liquidator's lookup table for the given market.
    /// This reduces transaction size by using address lookup tables.
    /// Each market has its own lookup table to stay under the 256 address limit.
    /// Uses LIQUIDATOR_LOOKUP_TABLE_FILE env var or defaults to "liquidator_lookup_tables.json".
    pub async fn load_lookup_table(&self, market_key: &Pubkey, market_accounts: &MarketAccounts) -> Result<()> {

        // Load existing LUT mappings from file
        self.load_lookup_tables_from_file().await?;

        // Get or create LUT for this market
        let expected_keys = collect_keys(
            &market_accounts.reserves,
            &self.liquidator,
            &market_accounts.lending_market,
        );

        self.ensure_market_lookup_table(market_key, expected_keys).await?;

        let luts = self.lookup_tables.read().unwrap();
        if let Some(lut) = luts.get(market_key) {
            info!("Lookup table for market {} loaded with {} addresses", market_key, lut.addresses.len());
        }
        Ok(())
    }

    /// Get the lookup table for a specific market
    pub fn get_lookup_table(&self, market: &Pubkey) -> Option<AddressLookupTableAccount> {
        self.lookup_tables.read().unwrap().get(market).cloned()
    }

    fn get_lookup_tables_filename() -> String {
        std::env::var("LIQUIDATOR_LOOKUP_TABLE_FILE")
            .unwrap_or_else(|_| "liquidator_lookup_tables.json".to_string())
    }

    /// Load all lookup table mappings from the JSON file
    /// Only loads if tables haven't been loaded yet (skips if already in memory)
    async fn load_lookup_tables_from_file(&self) -> Result<()> {
        // Skip if already loaded - check if we have any tables in memory
        {
            let tables = self.lookup_tables.read().unwrap();
            if !tables.is_empty() {
                return Ok(());
            }
        }

        let filename = Self::get_lookup_tables_filename();

        if !std::path::Path::new(&filename).exists() {
            // Create empty JSON file
            let mut file = File::create(&filename)?;
            file.write_all(b"{}")?;
            return Ok(());
        }

        let mut file = File::open(&filename)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        if content.trim().is_empty() || content.trim() == "{}" {
            return Ok(());
        }

        // Parse JSON: {"market_pubkey": "lut_pubkey", ...}
        let mapping: HashMap<String, String> = serde_json::from_str(&content)
            .map_err(|e| anyhow!("Failed to parse lookup tables JSON: {}", e))?;

        for (market_str, lut_str) in mapping {
            let market_key = Pubkey::from_str(&market_str)
                .map_err(|e| anyhow!("Invalid market pubkey {}: {}", market_str, e))?;
            let lut_key = Pubkey::from_str(&lut_str)
                .map_err(|e| anyhow!("Invalid LUT pubkey {}: {}", lut_str, e))?;

            // Fetch the actual lookup table data from chain
            match self.client.client.get_account(&lut_key).await {
                Ok(account) => {
                    match AddressLookupTable::deserialize(&account.data) {
                        Ok(table) => {
                            let lut_account = AddressLookupTableAccount {
                                key: lut_key,
                                addresses: table.addresses.to_vec(),
                            };
                            info!("Loaded lookup table {} for market {} with {} addresses",
                                  lut_key, market_key, lut_account.addresses.len());
                            self.lookup_tables.write().unwrap().insert(market_key, lut_account);
                        }
                        Err(e) => {
                            info!("Failed to deserialize lookup table {}: {:?}", lut_key, e);
                        }
                    }
                }
                Err(e) => {
                    info!("Failed to fetch lookup table {}: {:?}", lut_key, e);
                }
            }
        }

        Ok(())
    }

    /// Save the current lookup table mappings to file
    fn save_lookup_tables_to_file(&self) -> Result<()> {
        let filename = Self::get_lookup_tables_filename();

        let mapping: HashMap<String, String> = self.lookup_tables
            .read()
            .unwrap()
            .iter()
            .map(|(market, lut)| (market.to_string(), lut.key.to_string()))
            .collect();

        let json = serde_json::to_string_pretty(&mapping)
            .map_err(|e| anyhow!("Failed to serialize lookup tables: {}", e))?;

        let mut file = File::create(&filename)?;
        file.write_all(json.as_bytes())?;

        Ok(())
    }

    /// Ensure a lookup table exists for the given market with the expected keys
    async fn ensure_market_lookup_table(&self, market: &Pubkey, expected: HashSet<Pubkey>) -> Result<()> {
        const MAX_LUT_ADDRESSES: usize = 256;

        let has_lut = self.lookup_tables.read().unwrap().contains_key(market);

        if !has_lut {
            // Create new lookup table for this market
            info!("Creating new lookup table for market {}", market);
            let keys_to_add: Vec<Pubkey> = expected.iter().copied().take(MAX_LUT_ADDRESSES).collect();

            let lut = self.create_init_reserve_lookup_table(&keys_to_add, || {
                thread::sleep(Duration::from_secs(12));
            }).await?;

            info!("Created lookup table {} for market {} with {} addresses", lut.key, market, lut.addresses.len());
            self.lookup_tables.write().unwrap().insert(*market, lut);
            self.save_lookup_tables_to_file()?;
            return Ok(());
        }

        // Check if we need to extend the existing LUT
        let (lut_key, current_count, missing_keys) = {
            let luts = self.lookup_tables.read().unwrap();
            let lut = luts.get(market).ok_or_else(|| anyhow!("Lookup table not found"))?;
            let already_in_lut: HashSet<Pubkey> = HashSet::from_iter(lut.addresses.iter().copied());

            let missing: Vec<Pubkey> = expected
                .iter()
                .filter(|x| !already_in_lut.contains(x))
                .copied()
                .collect();

            info!("Market {} lookup table: {} current, {} missing keys",
                  market, lut.addresses.len(), missing.len());

            (lut.key, lut.addresses.len(), missing)
        };

        if !missing_keys.is_empty() {
            // Only add keys up to the 256 limit
            let space_available = MAX_LUT_ADDRESSES.saturating_sub(current_count);
            if space_available == 0 {
                warn!(
                    "Lookup table for market {} is full (256 addresses). {} keys not in LUT - \
                    liquidations involving low-volume reserves may fail due to transaction size.",
                    market, missing_keys.len()
                );
                return Ok(());
            }

            let keys_to_add: Vec<Pubkey> = missing_keys.into_iter().take(space_available).collect();
            info!("Extending lookup table for market {} with {} new keys", market, keys_to_add.len());

            self.extend_lut_with_keys(lut_key, &keys_to_add, || {
                thread::sleep(Duration::from_secs(12));
            }).await?;

            // Reload the lookup table
            let account = self.client.client.get_account(&lut_key).await
                .map_err(|e| anyhow!("Failed to fetch lookup table: {}", e))?;
            let table = AddressLookupTable::deserialize(&account.data)
                .map_err(|e| anyhow!("Failed to deserialize lookup table: {}", e))?;

            let lut_account = AddressLookupTableAccount {
                key: lut_key,
                addresses: table.addresses.to_vec(),
            };

            self.lookup_tables.write().unwrap().insert(*market, lut_account);
        }

        Ok(())
    }

    async fn create_init_reserve_lookup_table(
        &self,
        keys: &[Pubkey],
        delay_fn: impl Fn(),
    ) -> Result<AddressLookupTableAccount> {
        use solana_sdk::address_lookup_table::instruction;

        // Create lookup table
        let recent_slot = self
            .client
            .client
            .get_slot_with_commitment(CommitmentConfig::finalized())
            .await?;

        let (create_lookup_table, table_pk) = instruction::create_lookup_table(
            self.client.payer_pubkey(),
            self.client.payer_pubkey(),
            recent_slot,
        );

        let txn = self.client.create_tx(&[create_lookup_table], &[]).await?;

        self.client
            .send_retry_and_confirm_transaction(txn, None, false)
            .await?;

        let keys = keys
            .iter()
            .filter(|x| **x != NULL_PUBKEY)
            .copied()
            .collect::<Vec<Pubkey>>();

        self.extend_lut_with_keys(table_pk, &keys, delay_fn).await?;

        Ok(AddressLookupTableAccount {
            key: table_pk,
            addresses: keys,
        })
    }

    async fn extend_lut_with_keys(
        &self,
        table_pk: Pubkey,
        keys: &[Pubkey],
        delay_fn: impl Fn(),
    ) -> Result<()> {
        use solana_sdk::address_lookup_table::instruction;

        for selected_keys in keys.chunks(20) {
            info!("Extending lookup table with {} keys", selected_keys.len());
            let extend_ix = instruction::extend_lookup_table(
                table_pk,
                self.client.payer_pubkey(),
                Some(self.client.payer_pubkey()),
                selected_keys.to_vec(),
            );

            let tx = self.client.create_tx(&[extend_ix], &[]).await?;
            self.send_and_confirm_transaction(tx).await.unwrap();
            // wait until lookup table is active
            delay_fn();
        }

        Ok(())
    }

    // TODO: move this to orbitlink
    pub async fn send_and_confirm_transaction(
        &self,
        tx: VersionedTransaction,
    ) -> Result<(Signature, Option<TransactionResult>)> {
        let mut num_retries = 0;
        let max_retries = 5;
        loop {
            num_retries += 1;
            if num_retries > max_retries {
                return Err(anyhow!("Max retries reached"));
            }
            let (sig, res) = self
                .client
                .send_retry_and_confirm_transaction(tx.clone(), None, false)
                .await?;
            if let Some(Err(TransactionError::BlockhashNotFound)) = res {
                continue;
            } else {
                return Ok((sig, res));
            }
        }
    }

    pub async fn fetch_all_prices(
        &mut self,
        reserves: &[Reserve],
        usd_mint: &Pubkey,
    ) -> Result<Prices> {
        let mut mints = reserves
            .iter()
            .map(|x| x.liquidity.mint_pubkey)
            .collect::<HashSet<Pubkey>>();

        if let Some(c) = &self.rebalance_config {
            mints.insert(c.base_token);
            mints.insert(c.usdc_mint);
        };
        mints.insert(WRAPPED_SOL_MINT);

        // Convert mints to vec
        let mints = mints.into_iter().collect::<Vec<Pubkey>>();

        // TOOD: fix amount to be per token
        let amount = 100.0;
        px::fetch_prices(&mints, usd_mint, amount).await
    }

    /// Build liquidation instructions with optional post-farm refresh.
    ///
    /// # Arguments
    /// * `skip_post_farm_refresh` - If true, omits post-liquidation farm refresh instructions
    ///   to reduce transaction size. Use for flash loan liquidations.
    #[allow(clippy::too_many_arguments)]
    pub async fn liquidate_obligation_and_redeem_reserve_collateral_ixns(
        &self,
        lending_market: StateWithKey<LendingMarket>,
        debt_reserve: StateWithKey<Reserve>,
        coll_reserve: StateWithKey<Reserve>,
        obligation: StateWithKey<Obligation>,
        liquidity_amount: u64,
        min_acceptable_received_coll_amount: u64,
        max_allowed_ltv_override_pct_opt: Option<u64>,
        skip_post_farm_refresh: bool,
    ) -> Result<Vec<Instruction>> {
        let liquidate_ix = instructions::liquidate_obligation_and_redeem_reserve_collateral_ix(
            &self.program_id,
            lending_market,
            debt_reserve.clone(),
            coll_reserve.clone(),
            &self.liquidator,
            obligation.key,
            liquidity_amount,
            min_acceptable_received_coll_amount,
            max_allowed_ltv_override_pct_opt,
        );

        let (pre_instructions, post_instructions) = self
            .wrap_obligation_instruction_with_farms(
                &[&coll_reserve, &debt_reserve],
                &[ReserveFarmKind::Collateral, ReserveFarmKind::Debt],
                &obligation,
                &self.liquidator.wallet.clone(),
                skip_post_farm_refresh,
            )
            .await;

        let mut instructions = vec![];
        for ix in pre_instructions {
            instructions.push(ix.instruction);
        }
        instructions.push(liquidate_ix.instruction);
        for ix in post_instructions {
            instructions.push(ix.instruction);
        }

        Ok(instructions)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn liquidate_obligation_and_redeem_reserve_collateral(
        &mut self,
        lending_market: StateWithKey<LendingMarket>,
        debt_reserve: StateWithKey<Reserve>,
        coll_reserve: StateWithKey<Reserve>,
        obligation: StateWithKey<Obligation>,
        liquidity_amount: u64,
        min_acceptable_received_coll_amount: u64,
        max_allowed_ltv_override_pct_opt: Option<u64>,
    ) -> Result<VersionedTransaction> {
        let instructions = self
            .liquidate_obligation_and_redeem_reserve_collateral_ixns(
                lending_market,
                debt_reserve,
                coll_reserve,
                obligation,
                liquidity_amount,
                min_acceptable_received_coll_amount,
                max_allowed_ltv_override_pct_opt,
                false, // Include post farm refresh for non-flash loan liquidations
            )
            .await?;

        let txn = self.client.tx_builder().add_ixs(instructions);
        let txn_b64 = txn.to_base64();
        println!(
            "Simulation: https://explorer.solana.com/tx/inspector?message={}",
            urlencoding::encode(&txn_b64)
        );

        txn.build_with_budget_and_fee(&[]).await.map_err(Into::into)
    }

    /// Wraps an obligation instruction with necessary refresh/farm instructions.
    ///
    /// # Arguments
    /// * `skip_post_farm_refresh` - If true, skips post-liquidation farm refresh to reduce tx size.
    ///   Use this for flash loan liquidations where the tx is already at the size limit.
    pub async fn wrap_obligation_instruction_with_farms(
        &self,
        reserve_accts: &[&StateWithKey<Reserve>],
        farm_modes: &[ReserveFarmKind],
        obligation: &StateWithKey<Obligation>,
        payer: &Arc<Keypair>,
        skip_post_farm_refresh: bool,
    ) -> (Vec<InstructionBlocks>, Vec<InstructionBlocks>) {
        // If has farms, also do init farm obligations
        // Always do refresh_reserve
        // Always do refresh_obligation
        // If has farms, also do refresh farms (pre)
        // Then this ix
        // If has farms, also do refresh farms (post) - unless skip_post_farm_refresh

        let mut pre_instructions = vec![];
        let mut post_instructions = vec![];

        let wrap_start = std::time::Instant::now();
        let obligation_state = *(obligation.state.borrow());
        let obligation_address = obligation.key;

        let rpc_start = std::time::Instant::now();
        let (deposit_reserves, borrow_reserves, referrer_token_states) = self
            .get_obligation_reserves_and_referrer_token_states(&obligation_state)
            .await;
        tracing::debug!("  get_obligation_reserves_and_referrer_token_states: {}ms", rpc_start.elapsed().as_millis());

        let mut unique_reserves = deposit_reserves
            .iter()
            .chain(borrow_reserves.iter())
            .filter_map(|x| *x)
            .collect::<Vec<Pubkey>>();
        unique_reserves.sort();
        unique_reserves.dedup();

        let instruction_reserves = reserve_accts.iter().map(|x| x.key).collect::<Vec<Pubkey>>();

        // 1. Build init_obligation_farm if necessary
        for reserve in reserve_accts {
            let (farm_debt, farm_collateral) = {
                let reserve_state = reserve.state.borrow();
                (
                    reserve_state.get_farm(ReserveFarmKind::Debt),
                    reserve_state.get_farm(ReserveFarmKind::Collateral),
                )
            };
            let farm_start = std::time::Instant::now();
            let (obligation_farm_debt, obligation_farm_coll) =
                obligation_farms(&self.client, farm_debt, farm_collateral, obligation_address)
                    .await;
            tracing::debug!("  obligation_farms: {}ms", farm_start.elapsed().as_millis());

            if farm_debt != Pubkey::default() && obligation_farm_debt.is_none() {
                let init_obligation_farm_ix = instructions::init_obligation_farm_for_reserve_ix(
                    &self.program_id,
                    reserve,
                    farm_debt,
                    &obligation_address,
                    &obligation_state.owner,
                    payer,
                    ReserveFarmKind::Debt,
                );
                println!(
                    "Adding pre-ixn init_obligation_farm_ix current {:?} debt ",
                    farm_debt
                );
                pre_instructions.push(init_obligation_farm_ix.clone());
            }

            if farm_collateral != Pubkey::default() && obligation_farm_coll.is_none() {
                let init_obligation_farm_ix = instructions::init_obligation_farm_for_reserve_ix(
                    &self.program_id,
                    reserve,
                    farm_collateral,
                    &obligation_address,
                    &obligation_state.owner,
                    payer,
                    ReserveFarmKind::Collateral,
                );
                println!(
                    "Adding pre-ixn init_obligation_farm_ix current {:?} coll",
                    farm_collateral
                );
                pre_instructions.push(init_obligation_farm_ix.clone());
            }
        }

        // 2. Build Refresh Reserve (for the non-instruction reserves - i.e. deposit, borrow)
        let reserve_fetch_start = std::time::Instant::now();
        for reserve_acc in unique_reserves {
            if instruction_reserves.contains(&reserve_acc) {
                continue;
            }
            let reserve: Reserve = self.client.get_anchor_account(&reserve_acc).await.unwrap();
            let refresh_reserve_ix = instructions::refresh_reserve_ix(
                &self.program_id,
                reserve,
                &reserve_acc,
                payer.clone(),
            );
            println!("Adding pre-ixn refresh_reserve unique {:?}", reserve_acc);
            pre_instructions.push(refresh_reserve_ix);
        }
        tracing::debug!("  fetch unique reserves: {}ms", reserve_fetch_start.elapsed().as_millis());

        // 3. Build Refresh Reserve (for the current instruction - i.e. deposit, borrow)
        let ix_reserve_start = std::time::Instant::now();
        for reserve_acc in instruction_reserves {
            let reserve: Reserve = self.client.get_anchor_account(&reserve_acc).await.unwrap();
            let refresh_reserve_ix = instructions::refresh_reserve_ix(
                &self.program_id,
                reserve,
                &reserve_acc,
                payer.clone(),
            );
            println!("Adding pre-ixn refresh_reserve current {:?}", reserve_acc);
            pre_instructions.push(refresh_reserve_ix);
        }
        tracing::debug!("  fetch instruction reserves: {}ms", ix_reserve_start.elapsed().as_millis());

        // 4. Build Refresh Obligation
        let refresh_obligation_ix = instructions::refresh_obligation_ix(
            &self.program_id,
            obligation_state.lending_market,
            obligation_address,
            deposit_reserves,
            borrow_reserves,
            referrer_token_states,
            payer.clone(),
        );

        println!("Adding pre-ixn refresh_obligation");
        pre_instructions.push(refresh_obligation_ix);

        let farm_refresh_start = std::time::Instant::now();
        for (reserve_acc, farm_mode) in reserve_accts.iter().zip(farm_modes.iter()) {
            let reserve: Reserve = self
                .client
                .get_anchor_account(&reserve_acc.key)
                .await
                .unwrap();

            let farm = reserve.get_farm(*farm_mode);
            println!(
                "Reserve {} ({:?}): farm={} (has_farm={})",
                reserve_acc.key,
                farm_mode,
                farm,
                farm != Pubkey::default()
            );

            // 5.1 Build Refresh Obligation Farms
            if farm != Pubkey::default() {
                let refresh_farms_ix = instructions::refresh_obligation_farm_for_reserve_ix(
                    &self.program_id,
                    reserve_acc,
                    farm,
                    obligation_address,
                    payer,
                    *farm_mode,
                );

                println!("pre_ixs refresh_obligation_farms {:?}", farm);

                pre_instructions.push(refresh_farms_ix.clone());
                // Skip post farm refresh for flash loan liquidations to fit in tx size limit
                if !skip_post_farm_refresh {
                    post_instructions.push(refresh_farms_ix);
                }
            }
        }

        tracing::debug!("  farm refresh reserves: {}ms", farm_refresh_start.elapsed().as_millis());
        println!(
            "wrap_obligation_instruction_with_farms: {} pre_ixs, {} post_ixs (skip_post={})",
            pre_instructions.len(),
            post_instructions.len(),
            skip_post_farm_refresh
        );
        tracing::info!("wrap_obligation_instruction_with_farms total: {}ms", wrap_start.elapsed().as_millis());

        (pre_instructions, post_instructions)
    }

    pub async fn get_obligation_reserves_and_referrer_token_states(
        &self,
        obligation: &Obligation,
    ) -> (
        Vec<Option<Pubkey>>,
        Vec<Option<Pubkey>>,
        Vec<Option<Pubkey>>,
    ) {
        let deposit_reserves: Vec<Option<Pubkey>> = obligation
            .deposits
            .iter()
            .filter(|x| x.deposit_reserve != Pubkey::default())
            .map(|x| Some(x.deposit_reserve))
            .collect();

        let borrow_reserves: Vec<Option<Pubkey>> = obligation
            .borrows
            .iter()
            .filter(|x| x.borrow_reserve != Pubkey::default())
            .map(|x| Some(x.borrow_reserve))
            .collect();

        let referrer_token_states: Vec<Option<Pubkey>> = if obligation.has_referrer() {
            let mut vec = Vec::with_capacity(borrow_reserves.len());

            for borrow_reserve in borrow_reserves.iter() {
                match borrow_reserve {
                    Some(borrow_reserve) => {
                        let reserve_account: Reserve = self
                            .client
                            .get_anchor_account(borrow_reserve)
                            .await
                            .unwrap();

                        vec.push(Some(get_referrer_token_state_key(
                            &obligation.referrer,
                            &reserve_account.liquidity.mint_pubkey,
                        )));
                    }
                    None => {}
                }
            }
            vec
        } else {
            Vec::new()
        };

        (deposit_reserves, borrow_reserves, referrer_token_states)
    }
}

pub fn get_referrer_token_state_key(referrer: &Pubkey, mint: &Pubkey) -> Pubkey {
    let (referrer_token_state_key, _referrer_token_state_bump) = Pubkey::find_program_address(
        &[
            seeds::BASE_SEED_REFERRER_TOKEN_STATE,
            referrer.as_ref(),
            mint.as_ref(),
        ],
        &kamino_lending::id(),
    );

    referrer_token_state_key
}

pub async fn obligation_farms(
    client: &OrbitLink<RpcClient, Keypair>,
    farm_debt: Pubkey,
    farm_collateral: Pubkey,
    obligation_address: Pubkey,
) -> (
    Option<StateWithKey<farms::state::UserState>>,
    Option<StateWithKey<farms::state::UserState>>,
) {
    let (obligation_farm_debt, _) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            farm_debt.as_ref(),
            obligation_address.as_ref(),
        ],
        &farms::ID,
    );
    let (obligation_farm_coll, _) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            farm_collateral.as_ref(),
            obligation_address.as_ref(),
        ],
        &farms::ID,
    );

    let obligation_farm_debt_account = if farm_debt == Pubkey::default() {
        None
    } else {
        match find_account(&client.client, obligation_farm_debt).await {
            Ok(Some(_)) => {
                match client
                    .get_anchor_account::<farms::state::UserState>(&obligation_farm_debt)
                    .await
                {
                    Ok(acc) => Some(StateWithKey::new(acc, obligation_farm_debt)),
                    Err(_) => None,
                }
            }
            _ => None, // Ok(None) or Err(_) - account doesn't exist or error
        }
    };

    let obligation_farm_coll_account = if farm_collateral == Pubkey::default() {
        None
    } else {
        match find_account(&client.client, obligation_farm_coll).await {
            Ok(Some(_)) => {
                match client
                    .get_anchor_account::<farms::state::UserState>(&obligation_farm_coll)
                    .await
                {
                    Ok(acc) => Some(StateWithKey::new(acc, obligation_farm_coll)),
                    Err(_) => None,
                }
            }
            _ => None, // Ok(None) or Err(_) - account doesn't exist or error
        }
    };

    (obligation_farm_debt_account, obligation_farm_coll_account)
}

pub mod utils {
    use super::*;

    pub async fn fetch_markets_and_reserves(
        client: &Arc<KlendClient>,
        markets: &[Pubkey],
    ) -> anyhow::Result<HashMap<Pubkey, MarketAccounts>> {
        let futures = markets
            .iter()
            .map(|market| {
                let client = client.clone();
                let market = *market;
                task::spawn(async move { client.fetch_market_and_reserves(&market).await })
            })
            .collect::<Vec<_>>();

        let results = futures::future::join_all(futures).await;

        let mut map = HashMap::new();
        for (i, result) in results.into_iter().enumerate() {
            let r = result??;
            map.insert(markets[i], r);
        }
        Ok(map)
    }
}

pub mod rpc {
    use anchor_client::solana_client::{
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
    };
    use anchor_lang::{Discriminator, Owner};
    use bytemuck::{from_bytes, AnyBitPattern};
    use orbit_link::OrbitLink;
    use solana_account_decoder::UiAccountEncoding;
    use solana_rpc_client::nonblocking::rpc_client::RpcClient;

    use super::*;

    pub async fn get_zero_copy_pa<Acc>(
        client: &OrbitLink<RpcClient, Keypair>,
        program_id: &Pubkey,
        filters: &[RpcFilterType],
    ) -> Result<Vec<(Pubkey, Acc)>>
    where
        Acc: AnyBitPattern + Owner + Discriminator,
    {
        let size = u64::try_from(std::mem::size_of::<Acc>() + 8).unwrap();
        let discrim_memcmp = RpcFilterType::Memcmp(Memcmp::new(
            0,
            MemcmpEncodedBytes::Bytes(Acc::discriminator().to_vec()),
        ));
        let mut all_filters = vec![RpcFilterType::DataSize(size), discrim_memcmp];
        for f in filters {
            all_filters.push(f.clone());
        }
        let num_filters = all_filters.len();
        let config = RpcProgramAccountsConfig {
            filters: Some(all_filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64Zstd),
                ..RpcAccountInfoConfig::default()
            },
            ..RpcProgramAccountsConfig::default()
        };

        // Retry with exponential backoff for rate limiting
        let mut retries = 0;
        let max_retries = 5;
        let type_name = std::any::type_name::<Acc>().split("::").last().unwrap_or("Unknown");
        tracing::info!(
            "RPC: getProgramAccounts for {} (size={} bytes, filters={})",
            type_name,
            size,
            num_filters
        );
        let start = std::time::Instant::now();
        let accs = loop {
            match client
                .client
                .get_program_accounts_with_config(program_id, config.clone())
                .await
            {
                Ok(accs) => {
                    tracing::info!(
                        "RPC: getProgramAccounts for {} completed in {:?}, got {} accounts",
                        type_name,
                        start.elapsed(),
                        accs.len()
                    );
                    break accs;
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("429") && retries < max_retries {
                        retries += 1;
                        let delay = std::time::Duration::from_millis(500 * (1 << retries));
                        tracing::warn!(
                            "RPC: getProgramAccounts for {} rate limited after {:?}, retrying in {:?} (attempt {}/{})",
                            type_name,
                            start.elapsed(),
                            delay,
                            retries,
                            max_retries
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        };

        let parsed_accounts = accs
            .into_iter()
            .map(|(pubkey, account)| {
                let data: &[u8] = &account.data;
                let acc: &Acc = from_bytes(&data[8..]);
                Ok((pubkey, *acc))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(parsed_accounts)
    }
}
