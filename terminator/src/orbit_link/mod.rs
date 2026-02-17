//! Vendored subset of orbit-link — convenience layer over Solana's async client for Anchor programs.
//! Original: https://github.com/Kamino-Finance/orbit-link

use std::iter;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use anchor_client::{
    anchor_lang::AccountDeserialize,
    solana_sdk::{
        address_lookup_table_account::AddressLookupTableAccount,
        commitment_config::CommitmentConfig,
        instruction::Instruction,
        message::{v0, VersionedMessage},
        pubkey::Pubkey,
        signature::Signature,
        signer::{Signer, SignerError},
        transaction::{TransactionError, VersionedTransaction},
    },
};
use errors::ErrorKind;
use futures::future::join_all;

pub mod async_client;
pub mod consts;
pub mod errors;
pub mod fees;
pub mod tx_builder;

pub use consts::*;
use itertools::izip;
use solana_client::rpc_client::SerializableTransaction;
use tracing::{debug, warn};

pub type Result<T> = std::result::Result<T, errors::ErrorKind>;

/// Transaction result. `Ok` if the transaction was successful, `Err` from the transaction otherwise.
type TransactionResult = std::result::Result<(), TransactionError>;

const FEE_CACHE_TTL_S: u64 = 60;

// Min to 2000 lamports per 200_000 CU (default 1 ix transaction)
// 2000 * 1M / 200_000 = 10000
const MIN_FEE: u64 = 10000;

struct FeeCache {
    pub priority_fee: AtomicU64,
    pub creation: Instant,
    pub delta_s: AtomicU64,
}

impl Clone for FeeCache {
    fn clone(&self) -> Self {
        FeeCache {
            priority_fee: AtomicU64::new(
                self.priority_fee.load(std::sync::atomic::Ordering::Relaxed),
            ),
            creation: self.creation,
            delta_s: AtomicU64::new(self.delta_s.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }
}

pub struct OrbitLink<T, S>
where
    T: async_client::AsyncClient,
    S: Signer,
{
    pub client: T,
    payer: Option<S>,
    payer_pubkey: Option<Pubkey>,
    lookup_tables: Vec<AddressLookupTableAccount>,
    commitment_config: CommitmentConfig,
    fee_cache: FeeCache,
}

impl<T, S> OrbitLink<T, S>
where
    T: async_client::AsyncClient,
    S: Signer,
{
    #[allow(clippy::result_large_err)]
    pub fn new(
        client: T,
        payer: Option<S>,
        lookup_tables: impl Into<Option<Vec<AddressLookupTableAccount>>>,
        commitment_config: CommitmentConfig,
        payer_pubkey: Option<Pubkey>,
    ) -> Result<Self> {
        let lookup_tables: Option<Vec<AddressLookupTableAccount>> = lookup_tables.into();

        if payer.is_none() && payer_pubkey.is_none() {
            return Err(errors::ErrorKind::SignerError(SignerError::InvalidInput(
                "No payer nor payer_pubkey provided".to_string(),
            )));
        }

        let fee_cache = FeeCache {
            priority_fee: AtomicU64::new(MIN_FEE),
            creation: Instant::now(),
            delta_s: AtomicU64::new(0),
        };

        Ok(OrbitLink {
            client,
            payer,
            payer_pubkey,
            lookup_tables: lookup_tables.unwrap_or_default(),
            commitment_config,
            fee_cache,
        })
    }

    pub async fn refresh_fee_cache(&self) -> Result<()> {
        let solana_min_fee = self
            .client
            .get_recommended_micro_lamport_fee()
            .await
            .unwrap_or(0);
        let solana_compass_median_fee = fees::solanacompass::get_last_5_min_median_fee()
            .await
            .unwrap_or(0);
        const BASE_FEE: u64 = 25_000;
        const MAX_FEE: u64 = 6 * BASE_FEE;
        let fee = MIN_FEE
            .max(solana_min_fee)
            .max((solana_compass_median_fee * 95) / 100)
            .min(MAX_FEE);
        debug!(solana_min_fee, solana_compass_median_fee, fee);
        self.fee_cache
            .priority_fee
            .store(fee, std::sync::atomic::Ordering::Relaxed);
        self.fee_cache.delta_s.store(
            self.fee_cache.creation.elapsed().as_secs(),
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }

    pub async fn refresh_fee_cache_if_needed(&self) -> Result<()> {
        let delta_ts_now = self.fee_cache.creation.elapsed().as_secs();
        let delta_ts_pre = self
            .fee_cache
            .delta_s
            .load(std::sync::atomic::Ordering::Relaxed);
        let delta_ts = delta_ts_now - delta_ts_pre;
        if delta_ts > FEE_CACHE_TTL_S {
            self.refresh_fee_cache().await?;
        }
        Ok(())
    }

    pub fn get_recommended_micro_lamport_fee(&self) -> u64 {
        self.fee_cache
            .priority_fee
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn payer_pubkey(&self) -> Pubkey {
        match (&self.payer, self.payer_pubkey) {
            (Some(p), _) => p.pubkey(),
            (_, Some(p)) => p,
            _ => unreachable!("A payer or payer_pubkey should be provided"),
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn payer(&self) -> Result<&S> {
        self.payer
            .as_ref()
            .ok_or(errors::ErrorKind::SignerError(SignerError::InvalidInput(
                "No payer provided".to_string(),
            )))
    }

    pub fn add_lookup_table(&mut self, table: AddressLookupTableAccount) {
        self.lookup_tables.push(table);
    }

    pub async fn get_anchor_account<AccDeser: AccountDeserialize>(
        &self,
        pubkey: &Pubkey,
    ) -> Result<AccDeser> {
        let account = self.client.get_account(pubkey).await?;
        let mut data: &[u8] = &account.data;
        Ok(AccDeser::try_deserialize(&mut data)?)
    }

    pub fn tx_builder(&self) -> tx_builder::TxBuilder<T, S> {
        tx_builder::TxBuilder::new(self)
    }

    pub async fn create_tx(
        &self,
        instructions: &[Instruction],
        extra_signers: &[&dyn Signer],
    ) -> Result<VersionedTransaction> {
        let mut signers: Vec<&dyn Signer> = Vec::with_capacity(extra_signers.len() + 1);

        let payer = self.payer.as_ref().ok_or(errors::ErrorKind::SignerError(
            SignerError::InvalidInput("No payer provided".to_string()),
        ))?;

        signers.push(payer);
        signers.extend_from_slice(extra_signers);

        Ok(VersionedTransaction::try_new(
            VersionedMessage::V0(v0::Message::try_compile(
                &payer.pubkey(),
                instructions,
                &self.lookup_tables,
                self.client.get_latest_blockhash().await?,
            )?),
            &signers,
        )?)
    }

    pub async fn create_tx_with_extra_lookup_tables(
        &self,
        instructions: &[Instruction],
        extra_signers: &[&dyn Signer],
        lookup_tables_extra: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let mut signers: Vec<&dyn Signer> = Vec::with_capacity(extra_signers.len() + 1);

        match self.payer {
            Some(ref payer) => {
                signers.push(payer);
            }
            None => {
                return Err(errors::ErrorKind::SignerError(SignerError::InvalidInput(
                    "No payer provided".to_string(),
                )));
            }
        }

        signers.extend_from_slice(extra_signers);

        let mut lookup_tables = self.lookup_tables.clone();
        lookup_tables.extend_from_slice(lookup_tables_extra);

        Ok(VersionedTransaction::try_new(
            VersionedMessage::V0(v0::Message::try_compile(
                &self.payer.as_ref().unwrap().pubkey(),
                instructions,
                &lookup_tables,
                self.client.get_latest_blockhash().await?,
            )?),
            &signers,
        )?)
    }

    pub async fn send_transaction(&self, tx: &VersionedTransaction) -> Result<Signature> {
        self.client.send_transaction(tx).await
    }

    pub async fn send_transaction_no_retry(&self, tx: &VersionedTransaction) -> Result<Signature> {
        self.client.send_transaction_no_retry(tx).await
    }

    pub async fn send_transaction_no_retry_no_preflight(
        &self,
        tx: &VersionedTransaction,
    ) -> Result<Signature> {
        self.client.send_transaction_no_retry_no_preflight(tx).await
    }

    pub async fn send_retry_and_confirm_transactions(
        &self,
        transactions: &[VersionedTransaction],
        timeout: Option<Duration>,
        skip_all_preflights: bool,
    ) -> Result<Vec<(Signature, Option<TransactionResult>)>> {
        let start = Instant::now();

        let blockhash = transactions
            .get(0)
            .ok_or(ErrorKind::NoTransactions)?
            .get_recent_blockhash();

        let signatures = if skip_all_preflights {
            join_all(
                transactions
                    .iter()
                    .map(|tx| self.send_transaction_no_retry_no_preflight(tx)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<Signature>>>()?
        } else {
            join_all(
                transactions
                    .iter()
                    .map(|tx| self.send_transaction_no_retry(tx)),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<Signature>>>()?
        };

        let mut tx_to_confirm: Vec<TransactionAndStatus> = izip!(
            transactions.iter(),
            signatures.into_iter(),
            std::iter::repeat(None),
        )
        .map(|(tx, sig, result)| TransactionAndStatus { tx, sig, result })
        .collect();

        let timeout = timeout.unwrap_or(Duration::from_millis(MAX_TIMEOUT_TX_SEND_MS));
        while start.elapsed() < timeout {
            if !self.client.is_blockhash_valid(blockhash).await? {
                warn!("Blockhash is not valid anymore, stopping retry send loop");
                break;
            }
            let (remaining_signatures, mut remaining_tx_to_confirm) =
                Self::get_remaining_signatures_and_tx_to_confirm(&mut tx_to_confirm);
            let statuses = self
                .client
                .get_signature_statuses(&remaining_signatures)
                .await?;

            for (to_set, status) in remaining_tx_to_confirm.iter_mut().zip(statuses).filter_map(
                |(TransactionAndStatus { result: to_set, .. }, status)| status.map(|s| (to_set, s)),
            ) {
                if let Some(err) = status.err {
                    *to_set = Some(Err(err));
                } else if status.satisfies_commitment(CommitmentConfig::processed()) {
                    *to_set = Some(Ok(()));
                }
            }

            let txs_to_retry = remaining_tx_to_confirm
                .iter()
                .filter(|TransactionAndStatus { result, .. }| result.is_none())
                .map(|TransactionAndStatus { tx, .. }| tx);

            if txs_to_retry.clone().count() == 0 {
                break;
            }

            let _ =
                join_all(txs_to_retry.map(|tx| self.send_transaction_no_retry_no_preflight(tx)))
                    .await;

            tokio::time::sleep(std::time::Duration::from_millis(
                DELAY_MS_BETWEEN_TX_SEND_RETRY,
            ))
            .await;
        }

        let mut tx_to_confirm: Vec<(Signature, Option<TransactionResult>)> = tx_to_confirm
            .into_iter()
            .map(|TransactionAndStatus { sig, .. }| sig)
            .zip(iter::repeat(None))
            .collect();

        let remaining_timeout_ms: u64 = timeout
            .saturating_sub(start.elapsed())
            .as_millis()
            .try_into()
            .unwrap();
        let nb_attempts = timeout_to_retry_count(remaining_timeout_ms).max(1);
        self.confirm_transactions(&mut tx_to_confirm, self.commitment_config, nb_attempts)
            .await?;

        Ok(tx_to_confirm)
    }

    pub async fn send_retry_and_confirm_transaction(
        &self,
        transaction: VersionedTransaction,
        timeout: Option<Duration>,
        skip_preflight: bool,
    ) -> Result<(Signature, Option<TransactionResult>)> {
        let res = self
            .send_retry_and_confirm_transactions(&[transaction], timeout, skip_preflight)
            .await?;
        Ok(res
            .into_iter()
            .next()
            .expect("Sent and confirm one transaction, expect one result"))
    }

    fn get_remaining_signatures_to_confirm(
        tx_to_confirm: &mut [(Signature, Option<TransactionResult>)],
    ) -> (
        Vec<Signature>,
        Vec<&mut (Signature, Option<TransactionResult>)>,
    ) {
        let remaining_to_confirm: Vec<_> = tx_to_confirm
            .iter_mut()
            .filter(|(_, result)| result.is_none())
            .collect();
        let remaining_signatures: Vec<_> =
            remaining_to_confirm.iter().map(|(sig, _)| *sig).collect();
        (remaining_signatures, remaining_to_confirm)
    }

    fn get_remaining_signatures_and_tx_to_confirm<'a, 'b>(
        tx_to_confirm: &'a mut [TransactionAndStatus<'b>],
    ) -> (Vec<Signature>, Vec<&'a mut TransactionAndStatus<'b>>) {
        let remaining_to_confirm: Vec<_> = tx_to_confirm
            .iter_mut()
            .filter(|TransactionAndStatus { result, .. }| result.is_none())
            .collect();
        let remaining_signatures: Vec<_> = remaining_to_confirm
            .iter()
            .map(|TransactionAndStatus { sig, .. }| *sig)
            .collect();
        (remaining_signatures, remaining_to_confirm)
    }

    async fn confirm_transactions(
        &self,
        tx_to_confirm: &mut [(Signature, Option<TransactionResult>)],
        confirmation_level: CommitmentConfig,
        nb_attempts: usize,
    ) -> Result<()> {
        for _retry in 0..nb_attempts {
            let (remaining_signatures, mut remaining_tx_to_confirm) =
                Self::get_remaining_signatures_to_confirm(tx_to_confirm);
            if remaining_signatures.is_empty() {
                return Ok(());
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(
                    DEFAULT_STATUS_FETCH_DELAY_MS,
                ))
                .await;
            }
            let statuses = self
                .client
                .get_signature_statuses(&remaining_signatures)
                .await?;
            for (to_set, status) in remaining_tx_to_confirm
                .iter_mut()
                .zip(statuses)
                .filter_map(|((_sig, to_set), status)| status.map(|s| (to_set, s)))
            {
                if let Some(err) = status.err {
                    *to_set = Some(Err(err));
                } else if status.satisfies_commitment(confirmation_level) {
                    *to_set = Some(Ok(()));
                }
            }
        }
        Ok(())
    }
}

struct TransactionAndStatus<'a> {
    tx: &'a VersionedTransaction,
    sig: Signature,
    result: Option<TransactionResult>,
}
