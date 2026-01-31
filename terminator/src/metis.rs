//! Metis/Jupiter Swap API client for Triton
//!
//! This module provides swap functionality via Triton's Metis API,
//! which is Jupiter-compatible and returns address lookup tables.

use anchor_lang::prelude::Pubkey;
use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_sdk::instruction::{AccountMeta, Instruction};
use tracing::{debug, info};

/// Metis API configuration
#[derive(Clone, Debug)]
pub struct MetisConfig {
    /// Base URL for Metis API (e.g., "https://swissborg.rpcpool.com/<token>/metis")
    pub endpoint: String,
}

impl MetisConfig {
    pub fn from_env() -> Result<Self> {
        let endpoint = std::env::var("METIS_ENDPOINT")
            .map_err(|_| anyhow::anyhow!("METIS_ENDPOINT environment variable not set"))?;
        Ok(Self { endpoint })
    }
}

/// Metis API client
pub struct MetisClient {
    config: MetisConfig,
    client: Client,
}

impl MetisClient {
    pub fn new(config: MetisConfig) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
        Self { config, client }
    }

    /// Get a swap quote
    pub async fn get_quote(&self, request: &QuoteRequest) -> Result<QuoteResponse> {
        let url = format!("{}/quote", self.config.endpoint);

        let mut query_params = vec![
            ("inputMint", request.input_mint.to_string()),
            ("outputMint", request.output_mint.to_string()),
            ("amount", request.amount.to_string()),
        ];

        if let Some(slippage) = request.slippage_bps {
            query_params.push(("slippageBps", slippage.to_string()));
        }
        if let Some(only_direct) = request.only_direct_routes {
            query_params.push(("onlyDirectRoutes", only_direct.to_string()));
        }
        if let Some(max_accounts) = request.max_accounts {
            query_params.push(("maxAccounts", max_accounts.to_string()));
        }

        debug!("Metis quote request: {} -> {} amount={}",
            request.input_mint, request.output_mint, request.amount);

        let response = self.client
            .get(&url)
            .query(&query_params)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Metis quote failed: {} - {}", status, body));
        }

        let quote: QuoteResponse = response.json().await?;
        info!("Metis quote: in={} out={} price_impact={}%",
            quote.in_amount, quote.out_amount, quote.price_impact_pct);

        Ok(quote)
    }

    /// Get swap instructions from a quote
    pub async fn get_swap_instructions(
        &self,
        quote: &QuoteResponse,
        user_public_key: &Pubkey,
    ) -> Result<SwapInstructionsResponse> {
        let url = format!("{}/swap-instructions", self.config.endpoint);

        let request = SwapInstructionsRequest {
            quote_response: quote.clone(),
            user_public_key: user_public_key.to_string(),
            wrap_and_unwrap_sol: true,
            compute_unit_price_micro_lamports: Some(100000), // Auto compute budget
            dynamic_compute_unit_limit: true,
        };

        debug!("Metis swap-instructions request for user {}", user_public_key);

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Metis swap-instructions failed: {} - {}", status, body));
        }

        let swap_ixs: SwapInstructionsResponse = response.json().await?;
        info!("Metis swap instructions: {} ALTs, swap_ix accounts={}",
            swap_ixs.address_lookup_table_addresses.len(),
            swap_ixs.swap_instruction.accounts.len());

        Ok(swap_ixs)
    }
}

/// Quote request parameters
#[derive(Debug, Clone)]
pub struct QuoteRequest {
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub amount: u64,
    pub slippage_bps: Option<u16>,
    pub only_direct_routes: Option<bool>,
    pub max_accounts: Option<u8>,
}

/// Quote response from Metis API
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteResponse {
    pub input_mint: String,
    pub in_amount: String,
    pub output_mint: String,
    pub out_amount: String,
    pub other_amount_threshold: String,
    pub swap_mode: String,
    pub slippage_bps: u16,
    pub price_impact_pct: String,
    pub route_plan: Vec<RoutePlanStep>,
    #[serde(default)]
    pub context_slot: Option<u64>,
    #[serde(default)]
    pub time_taken: Option<f64>,
}

impl QuoteResponse {
    pub fn in_amount_u64(&self) -> u64 {
        self.in_amount.parse().unwrap_or(0)
    }

    pub fn out_amount_u64(&self) -> u64 {
        self.out_amount.parse().unwrap_or(0)
    }
}

/// Route plan step
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutePlanStep {
    pub swap_info: SwapInfo,
    pub percent: u8,
}

/// Swap info within a route step
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInfo {
    pub amm_key: String,
    pub label: Option<String>,
    pub input_mint: String,
    pub output_mint: String,
    pub in_amount: String,
    pub out_amount: String,
    pub fee_amount: String,
    pub fee_mint: String,
}

/// Request for swap instructions
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructionsRequest {
    pub quote_response: QuoteResponse,
    pub user_public_key: String,
    #[serde(default)]
    pub wrap_and_unwrap_sol: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_unit_price_micro_lamports: Option<u64>,
    #[serde(default)]
    pub dynamic_compute_unit_limit: bool,
}

/// Response containing swap instructions
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapInstructionsResponse {
    #[serde(default)]
    pub compute_budget_instructions: Vec<InstructionData>,
    #[serde(default)]
    pub setup_instructions: Vec<InstructionData>,
    pub swap_instruction: InstructionData,
    #[serde(default)]
    pub cleanup_instruction: Option<InstructionData>,
    #[serde(default)]
    pub address_lookup_table_addresses: Vec<String>,
    #[serde(default)]
    pub other_instructions: Vec<InstructionData>,
}

/// Instruction data from Metis API
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstructionData {
    pub program_id: String,
    pub accounts: Vec<AccountMetaData>,
    pub data: String,
}

impl InstructionData {
    /// Convert to Solana Instruction
    pub fn to_instruction(&self) -> Result<Instruction> {
        let program_id: Pubkey = self.program_id.parse()
            .map_err(|e| anyhow::anyhow!("Invalid program_id {}: {}", self.program_id, e))?;

        let accounts: Result<Vec<AccountMeta>> = self.accounts
            .iter()
            .map(|a| {
                let pubkey: Pubkey = a.pubkey.parse()
                    .map_err(|e| anyhow::anyhow!("Invalid pubkey {}: {}", a.pubkey, e))?;
                Ok(AccountMeta {
                    pubkey,
                    is_signer: a.is_signer,
                    is_writable: a.is_writable,
                })
            })
            .collect();

        let data = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &self.data
        ).map_err(|e| anyhow::anyhow!("Invalid instruction data: {}", e))?;

        Ok(Instruction {
            program_id,
            accounts: accounts?,
            data,
        })
    }
}

/// Account metadata
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountMetaData {
    pub pubkey: String,
    pub is_signer: bool,
    pub is_writable: bool,
}

// Global client instance
use std::sync::OnceLock;
static METIS_CLIENT: OnceLock<MetisClient> = OnceLock::new();

fn get_metis_client() -> &'static MetisClient {
    METIS_CLIENT.get_or_init(|| {
        let config = MetisConfig::from_env().expect("Failed to load Metis config");
        info!("Initializing Metis client with endpoint: {}", config.endpoint);
        MetisClient::new(config)
    })
}

/// Convenience function to get a quote
pub async fn get_quote(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    slippage_bps: Option<u16>,
    only_direct_routes: Option<bool>,
    max_accounts: Option<u8>,
) -> Result<QuoteResponse> {
    let request = QuoteRequest {
        input_mint: *input_mint,
        output_mint: *output_mint,
        amount,
        slippage_bps,
        only_direct_routes,
        max_accounts,
    };
    get_metis_client().get_quote(&request).await
}

/// Convenience function to get swap instructions
pub async fn get_swap_instructions(
    quote: &QuoteResponse,
    user_public_key: &Pubkey,
) -> Result<SwapInstructionsResponse> {
    get_metis_client().get_swap_instructions(quote, user_public_key).await
}

/// Get quote and swap instructions in sequence
pub async fn get_quote_and_swap_instructions(
    input_mint: &Pubkey,
    output_mint: &Pubkey,
    amount: u64,
    slippage_bps: Option<u16>,
    only_direct_routes: Option<bool>,
    max_accounts: Option<u8>,
    user_public_key: &Pubkey,
) -> Result<(QuoteResponse, SwapInstructionsResponse)> {
    let quote = get_quote(
        input_mint,
        output_mint,
        amount,
        slippage_bps,
        only_direct_routes,
        max_accounts,
    ).await?;

    let swap_ixs = get_swap_instructions(&quote, user_public_key).await?;

    Ok((quote, swap_ixs))
}

/// Swap result for routing module compatibility
#[derive(Debug)]
pub struct SwapRoute {
    pub in_amount: u64,
    pub out_amount: u64,
    pub slippage_bps: u16,
    pub price_impact_pct: String,
    pub address_lookup_tables: Vec<Pubkey>,
}

impl From<&QuoteResponse> for SwapRoute {
    fn from(quote: &QuoteResponse) -> Self {
        Self {
            in_amount: quote.in_amount_u64(),
            out_amount: quote.out_amount_u64(),
            slippage_bps: quote.slippage_bps,
            price_impact_pct: quote.price_impact_pct.clone(),
            address_lookup_tables: vec![], // ALTs come from swap instructions
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Integration test for Metis quote
    /// Run with: cargo test --package klend-terminator test_metis_quote -- --nocapture --ignored
    #[tokio::test]
    #[ignore]
    async fn test_metis_quote() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();

        let sol_mint: Pubkey = "So11111111111111111111111111111111111111112".parse().unwrap();
        let usdc_mint: Pubkey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();

        // Test amount: 1 SOL
        let amount = 1_000_000_000u64;

        println!("\n=== Testing Metis Quote API ===");
        println!("SOL -> USDC, amount={} lamports (1 SOL)", amount);

        let quote = get_quote(
            &sol_mint,
            &usdc_mint,
            amount,
            Some(100), // 1% slippage
            None,      // any route
            Some(64),  // max accounts
        ).await;

        match quote {
            Ok(q) => {
                println!("Quote SUCCESS:");
                println!("  in_amount: {}", q.in_amount);
                println!("  out_amount: {} ({} USDC)", q.out_amount, q.out_amount_u64() as f64 / 1e6);
                println!("  slippage_bps: {}", q.slippage_bps);
                println!("  price_impact_pct: {}", q.price_impact_pct);
                println!("  route_plan: {} steps", q.route_plan.len());
                for (i, step) in q.route_plan.iter().enumerate() {
                    println!("    step[{}]: {} ({}%)", i,
                        step.swap_info.label.as_deref().unwrap_or("unknown"),
                        step.percent);
                }
            }
            Err(e) => {
                println!("Quote FAILED: {:?}", e);
                panic!("Quote failed");
            }
        }
    }

    /// Integration test for Metis swap instructions with ALTs
    /// Run with: cargo test --package klend-terminator test_metis_swap_instructions -- --nocapture --ignored
    #[tokio::test]
    #[ignore]
    async fn test_metis_swap_instructions() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .init();

        let sol_mint: Pubkey = "So11111111111111111111111111111111111111112".parse().unwrap();
        let usdc_mint: Pubkey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();
        let weth_mint: Pubkey = "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs".parse().unwrap();

        // Load user keypair
        use solana_sdk::signer::Signer;
        let user: Pubkey = std::env::var("KEYPAIR_PATH")
            .ok()
            .and_then(|path| {
                let data = std::fs::read_to_string(&path).ok()?;
                let bytes: Vec<u8> = serde_json::from_str(&data).ok()?;
                solana_sdk::signature::Keypair::from_bytes(&bytes).ok()
            })
            .map(|kp| kp.pubkey())
            .unwrap_or_else(|| "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse().unwrap());

        println!("\n=== Testing Metis Swap Instructions with ALTs ===\n");

        // Test cases: (name, from_mint, to_mint, amount)
        let test_cases = [
            ("SOL→USDC (6.3 SOL)", sol_mint, usdc_mint, 6_319_669_634u64),
            ("ETH→USDC (0.1154 ETH)", weth_mint, usdc_mint, 11_545_304u64),
            ("SOL→USDC (1 SOL)", sol_mint, usdc_mint, 1_000_000_000u64),
        ];

        for (name, from_mint, to_mint, amount) in test_cases {
            println!("--- {} ---", name);

            match get_quote_and_swap_instructions(
                &from_mint,
                &to_mint,
                amount,
                Some(100), // 1% slippage
                None,      // any route
                Some(64),  // max accounts
                &user,
            ).await {
                Ok((quote, swap_ixs)) => {
                    println!("  Quote: in={} out={}", quote.in_amount, quote.out_amount);
                    println!("  Swap program_id: {}", swap_ixs.swap_instruction.program_id);
                    println!("  Compute budget ixs: {}", swap_ixs.compute_budget_instructions.len());
                    println!("  Setup ixs: {}", swap_ixs.setup_instructions.len());
                    println!("  Swap ix accounts: {}", swap_ixs.swap_instruction.accounts.len());
                    println!("  Cleanup ix: {}", swap_ixs.cleanup_instruction.is_some());
                    println!("  ALTs: {}", swap_ixs.address_lookup_table_addresses.len());

                    if !swap_ixs.address_lookup_table_addresses.is_empty() {
                        for alt in &swap_ixs.address_lookup_table_addresses {
                            println!("    ALT: {}", alt);
                        }
                    } else {
                        println!("  WARNING: No ALTs returned!");
                    }
                }
                Err(e) => {
                    println!("  FAILED: {:?}", e);
                }
            }
            println!();
        }

        println!("=== Test Complete ===");
    }

    /// Test comparing different swap pairs for ALT availability
    /// Run with: cargo test --package klend-terminator test_metis_alt_availability -- --nocapture --ignored
    #[tokio::test]
    #[ignore]
    async fn test_metis_alt_availability() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .init();

        // Token mints
        let sol_mint: Pubkey = "So11111111111111111111111111111111111111112".parse().unwrap();
        let usdc_mint: Pubkey = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".parse().unwrap();
        let weth_mint: Pubkey = "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs".parse().unwrap();
        let popcat_mint: Pubkey = "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr".parse().unwrap();

        use solana_sdk::signer::Signer;
        let user: Pubkey = std::env::var("KEYPAIR_PATH")
            .ok()
            .and_then(|path| {
                let data = std::fs::read_to_string(&path).ok()?;
                let bytes: Vec<u8> = serde_json::from_str(&data).ok()?;
                solana_sdk::signature::Keypair::from_bytes(&bytes).ok()
            })
            .map(|kp| kp.pubkey())
            .unwrap_or_else(|| "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".parse().unwrap());

        println!("\n=== Testing Metis ALT Availability ===\n");
        println!("{:<30} {:>15} {:>10} {:>8}", "Pair", "Out Amount", "Swap Accs", "ALTs");
        println!("{}", "-".repeat(70));

        let test_cases = [
            ("SOL→USDC", sol_mint, usdc_mint, 1_000_000_000u64),
            ("ETH→USDC", weth_mint, usdc_mint, 10_000_000u64),
            ("POPCAT→USDC", popcat_mint, usdc_mint, 1_000_000_000_000u64),
            ("USDC→SOL", usdc_mint, sol_mint, 100_000_000u64),
        ];

        for (name, from_mint, to_mint, amount) in test_cases {
            match get_quote_and_swap_instructions(
                &from_mint,
                &to_mint,
                amount,
                Some(100),
                None,
                Some(64),
                &user,
            ).await {
                Ok((quote, swap_ixs)) => {
                    println!("{:<30} {:>15} {:>10} {:>8}",
                        name,
                        quote.out_amount,
                        swap_ixs.swap_instruction.accounts.len(),
                        swap_ixs.address_lookup_table_addresses.len()
                    );
                }
                Err(e) => {
                    println!("{:<30} FAILED: {}", name, e);
                }
            }
        }

        println!("\n=== Test Complete ===");
    }
}
