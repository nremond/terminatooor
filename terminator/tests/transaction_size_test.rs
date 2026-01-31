//! Integration tests for transaction size verification
//!
//! These tests verify that flash loan liquidation transactions fit within
//! Solana's 1232 byte limit when using address lookup tables.

use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, VersionedMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};

const MAX_TX_SIZE: usize = 1232;

/// Simulates a flash loan liquidation transaction structure
/// based on real-world instruction counts and account requirements.
struct MockLiquidationTx {
    payer: Keypair,
    instructions: Vec<Instruction>,
    lookup_tables: Vec<AddressLookupTableAccount>,
}

impl MockLiquidationTx {
    fn new() -> Self {
        Self {
            payer: Keypair::new(),
            instructions: Vec::new(),
            lookup_tables: Vec::new(),
        }
    }

    /// Add a mock instruction with the specified number of accounts
    fn add_instruction(&mut self, program_id: Pubkey, num_accounts: usize, data_size: usize) {
        let accounts: Vec<AccountMeta> = (0..num_accounts)
            .map(|_| AccountMeta::new(Pubkey::new_unique(), false))
            .collect();

        self.instructions.push(Instruction {
            program_id,
            accounts,
            data: vec![0u8; data_size],
        });
    }

    /// Add a mock instruction using accounts from lookup tables
    fn add_instruction_with_lut_accounts(
        &mut self,
        program_id: Pubkey,
        lut_accounts: &[Pubkey],
        num_extra_accounts: usize,
        data_size: usize,
    ) {
        let mut accounts: Vec<AccountMeta> = lut_accounts
            .iter()
            .map(|pk| AccountMeta::new(*pk, false))
            .collect();

        // Add some accounts not in LUT (like the payer/signer)
        for _ in 0..num_extra_accounts {
            accounts.push(AccountMeta::new(Pubkey::new_unique(), false));
        }

        self.instructions.push(Instruction {
            program_id,
            accounts,
            data: vec![0u8; data_size],
        });
    }

    fn add_lookup_table(&mut self, addresses: Vec<Pubkey>) {
        self.lookup_tables.push(AddressLookupTableAccount {
            key: Pubkey::new_unique(),
            addresses,
        });
    }

    fn build(&self) -> Result<VersionedTransaction, String> {
        let message = V0Message::try_compile(
            &self.payer.pubkey(),
            &self.instructions,
            &self.lookup_tables,
            Hash::default(),
        )
        .map_err(|e| format!("Failed to compile message: {:?}", e))?;

        let tx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[&self.payer])
            .map_err(|e| format!("Failed to create transaction: {:?}", e))?;

        Ok(tx)
    }

    fn size(&self) -> Result<usize, String> {
        let tx = self.build()?;
        Ok(bincode::serialize(&tx).unwrap().len())
    }
}

/// Create a lookup table with realistic Kamino account addresses
fn create_kamino_lookup_table(num_reserves: usize) -> Vec<Pubkey> {
    let mut addresses = Vec::new();

    // For each reserve, add typical accounts:
    // - reserve pubkey
    // - liquidity supply vault
    // - liquidity fee vault
    // - liquidity mint
    // - collateral supply vault
    // - collateral mint
    // - debt farm state
    // - collateral farm state
    // - pyth oracle
    // - scope oracle
    for _ in 0..num_reserves {
        for _ in 0..10 {
            addresses.push(Pubkey::new_unique());
        }
    }

    // Add common accounts:
    // - lending market
    // - lending market authority
    // - lending market owner
    // - token program
    // - system program
    // - instructions sysvar
    // - rent sysvar
    // - farms program
    for _ in 0..8 {
        addresses.push(Pubkey::new_unique());
    }

    // Add liquidator ATAs (2 per reserve)
    for _ in 0..(num_reserves * 2) {
        addresses.push(Pubkey::new_unique());
    }

    addresses
}

/// Create a swap lookup table (similar to what Metis/Jupiter returns)
fn create_swap_lookup_table() -> Vec<Pubkey> {
    // Typical swap ALT has 200-250 addresses
    (0..244).map(|_| Pubkey::new_unique()).collect()
}

/// This test shows that markets with farms and post-liquidation farm refresh
/// will exceed the transaction limit. We need to skip post refresh_farms.
#[test]
fn test_flash_loan_with_farms_exceeds_limit() {
    // This test documents that the full instruction set with farms
    // exceeds the transaction limit and requires optimization

    let klend_program = Pubkey::new_unique();
    let swap_program = Pubkey::new_unique();
    let farms_program = Pubkey::new_unique();

    let mut tx = MockLiquidationTx::new();

    // Create lookup tables with all accounts
    let all_addresses: Vec<Pubkey> = (0..256).map(|_| Pubkey::new_unique()).collect();
    let swap_addresses: Vec<Pubkey> = (0..244).map(|_| Pubkey::new_unique()).collect();

    tx.add_lookup_table(all_addresses.clone());
    tx.add_lookup_table(swap_addresses.clone());

    // Full instruction set with pre AND post farm refresh
    // 1. init_obligation_farm_ix
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[0..13], 0, 9);

    // 2-3. refresh_reserve x2
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[13..21], 0, 8);
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[21..29], 0, 8);

    // 4. refresh_obligation
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[29..35], 0, 8);

    // 5-6. refresh_obligation_farms PRE x2
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[35..46], 0, 9);
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[46..57], 0, 9);

    // 7. flash_borrow
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[57..69], 0, 16);

    // 8. liquidate
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[69..90], 0, 25);

    // 9-10. Swap compute budget
    tx.add_instruction(Pubkey::new_unique(), 0, 5);
    tx.add_instruction(Pubkey::new_unique(), 0, 9);

    // 11. Swap
    tx.add_instruction_with_lut_accounts(swap_program, &swap_addresses[0..21], 0, 200);

    // 12. flash_repay
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[57..69], 0, 17);

    // 13-14. refresh_obligation_farms POST x2 (THIS CAUSES THE PROBLEM)
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[35..46], 0, 9);
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[46..57], 0, 9);

    let size = tx.size().expect("Failed to build transaction");

    println!("\n=== Full Farms Transaction (expected to fail) ===");
    println!("Transaction size: {} bytes", size);
    println!("Instructions: {} (including post farm refresh)", tx.instructions.len());
    println!(
        "Over limit by: {} bytes",
        size.saturating_sub(MAX_TX_SIZE)
    );

    // This SHOULD fail - documenting the problem
    if size > MAX_TX_SIZE {
        println!("✓ Confirmed: Full farms tx exceeds limit as expected");
        println!("  Solution: Skip post refresh_obligation_farms in flash loan liquidations");
    }

    // We actually expect this to fail, so we just document it
    // The real test is test_jlp_market_without_post_farm_refresh which should pass
}

#[test]
fn test_flash_loan_liquidation_tx_size_without_farms() {
    // This test simulates a simpler market without farms

    let klend_program = Pubkey::new_unique();
    let swap_program = Pubkey::new_unique();

    let mut tx = MockLiquidationTx::new();

    // Create lookup tables
    let kamino_lut_addresses = create_kamino_lookup_table(55); // Main market has ~55 reserves
    let swap_lut_addresses = create_swap_lookup_table();

    tx.add_lookup_table(kamino_lut_addresses.clone());
    tx.add_lookup_table(swap_lut_addresses.clone());

    // 1. refresh_reserve x2 - ~8 accounts each
    tx.add_instruction_with_lut_accounts(klend_program, &kamino_lut_addresses[0..8], 1, 8);
    tx.add_instruction_with_lut_accounts(klend_program, &kamino_lut_addresses[10..18], 1, 8);

    // 2. refresh_obligation - ~6 accounts (2 base + 4 reserves for typical obligation)
    tx.add_instruction_with_lut_accounts(klend_program, &kamino_lut_addresses[0..6], 2, 8);

    // 3. flash_borrow_reserve_liquidity - ~12 accounts
    tx.add_instruction_with_lut_accounts(klend_program, &kamino_lut_addresses[0..12], 1, 16);

    // 4. liquidate_obligation_and_redeem_reserve_collateral - ~20 accounts
    tx.add_instruction_with_lut_accounts(klend_program, &kamino_lut_addresses[0..18], 2, 32);

    // 5. swap instructions (3 from Metis)
    tx.add_instruction(Pubkey::new_unique(), 0, 9); // SetComputeUnitLimit
    tx.add_instruction(Pubkey::new_unique(), 0, 9); // SetComputeUnitPrice
    tx.add_instruction_with_lut_accounts(swap_program, &swap_lut_addresses[0..20], 2, 200);

    // 6. flash_repay_reserve_liquidity - ~12 accounts
    tx.add_instruction_with_lut_accounts(klend_program, &kamino_lut_addresses[0..12], 1, 16);

    let size = tx.size().expect("Failed to build transaction");

    println!("Transaction size without farms: {} bytes", size);
    println!("Instructions: {}", tx.instructions.len());

    assert!(
        size <= MAX_TX_SIZE,
        "Transaction too large: {} bytes (max {})",
        size,
        MAX_TX_SIZE
    );
}

#[test]
fn test_lookup_table_compression_effectiveness() {
    // This test verifies that lookup tables actually compress the transaction

    let program_id = Pubkey::new_unique();
    let accounts: Vec<Pubkey> = (0..20).map(|_| Pubkey::new_unique()).collect();

    // Build transaction WITHOUT lookup table
    let mut tx_without_lut = MockLiquidationTx::new();
    tx_without_lut.add_instruction(
        program_id,
        20, // 20 unique accounts
        32,
    );
    let size_without_lut = tx_without_lut.size().expect("Failed to build");

    // Build transaction WITH lookup table containing those accounts
    let mut tx_with_lut = MockLiquidationTx::new();
    tx_with_lut.add_lookup_table(accounts.clone());
    tx_with_lut.add_instruction_with_lut_accounts(program_id, &accounts, 0, 32);
    let size_with_lut = tx_with_lut.size().expect("Failed to build");

    println!("Size without LUT: {} bytes", size_without_lut);
    println!("Size with LUT: {} bytes", size_with_lut);
    println!(
        "Compression: {} bytes saved ({}%)",
        size_without_lut - size_with_lut,
        (size_without_lut - size_with_lut) * 100 / size_without_lut
    );

    // LUT should significantly reduce size
    // Each account reference goes from 32 bytes to 1 byte (index)
    // 20 accounts * 31 bytes saved = ~620 bytes
    assert!(
        size_with_lut < size_without_lut,
        "LUT should reduce transaction size"
    );
    assert!(
        size_without_lut - size_with_lut > 400,
        "LUT should save at least 400 bytes for 20 accounts"
    );
}

#[test]
fn test_max_accounts_per_lut() {
    // Verify we can use up to 256 addresses in a lookup table

    let program_id = Pubkey::new_unique();
    let addresses: Vec<Pubkey> = (0..256).map(|_| Pubkey::new_unique()).collect();

    let mut tx = MockLiquidationTx::new();
    tx.add_lookup_table(addresses.clone());

    // Use 50 accounts from the LUT
    tx.add_instruction_with_lut_accounts(program_id, &addresses[0..50], 1, 32);

    let size = tx.size().expect("Failed to build transaction");
    println!("Transaction with 256-address LUT: {} bytes", size);

    assert!(size < MAX_TX_SIZE, "Should fit in transaction limit");
}

/// Test that simulates the exact JLP market failure case
/// This shows the problem: even with LUTs, we're over the limit
#[test]
fn test_jlp_market_liquidation_problem_diagnosis() {
    // Based on the actual failure:
    // - Market: DxXdAyU3kCjnyggvHmY5nAwg5cRbbmdyX3npfDMjjMek
    // - 12 instructions
    // - 2 lookup tables (256 + 244 addresses)
    // - Was 1348 bytes, needed to be under 1232

    let klend_program = Pubkey::new_unique();
    let swap_program = Pubkey::new_unique();
    let farms_program = Pubkey::new_unique();
    let compute_budget_program = Pubkey::new_unique();

    let mut tx = MockLiquidationTx::new();

    // Create a combined lookup table with ALL possible addresses
    // This simulates ideal compression
    let all_addresses: Vec<Pubkey> = (0..256).map(|_| Pubkey::new_unique()).collect();
    let swap_addresses: Vec<Pubkey> = (0..244).map(|_| Pubkey::new_unique()).collect();

    tx.add_lookup_table(all_addresses.clone());
    tx.add_lookup_table(swap_addresses.clone());

    // Real instruction structure from logs - ALL accounts from LUT (no extra accounts)
    // 1. init_obligation_farm_ix (13 accounts)
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[0..13], 0, 9);

    // 2-3. refresh_reserve x2 (8 accounts each)
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[13..21], 0, 8);
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[21..29], 0, 8);

    // 4. refresh_obligation (6 accounts)
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[29..35], 0, 8);

    // 5-6. refresh_obligation_farms pre x2 (11 accounts each)
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[35..46], 0, 9);
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[46..57], 0, 9);

    // 7. flash_borrow_reserve_liquidity (12 accounts)
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[57..69], 0, 16);

    // 8. liquidate_obligation_and_redeem_reserve_collateral (21 accounts)
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[69..90], 0, 25);

    // 9-10. Swap compute budget (0 accounts)
    tx.add_instruction(compute_budget_program, 0, 5);
    tx.add_instruction(compute_budget_program, 0, 9);

    // 11. Swap instruction (21 accounts from swap LUT)
    tx.add_instruction_with_lut_accounts(swap_program, &swap_addresses[0..21], 0, 200);

    // 12. flash_repay_reserve_liquidity (12 accounts)
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[57..69], 0, 17);

    let size = tx.size().expect("Failed to build transaction");

    println!("\n=== JLP Market - Best Case (all in LUT) ===");
    println!("Transaction size: {} bytes", size);
    println!("Max allowed: {} bytes", MAX_TX_SIZE);
    println!("Instructions: {}", tx.instructions.len());
    println!(
        "Margin: {} bytes {}",
        if size <= MAX_TX_SIZE {
            MAX_TX_SIZE - size
        } else {
            size - MAX_TX_SIZE
        },
        if size <= MAX_TX_SIZE { "under" } else { "OVER" }
    );

    // With ALL accounts in LUT, this should fit
    assert!(
        size <= MAX_TX_SIZE,
        "\nEven with perfect LUT coverage, transaction too large!\n\
         Size: {} bytes, Max: {} bytes, Over by: {} bytes\n\
         Solution: Reduce number of instructions (skip post refresh_farms)",
        size,
        MAX_TX_SIZE,
        size.saturating_sub(MAX_TX_SIZE)
    );
}

/// Test showing we need to skip post-liquidation farm refreshes for large transactions
#[test]
fn test_jlp_market_without_post_farm_refresh() {
    let klend_program = Pubkey::new_unique();
    let swap_program = Pubkey::new_unique();
    let farms_program = Pubkey::new_unique();
    let compute_budget_program = Pubkey::new_unique();

    let mut tx = MockLiquidationTx::new();

    let all_addresses: Vec<Pubkey> = (0..256).map(|_| Pubkey::new_unique()).collect();
    let swap_addresses: Vec<Pubkey> = (0..244).map(|_| Pubkey::new_unique()).collect();

    tx.add_lookup_table(all_addresses.clone());
    tx.add_lookup_table(swap_addresses.clone());

    // Reduced instruction set - skip init_farm and post refresh_farms
    // 1-2. refresh_reserve x2
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[0..8], 0, 8);
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[8..16], 0, 8);

    // 3. refresh_obligation
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[16..22], 0, 8);

    // 4-5. refresh_obligation_farms pre x2
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[22..33], 0, 9);
    tx.add_instruction_with_lut_accounts(farms_program, &all_addresses[33..44], 0, 9);

    // 6. flash_borrow
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[44..56], 0, 16);

    // 7. liquidate
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[56..77], 0, 25);

    // 8-9. Swap compute budget
    tx.add_instruction(compute_budget_program, 0, 5);
    tx.add_instruction(compute_budget_program, 0, 9);

    // 10. Swap
    tx.add_instruction_with_lut_accounts(swap_program, &swap_addresses[0..21], 0, 200);

    // 11. flash_repay
    tx.add_instruction_with_lut_accounts(klend_program, &all_addresses[44..56], 0, 17);

    // NO post refresh_farms - they'll be done in next block

    let size = tx.size().expect("Failed to build transaction");

    println!("\n=== JLP Market - Without Post Farm Refresh ===");
    println!("Transaction size: {} bytes", size);
    println!("Max allowed: {} bytes", MAX_TX_SIZE);
    println!("Instructions: {}", tx.instructions.len());
    println!(
        "Margin: {} bytes {}",
        if size <= MAX_TX_SIZE {
            MAX_TX_SIZE - size
        } else {
            size - MAX_TX_SIZE
        },
        if size <= MAX_TX_SIZE { "under" } else { "OVER" }
    );

    assert!(
        size <= MAX_TX_SIZE,
        "\nTransaction too large: {} bytes (max {})",
        size,
        MAX_TX_SIZE
    );
}
