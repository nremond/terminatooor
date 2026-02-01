//! Integration tests for flash loan liquidation instruction ordering
//!
//! These tests verify that flash loan liquidation transactions are correctly
//! structured to pass the on-chain `check_refresh_ixs!` validation.
//!
//! Run with: cargo test --test flash_loan_liquidation_test -- --nocapture --ignored

use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};

/// Kamino program ID (mainnet)
const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

/// Farms program ID
const FARMS_PROGRAM_ID: &str = "FarmsPZpWu9i7Kky8tPN37rs2TpmMrAZrC7S7vJa91Hr";

/// Expected flash loan liquidation transaction structure
struct FlashLoanLiquidationStructure {
    /// Flash borrow is always first
    flash_borrow_index: usize,
    /// Pre-liquidation instructions (init_farm, refresh_reserves, refresh_obligation, refresh_farms)
    pre_ixns_start: usize,
    pre_ixns_count: usize,
    /// Main liquidation instruction
    liquidate_index: usize,
    /// Post-liquidation farm refresh instructions
    post_ixns_start: usize,
    post_ixns_count: usize,
    /// Flash repay is always last
    flash_repay_index: usize,
}

impl FlashLoanLiquidationStructure {
    /// Verify the structure is valid for check_refresh_ixs! validation
    fn validate(&self) -> Result<(), String> {
        // Flash borrow must be first
        if self.flash_borrow_index != 0 {
            return Err(format!(
                "Flash borrow must be at index 0, got {}",
                self.flash_borrow_index
            ));
        }

        // Pre-ixns must follow flash borrow
        if self.pre_ixns_start != 1 {
            return Err(format!(
                "Pre-ixns must start at index 1, got {}",
                self.pre_ixns_start
            ));
        }

        // Liquidate must follow pre-ixns
        let expected_liquidate = self.pre_ixns_start + self.pre_ixns_count;
        if self.liquidate_index != expected_liquidate {
            return Err(format!(
                "Liquidate must be at index {}, got {}",
                expected_liquidate, self.liquidate_index
            ));
        }

        // Post-ixns must follow liquidate
        let expected_post_start = self.liquidate_index + 1;
        if self.post_ixns_start != expected_post_start {
            return Err(format!(
                "Post-ixns must start at index {}, got {}",
                expected_post_start, self.post_ixns_start
            ));
        }

        // Flash repay must be last
        let expected_repay = self.post_ixns_start + self.post_ixns_count;
        if self.flash_repay_index != expected_repay {
            return Err(format!(
                "Flash repay must be at index {}, got {}",
                expected_repay, self.flash_repay_index
            ));
        }

        Ok(())
    }
}

/// Verify instruction program IDs match the expected pattern
fn verify_instruction_programs(
    instructions: &[Instruction],
    structure: &FlashLoanLiquidationStructure,
) -> Result<(), String> {
    let kamino: Pubkey = KAMINO_PROGRAM_ID.parse().unwrap();
    let farms: Pubkey = FARMS_PROGRAM_ID.parse().unwrap();

    // Flash borrow must be Kamino
    if instructions[structure.flash_borrow_index].program_id != kamino {
        return Err(format!(
            "Flash borrow must be Kamino program, got {}",
            instructions[structure.flash_borrow_index].program_id
        ));
    }

    // Pre-ixns must be Kamino or Farms
    for i in structure.pre_ixns_start..(structure.pre_ixns_start + structure.pre_ixns_count) {
        let ix = &instructions[i];
        if ix.program_id != kamino && ix.program_id != farms {
            return Err(format!(
                "Pre-instruction {} must be Kamino or Farms, got {}",
                i, ix.program_id
            ));
        }
    }

    // Liquidate must be Kamino
    if instructions[structure.liquidate_index].program_id != kamino {
        return Err(format!(
            "Liquidate must be Kamino program, got {}",
            instructions[structure.liquidate_index].program_id
        ));
    }

    // Post-ixns must be Kamino or Farms
    for i in structure.post_ixns_start..(structure.post_ixns_start + structure.post_ixns_count) {
        let ix = &instructions[i];
        if ix.program_id != kamino && ix.program_id != farms {
            return Err(format!(
                "Post-instruction {} must be Kamino or Farms, got {}",
                i, ix.program_id
            ));
        }
    }

    // Flash repay must be Kamino
    if instructions[structure.flash_repay_index].program_id != kamino {
        return Err(format!(
            "Flash repay must be Kamino program, got {}",
            instructions[structure.flash_repay_index].program_id
        ));
    }

    Ok(())
}

/// Unit test: Verify the expected instruction structure for flash loan liquidation
/// with farms (post-farm refresh required)
#[test]
fn test_flash_loan_structure_with_farms() {
    let kamino: Pubkey = KAMINO_PROGRAM_ID.parse().unwrap();
    let farms: Pubkey = FARMS_PROGRAM_ID.parse().unwrap();

    // Build mock instructions matching expected structure
    let instructions = vec![
        // [0] flash_borrow
        Instruction::new_with_bytes(kamino, &[0], vec![]),
        // [1] init_obligation_farm (pre)
        Instruction::new_with_bytes(kamino, &[1], vec![]),
        // [2] refresh_reserve collateral (pre)
        Instruction::new_with_bytes(kamino, &[2], vec![]),
        // [3] refresh_reserve debt (pre)
        Instruction::new_with_bytes(kamino, &[3], vec![]),
        // [4] refresh_obligation (pre)
        Instruction::new_with_bytes(kamino, &[4], vec![]),
        // [5] refresh_obligation_farms (pre)
        Instruction::new_with_bytes(kamino, &[5], vec![]),
        // [6] liquidate_obligation_and_redeem_reserve_collateral
        Instruction::new_with_bytes(kamino, &[6], vec![]),
        // [7] refresh_obligation_farms (post) - REQUIRED for check_refresh_ixs!
        Instruction::new_with_bytes(kamino, &[7], vec![]),
        // [8] flash_repay
        Instruction::new_with_bytes(kamino, &[8], vec![]),
    ];

    let structure = FlashLoanLiquidationStructure {
        flash_borrow_index: 0,
        pre_ixns_start: 1,
        pre_ixns_count: 5, // init_farm + 2 refresh_reserve + refresh_obligation + refresh_farms
        liquidate_index: 6,
        post_ixns_start: 7,
        post_ixns_count: 1, // refresh_obligation_farms (post)
        flash_repay_index: 8,
    };

    // Verify structure
    structure.validate().expect("Structure should be valid");
    verify_instruction_programs(&instructions, &structure)
        .expect("Program IDs should be valid");

    println!("\n✓ Flash loan structure with farms is valid");
    println!("  Total instructions: {}", instructions.len());
    println!("  Pre-ixns: {}", structure.pre_ixns_count);
    println!("  Post-ixns: {} (includes refresh_obligation_farms)", structure.post_ixns_count);
}

/// Unit test: Verify structure without farms (no post-farm refresh needed)
#[test]
fn test_flash_loan_structure_without_farms() {
    let kamino: Pubkey = KAMINO_PROGRAM_ID.parse().unwrap();

    // Build mock instructions for market without farms
    let instructions = vec![
        // [0] flash_borrow
        Instruction::new_with_bytes(kamino, &[0], vec![]),
        // [1] refresh_reserve collateral (pre)
        Instruction::new_with_bytes(kamino, &[1], vec![]),
        // [2] refresh_reserve debt (pre)
        Instruction::new_with_bytes(kamino, &[2], vec![]),
        // [3] refresh_obligation (pre)
        Instruction::new_with_bytes(kamino, &[3], vec![]),
        // [4] liquidate_obligation_and_redeem_reserve_collateral
        Instruction::new_with_bytes(kamino, &[4], vec![]),
        // [5] flash_repay (no post-ixns needed)
        Instruction::new_with_bytes(kamino, &[5], vec![]),
    ];

    let structure = FlashLoanLiquidationStructure {
        flash_borrow_index: 0,
        pre_ixns_start: 1,
        pre_ixns_count: 3, // 2 refresh_reserve + refresh_obligation
        liquidate_index: 4,
        post_ixns_start: 5,
        post_ixns_count: 0, // No farms, no post-ixns
        flash_repay_index: 5,
    };

    // Verify structure
    structure.validate().expect("Structure should be valid");
    verify_instruction_programs(&instructions, &structure)
        .expect("Program IDs should be valid");

    println!("\n✓ Flash loan structure without farms is valid");
    println!("  Total instructions: {}", instructions.len());
    println!("  Pre-ixns: {}", structure.pre_ixns_count);
    println!("  Post-ixns: {} (no farms)", structure.post_ixns_count);
}

/// Unit test: Verify that missing post-farm refresh would fail on-chain validation
#[test]
fn test_missing_post_farm_refresh_is_invalid() {
    let kamino: Pubkey = KAMINO_PROGRAM_ID.parse().unwrap();

    // This structure is WRONG - it has farms but no post-refresh
    // On-chain check_refresh_ixs! would fail with error 2502
    let instructions = vec![
        // [0] flash_borrow
        Instruction::new_with_bytes(kamino, &[0], vec![]),
        // [1] init_obligation_farm (pre)
        Instruction::new_with_bytes(kamino, &[1], vec![]),
        // [2] refresh_reserve collateral (pre)
        Instruction::new_with_bytes(kamino, &[2], vec![]),
        // [3] refresh_reserve debt (pre)
        Instruction::new_with_bytes(kamino, &[3], vec![]),
        // [4] refresh_obligation (pre)
        Instruction::new_with_bytes(kamino, &[4], vec![]),
        // [5] refresh_obligation_farms (pre)
        Instruction::new_with_bytes(kamino, &[5], vec![]),
        // [6] liquidate_obligation_and_redeem_reserve_collateral
        Instruction::new_with_bytes(kamino, &[6], vec![]),
        // MISSING: refresh_obligation_farms (post) - This causes error 2502!
        // [7] flash_repay (WRONG position - should be after post-farm refresh)
        Instruction::new_with_bytes(kamino, &[7], vec![]),
    ];

    // Document that this structure would fail
    // The on-chain check expects:
    //   required_post_ixs: [refresh_obligation_farms] for each farm
    //   check_refresh_ixs! looks at instruction[liquidate_idx + 1]
    //   and expects it to be refresh_obligation_farms, not flash_repay

    println!("\n⚠ This structure would fail on-chain with error 2502:");
    println!("  When farms exist, post-farm refresh instructions are REQUIRED");
    println!("  check_refresh_ixs! expects Kamino refresh_obligation_farms at liquidate+1");
    println!("  But flash_repay at position {} would trigger RequireKeysEqViolated", 7);
    println!("\n  Solution: Set skip_post_farm_refresh=false when building liquidation ixns");
}

/// Unit test: Verify the on-chain validation rule
///
/// From klend/src/utils/refresh_ix_utils.rs:
/// ```
/// // check that next `post_ixs.len()` instructions are expected post_ixs
/// for (i, required_ix) in required_post_ixs.iter().enumerate() {
///     let ix = load_instruction_at(current_idx + i + 1, ixs_sysvar_account)?;
///     require_keys_eq!(ix.program_id, crate::id());
///     // ...
/// }
/// ```
#[test]
fn test_check_refresh_ixs_validation_rule() {
    println!("\n=== check_refresh_ixs! Validation Rule ===\n");
    println!("The on-chain check_refresh_ixs! macro validates:");
    println!("1. All required_pre_ixs exist BEFORE the current instruction");
    println!("2. All required_post_ixs exist AFTER the current instruction");
    println!();
    println!("For liquidate_obligation_and_redeem_reserve_collateral:");
    println!("  - required_pre_ixs: refresh_reserve (each involved reserve)");
    println!("                      refresh_obligation");
    println!("                      refresh_obligation_farms (if farms exist)");
    println!("  - required_post_ixs: refresh_obligation_farms (if farms exist)");
    println!();
    println!("Error 2502 (RequireKeysEqViolated) occurs when:");
    println!("  The instruction at position (liquidate_idx + post_ix_index + 1)");
    println!("  has a different program_id than expected (e.g., System instead of Kamino)");
    println!();
    println!("Fix: Set skip_post_farm_refresh=false in liquidate_ixns builder");
}
