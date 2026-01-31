use std::sync::Arc;

use anchor_client::solana_sdk::{instruction::Instruction, pubkey::Pubkey, signature::Keypair};
use anchor_lang::{prelude::Rent, system_program::System, Id, InstructionData, ToAccountMetas};
use kamino_lending::{LendingMarket, Reserve, ReserveFarmKind};
use solana_sdk::{
    signer::Signer,
    sysvar::{
        SysvarId, {self},
    },
};

use crate::{consts::NULL_PUBKEY, liquidator::Liquidator, model::StateWithKey, readable, writable};

#[derive(Clone)]
pub struct InstructionBlocks {
    pub instruction: Instruction,
    pub payer: Pubkey,
    pub signers: Vec<Arc<Keypair>>,
}

#[allow(clippy::too_many_arguments)]
pub fn liquidate_obligation_and_redeem_reserve_collateral_ix(
    program_id: &Pubkey,
    lending_market: StateWithKey<LendingMarket>,
    debt_reserve: StateWithKey<Reserve>,
    coll_reserve: StateWithKey<Reserve>,
    liquidator: &Liquidator,
    obligation: Pubkey,
    liquidity_amount: u64,
    min_acceptable_received_liq_amount: u64,
    max_allowed_ltv_override_pct_opt: Option<u64>,
) -> InstructionBlocks {
    let lending_market_pubkey = lending_market.key;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&lending_market_pubkey);

    let coll_reserve_state = coll_reserve.state.borrow();
    let coll_reserve_address = coll_reserve.key;

    let debt_reserve_state = debt_reserve.state.borrow();
    let debt_reserve_address = debt_reserve.key;

    let collateral_ctoken = coll_reserve_state.collateral.mint_pubkey;
    let collateral_token = coll_reserve_state.liquidity.mint_pubkey;
    let debt_token = debt_reserve_state.liquidity.mint_pubkey;

    let (user_destination_collateral, user_destination_liquidity, user_source_liquidity) = {
        let atas = liquidator.atas.read().unwrap();
        (
            *atas.get(&collateral_ctoken).unwrap(),
            *atas.get(&collateral_token).unwrap(),
            *atas.get(&debt_token).unwrap(),
        )
    };

    // Get the correct token programs for each mint
    let repay_token_program = liquidator.token_program_for_mint(&debt_token);
    let collateral_token_program = liquidator.token_program_for_mint(&collateral_ctoken);
    let withdraw_token_program = liquidator.token_program_for_mint(&collateral_token);

    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::LiquidateObligationAndRedeemReserveCollateral {
            liquidator: liquidator.wallet.pubkey(),
            lending_market: lending_market.key,
            repay_reserve: debt_reserve_address,
            repay_reserve_liquidity_supply: debt_reserve_state.liquidity.supply_vault,
            withdraw_reserve: coll_reserve_address,
            withdraw_reserve_collateral_mint: coll_reserve_state.collateral.mint_pubkey,
            withdraw_reserve_collateral_supply: coll_reserve_state.collateral.supply_vault,
            withdraw_reserve_liquidity_supply: coll_reserve_state.liquidity.supply_vault,
            withdraw_reserve_liquidity_fee_receiver: coll_reserve_state.liquidity.fee_vault,
            lending_market_authority,
            obligation,
            user_destination_collateral,
            user_destination_liquidity,
            user_source_liquidity,
            instruction_sysvar_account: sysvar::instructions::ID,
            repay_liquidity_token_program: repay_token_program,
            repay_reserve_liquidity_mint: debt_reserve_state.liquidity.mint_pubkey,
            withdraw_reserve_liquidity_mint: coll_reserve_state.liquidity.mint_pubkey,
            collateral_token_program,
            withdraw_liquidity_token_program: withdraw_token_program,
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::LiquidateObligationAndRedeemReserveCollateral {
            liquidity_amount,
            min_acceptable_received_liquidity_amount: min_acceptable_received_liq_amount,
            max_allowed_ltv_override_percent: max_allowed_ltv_override_pct_opt.unwrap_or(0),
        }
        .data(),
    };

    InstructionBlocks {
        instruction,
        payer: liquidator.wallet.pubkey(),
        signers: vec![liquidator.wallet.clone()],
    }
}

pub fn refresh_reserve_ix(
    program_id: &Pubkey,
    reserve: Reserve,
    address: &Pubkey,
    payer: Arc<Keypair>,
) -> InstructionBlocks {
    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::RefreshReserve {
            lending_market: reserve.lending_market,
            reserve: *address,
            pyth_oracle: maybe_null_pk(reserve.config.token_info.pyth_configuration.price),
            switchboard_price_oracle: maybe_null_pk(
                reserve
                    .config
                    .token_info
                    .switchboard_configuration
                    .price_aggregator,
            ),
            switchboard_twap_oracle: maybe_null_pk(
                reserve
                    .config
                    .token_info
                    .switchboard_configuration
                    .twap_aggregator,
            ),
            scope_prices: maybe_null_pk(reserve.config.token_info.scope_configuration.price_feed),
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::RefreshReserve.data(),
    };

    InstructionBlocks {
        instruction,
        payer: payer.pubkey(),
        signers: vec![payer.clone()],
    }
}

pub fn refresh_obligation_farm_for_reserve_ix(
    program_id: &Pubkey,
    reserve: &StateWithKey<Reserve>,
    reserve_farm_state: Pubkey,
    obligation: Pubkey,
    owner: &Arc<Keypair>,
    farms_mode: ReserveFarmKind,
) -> InstructionBlocks {
    let (user_farm_state, _) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            reserve_farm_state.as_ref(),
            obligation.as_ref(),
        ],
        &farms::ID,
    );

    let reserve_state = reserve.state.borrow();
    let reserve_address = reserve.key;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&reserve_state.lending_market);

    let accts = kamino_lending::accounts::RefreshObligationFarmsForReserve {
        crank: owner.pubkey(),
        obligation,
        lending_market: reserve_state.lending_market,
        lending_market_authority,
        reserve: reserve_address,
        obligation_farm_user_state: user_farm_state,
        reserve_farm_state,
        rent: Rent::id(),
        farms_program: farms::id(),
        system_program: System::id(),
    };

    let instruction = Instruction {
        program_id: *program_id,
        accounts: accts.to_account_metas(None),
        data: kamino_lending::instruction::RefreshObligationFarmsForReserve {
            mode: farms_mode as u8,
        }
        .data(),
    };

    InstructionBlocks {
        instruction,
        payer: owner.pubkey(),
        signers: vec![owner.clone()],
    }
}

#[allow(clippy::too_many_arguments)]
pub fn init_obligation_farm_for_reserve_ix(
    program_id: &Pubkey,
    reserve_accounts: &StateWithKey<Reserve>,
    reserve_farm_state: Pubkey,
    obligation: &Pubkey,
    owner: &Pubkey,
    payer: &Arc<Keypair>,
    mode: ReserveFarmKind,
) -> InstructionBlocks {
    let (obligation_farm_state, _user_state_bump) = Pubkey::find_program_address(
        &[
            farms::utils::consts::BASE_SEED_USER_STATE,
            reserve_farm_state.as_ref(),
            obligation.as_ref(),
        ],
        &farms::ID,
    );

    let reserve_state = reserve_accounts.state.borrow();
    let reserve_address = reserve_accounts.key;
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(&reserve_state.lending_market);

    let accts = kamino_lending::accounts::InitObligationFarmsForReserve {
        payer: payer.pubkey(),
        owner: *owner,
        obligation: *obligation,
        lending_market: reserve_state.lending_market,
        lending_market_authority,
        reserve: reserve_address,
        obligation_farm: obligation_farm_state,
        reserve_farm_state,
        rent: Rent::id(),
        farms_program: farms::id(),
        system_program: System::id(),
    };

    let instruction = Instruction {
        program_id: *program_id,
        accounts: accts.to_account_metas(None),
        data: kamino_lending::instruction::InitObligationFarmsForReserve { mode: mode as u8 }
            .data(),
    };

    InstructionBlocks {
        instruction,
        payer: payer.pubkey(),
        signers: vec![payer.clone()],
    }
}

pub fn refresh_obligation_ix(
    program_id: &Pubkey,
    market: Pubkey,
    obligation: Pubkey,
    deposit_reserves: Vec<Option<Pubkey>>,
    borrow_reserves: Vec<Option<Pubkey>>,
    referrer_token_states: Vec<Option<Pubkey>>,
    payer: Arc<Keypair>,
) -> InstructionBlocks {
    let mut accounts = kamino_lending::accounts::RefreshObligation {
        obligation,
        lending_market: market,
    }
    .to_account_metas(None);

    deposit_reserves
        .iter()
        .filter(|reserve| reserve.is_some())
        .for_each(|reserve| {
            accounts.push(readable!(reserve.unwrap()));
        });

    borrow_reserves
        .iter()
        .filter(|reserve| reserve.is_some())
        .for_each(|reserve| {
            accounts.push(writable!(reserve.unwrap()));
        });

    referrer_token_states
        .iter()
        .filter(|referrer_token_state: &&Option<Pubkey>| referrer_token_state.is_some())
        .for_each(|referrer_token_state| {
            accounts.push(writable!(referrer_token_state.unwrap()));
        });

    let instruction = Instruction {
        program_id: *program_id,
        accounts,
        data: kamino_lending::instruction::RefreshObligation.data(),
    };

    InstructionBlocks {
        instruction,
        payer: payer.pubkey(),
        signers: vec![payer.clone()],
    }
}

pub fn maybe_null_pk(pubkey: Pubkey) -> Option<Pubkey> {
    if pubkey == Pubkey::default() || pubkey == NULL_PUBKEY {
        None
    } else {
        Some(pubkey)
    }
}

/// Create a flash borrow instruction to borrow liquidity from a reserve
#[allow(clippy::too_many_arguments)]
pub fn flash_borrow_reserve_liquidity_ix(
    program_id: &Pubkey,
    lending_market: &Pubkey,
    reserve: &StateWithKey<Reserve>,
    user_destination_liquidity: &Pubkey,
    liquidator: &Liquidator,
    liquidity_amount: u64,
    referrer_token_state: Option<Pubkey>,
    referrer_account: Option<Pubkey>,
    token_program: Pubkey,
) -> InstructionBlocks {
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(lending_market);

    let reserve_state = reserve.state.borrow();

    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::FlashBorrowReserveLiquidity {
            user_transfer_authority: liquidator.wallet.pubkey(),
            lending_market_authority,
            lending_market: *lending_market,
            reserve: reserve.key,
            reserve_liquidity_mint: reserve_state.liquidity.mint_pubkey,
            reserve_source_liquidity: reserve_state.liquidity.supply_vault,
            user_destination_liquidity: *user_destination_liquidity,
            reserve_liquidity_fee_receiver: reserve_state.liquidity.fee_vault,
            referrer_token_state,
            referrer_account,
            sysvar_info: sysvar::instructions::ID,
            token_program,
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::FlashBorrowReserveLiquidity { liquidity_amount }.data(),
    };

    InstructionBlocks {
        instruction,
        payer: liquidator.wallet.pubkey(),
        signers: vec![liquidator.wallet.clone()],
    }
}

/// Create a flash repay instruction to repay a flash loan
#[allow(clippy::too_many_arguments)]
pub fn flash_repay_reserve_liquidity_ix(
    program_id: &Pubkey,
    lending_market: &Pubkey,
    reserve: &StateWithKey<Reserve>,
    user_source_liquidity: &Pubkey,
    liquidator: &Liquidator,
    liquidity_amount: u64,
    borrow_instruction_index: u8,
    referrer_token_state: Option<Pubkey>,
    referrer_account: Option<Pubkey>,
    token_program: Pubkey,
) -> InstructionBlocks {
    let lending_market_authority =
        kamino_lending::utils::seeds::pda::lending_market_auth(lending_market);

    let reserve_state = reserve.state.borrow();

    let instruction = Instruction {
        program_id: *program_id,
        accounts: kamino_lending::accounts::FlashRepayReserveLiquidity {
            user_transfer_authority: liquidator.wallet.pubkey(),
            lending_market_authority,
            lending_market: *lending_market,
            reserve: reserve.key,
            reserve_liquidity_mint: reserve_state.liquidity.mint_pubkey,
            user_source_liquidity: *user_source_liquidity,
            reserve_destination_liquidity: reserve_state.liquidity.supply_vault,
            reserve_liquidity_fee_receiver: reserve_state.liquidity.fee_vault,
            referrer_token_state,
            referrer_account,
            sysvar_info: sysvar::instructions::ID,
            token_program,
        }
        .to_account_metas(None),
        data: kamino_lending::instruction::FlashRepayReserveLiquidity {
            liquidity_amount,
            borrow_instruction_index,
        }
        .data(),
    };

    InstructionBlocks {
        instruction,
        payer: liquidator.wallet.pubkey(),
        signers: vec![liquidator.wallet.clone()],
    }
}

/// Calculate the flash loan repay amount including fees
pub fn calculate_flash_loan_repay_amount(reserve: &Reserve, borrow_amount: u64) -> u64 {
    let flash_loan_fee_sf = reserve.config.fees.flash_loan_fee_sf;
    if flash_loan_fee_sf == u64::MAX {
        // Flash loans disabled
        return 0;
    }

    // Fee calculation: repay = borrow + (borrow * fee_sf / FRACTION_ONE_SF)
    // FRACTION_ONE_SF = 2^60 (from kamino_lending)
    const FRACTION_ONE_SF: u128 = 1u128 << 60;
    let fee = (borrow_amount as u128 * flash_loan_fee_sf as u128) / FRACTION_ONE_SF;
    borrow_amount + fee as u64
}

/// Number of ComputeBudget instructions prepended by build_with_budget_and_fee
/// This is used to calculate the correct instruction index for flash loan repay
pub const COMPUTE_BUDGET_IX_COUNT: u8 = 2;

/// Calculate the flash borrow instruction index in the final transaction
/// The flash borrow is at index 0 in our instruction list, but build_with_budget_and_fee
/// prepends COMPUTE_BUDGET_IX_COUNT instructions (SetComputeUnitLimit, SetComputeUnitPrice)
pub fn flash_borrow_instruction_index(base_index: u8) -> u8 {
    base_index + COMPUTE_BUDGET_IX_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flash_borrow_instruction_index() {
        // When flash_borrow is the first instruction in our list (index 0),
        // it should be at index 2 in the final transaction after ComputeBudget ixs
        assert_eq!(flash_borrow_instruction_index(0), 2);

        // Verify the constant matches what OrbitLink's build_with_budget_and_fee prepends:
        // - SetComputeUnitLimit (index 0)
        // - SetComputeUnitPrice (index 1)
        assert_eq!(COMPUTE_BUDGET_IX_COUNT, 2);
    }

    #[test]
    fn test_flash_borrow_index_with_offset() {
        // If we had other instructions before flash_borrow
        assert_eq!(flash_borrow_instruction_index(1), 3);
        assert_eq!(flash_borrow_instruction_index(5), 7);
    }
}
