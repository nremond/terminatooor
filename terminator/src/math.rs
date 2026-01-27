use anchor_lang::prelude::Pubkey;
use anyhow::Result;
use colored::Colorize;
pub use kamino_lending::utils::Fraction;
use kamino_lending::{LiquidationParams, Obligation};
use tracing::{debug, info};

use crate::{liquidator::Holdings, model::StateWithKey};

#[derive(Debug)]
pub enum LiquidationStrategy {
    LiquidateAndRedeem(u64),
    SwapThenLiquidate(u64, u64),
    /// Flash loan liquidation: borrow debt token, liquidate, swap collateral, repay
    /// Parameters: (liquidate_amount, expected_collateral_amount)
    FlashLoanLiquidate(u64, u64),
}

pub struct ObligationInfo {
    pub borrowed_amount: Fraction,
    pub deposited_amount: Fraction,
    pub ltv: Fraction,
    pub unhealthy_ltv: Fraction,
}

pub fn obligation_info(address: &Pubkey, obligation: &Obligation) -> ObligationInfo {
    let borrowed_amount = Fraction::from_bits(obligation.borrow_factor_adjusted_debt_value_sf);
    let deposited_amount = Fraction::from_bits(obligation.deposited_value_sf);

    let (ltv, unhealthy_ltv) = if borrowed_amount > 0 && deposited_amount == 0 {
        info!("Obligation {address} has bad debt: {:?}", obligation);
        (Fraction::ZERO, Fraction::ZERO)
    } else if deposited_amount > 0 || borrowed_amount > 0 {
        (
            obligation.loan_to_value(),
            obligation.unhealthy_loan_to_value(),
        )
    } else {
        (Fraction::ZERO, Fraction::ZERO)
    };

    ObligationInfo {
        borrowed_amount,
        deposited_amount,
        ltv,
        unhealthy_ltv,
    }
}

pub fn print_obligation_stats(
    obl_info: &ObligationInfo,
    address: &Pubkey,
    i: usize,
    num_obligations: usize,
) {
    let ObligationInfo {
        borrowed_amount,
        deposited_amount,
        ltv,
        unhealthy_ltv,
    } = obl_info;

    let is_liquidatable = ltv > unhealthy_ltv;
    let msg = format!(
        "{}/{} obligation: {}, healthy: {}, LTV: {:?}%/{:?}%, deposited: {} borrowed: {}",
        i + 1,
        num_obligations,
        address.to_string().green(),
        if is_liquidatable {
            "NO".red()
        } else {
            "YES".green()
        },
        ltv * 100,
        unhealthy_ltv * 100,
        deposited_amount,
        borrowed_amount
    );
    if is_liquidatable {
        info!("{}", msg);
    } else {
        debug!("{}", msg);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn decide_liquidation_strategy(
    _base_mint: &Pubkey,
    obligation: &StateWithKey<Obligation>,
    lending_market: &StateWithKey<kamino_lending::LendingMarket>,
    coll_reserve: &StateWithKey<kamino_lending::Reserve>,
    debt_reserve: &StateWithKey<kamino_lending::Reserve>,
    clock: &anchor_lang::prelude::Clock,
    max_allowed_ltv_override_pct_opt: Option<u64>,
    liquidation_swap_slippage_pct: f64,
    _holdings: Holdings,
) -> Result<Option<LiquidationStrategy>> {
    let debt_res_key = debt_reserve.key;
    let coll_res_key = coll_reserve.key;

    // Calculate what is possible first
    let LiquidationParams { user_ltv, liquidation_bonus_rate, .. } =
        kamino_lending::liquidation_operations::get_liquidation_params(
            &lending_market.state.borrow(),
            &coll_reserve.state.borrow(),
            &debt_reserve.state.borrow(),
            &obligation.state.borrow(),
            clock.slot,
            true, // todo actually check if this is true
            true, // todo actually check if this is true
            max_allowed_ltv_override_pct_opt,
        )?;

    let obligation_state = obligation.state.borrow();
    let lending_market_state = lending_market.state.borrow();
    let debt_reserve_state = debt_reserve.state.borrow();
    let _coll_reserve_state = coll_reserve.state.borrow();

    let (debt_liquidity, _) = obligation_state
        .find_liquidity_in_borrows(debt_res_key)
        .unwrap();

    let coll_deposit = obligation_state
        .find_collateral_in_deposits(coll_res_key)
        .unwrap();

    let full_debt_amount_f = Fraction::from_bits(debt_liquidity.borrowed_amount_sf);
    let full_debt_mv_f = Fraction::from_bits(debt_liquidity.market_value_sf);

    let liquidatable_debt =
        kamino_lending::liquidation_operations::max_liquidatable_borrowed_amount(
            &obligation_state,
            lending_market_state.liquidation_max_debt_close_factor_pct,
            lending_market_state.max_liquidatable_debt_market_value_at_once,
            debt_liquidity,
            user_ltv,
            lending_market_state.insolvency_risk_unhealthy_ltv_pct,
        )
        .min(full_debt_amount_f);

    let liquidation_ratio = liquidatable_debt / full_debt_amount_f;

    // This is what is possible, liquidatable/repayable debt in lamports and in $ terms
    let liquidatable_amount: u64 = liquidatable_debt.to_num();
    let liquidatable_mv = liquidation_ratio * full_debt_mv_f;

    // Check if flash loans are disabled for this reserve
    let flash_loan_fee_sf = debt_reserve_state.config.fees.flash_loan_fee_sf;
    if flash_loan_fee_sf == u64::MAX {
        info!("Flash loans disabled for debt reserve, skipping liquidation");
        return Ok(None);
    }

    // Calculate expected collateral to receive (in collateral token lamports)
    // Collateral received = (debt_repaid * debt_price / coll_price) * (1 + liquidation_bonus_rate)
    let debt_mv = Fraction::from_bits(debt_liquidity.market_value_sf);
    let debt_amt = Fraction::from_bits(debt_liquidity.borrowed_amount_sf);
    let coll_mv = Fraction::from_bits(coll_deposit.market_value_sf);
    let coll_amt = Fraction::from_num(coll_deposit.deposited_amount);

    let debt_price = debt_mv / debt_amt;
    let coll_price = coll_mv / coll_amt;

    debug!(
        "Price calc: debt_mv={}, debt_amt={}, debt_price={}, coll_mv={}, coll_amt={}, coll_price={}",
        debt_mv, debt_amt, debt_price, coll_mv, coll_amt, coll_price
    );

    // liquidation_bonus_rate is already a fraction (e.g., 0.05 for 5% bonus)
    let liquidation_bonus_multiplier = Fraction::from_num(1) + liquidation_bonus_rate;

    // Calculate expected collateral: (debt_to_repay * debt_price / coll_price) * (1 + bonus)
    let debt_to_repay_value = Fraction::from_num(liquidatable_amount) * debt_price;
    let expected_collateral_value = debt_to_repay_value * liquidation_bonus_multiplier;
    let expected_collateral_amount: u64 = (expected_collateral_value / coll_price).to_num();

    debug!(
        "Collateral calc: liquidatable_amount={}, debt_to_repay_value={}, expected_value={}, expected_amount={}",
        liquidatable_amount, debt_to_repay_value, expected_collateral_value, expected_collateral_amount
    );

    // Always use flash loans for maximum capital efficiency
    let decision = Some(LiquidationStrategy::FlashLoanLiquidate(
        liquidatable_amount,
        expected_collateral_amount,
    ));

    // Print everything such that we can debug later how the decision was made
    info!(
        "Liquidation decision: {decision:?},
        full_debt_amount: {full_debt_amount_f},
        full_debt_mv: {full_debt_mv_f},
        liquidatable_debt: {liquidatable_debt},
        liquidation_ratio: {liquidation_ratio},
        liquidatable_amount: {liquidatable_amount},
        liquidatable_mv: {liquidatable_mv},
        expected_collateral_amount: {expected_collateral_amount},
        liquidation_bonus_rate: {liquidation_bonus_rate},
        flash_loan_fee_sf: {flash_loan_fee_sf},
        liquidation_swap_slippage_pct: {liquidation_swap_slippage_pct}
    "
    );

    Ok(decision)
}
