#!/bin/bash
# Analyse liquidation attempts from a terminatooor log file
# Correlates with on-chain winning transactions to understand race outcomes
#
# Usage: ./analyse-liquidations.sh <logfile> [rpc_url]
#
# Examples:
#   ./analyse-liquidations.sh terminatooor-20260217-144115.log
#   ./analyse-liquidations.sh terminatooor-20260217-144115.log https://api.mainnet-beta.solana.com

set -euo pipefail

LOG_FILE="${1:?Usage: $0 <logfile> [rpc_url]}"
RPC_URL="${2:-https://api.mainnet-beta.solana.com}"

if [ ! -f "$LOG_FILE" ]; then
    echo "Error: File not found: $LOG_FILE"
    exit 1
fi

# Strip ANSI codes for processing
CLEAN_LOG=$(mktemp)
sed 's/\x1b\[[0-9;]*m//g' "$LOG_FILE" > "$CLEAN_LOG"
trap "rm -f $CLEAN_LOG" EXIT

python3 - "$CLEAN_LOG" "$RPC_URL" << 'PYEOF'
import json
import os
import re
import subprocess
import sys
from datetime import datetime

LOG_FILE = sys.argv[1]
RPC_URL = sys.argv[2]

def rpc_call(method, params):
    """Make a Solana RPC call."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    try:
        result = subprocess.run(
            ["curl", "-s", RPC_URL, "-X", "POST", "-H", "Content-Type: application/json", "-d", payload],
            capture_output=True, text=True, timeout=15
        )
        data = json.loads(result.stdout)
        return data.get("result")
    except Exception as e:
        print(f"  RPC error: {e}", file=sys.stderr)
        return None

def get_signatures(pubkey, limit=25):
    """Get recent transaction signatures for an account."""
    return rpc_call("getSignaturesForAddress", [pubkey, {"limit": limit}]) or []

def get_transaction(sig):
    """Get parsed transaction details."""
    return rpc_call("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])

def parse_timestamp(ts_str):
    """Parse log timestamp."""
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except:
        return None

def is_error_6016(err):
    """Check if error is Custom(6016) - obligation is healthy / position recovered."""
    if isinstance(err, dict):
        ie = err.get("InstructionError")
        if isinstance(ie, list) and len(ie) >= 2:
            custom = ie[1]
            if isinstance(custom, dict) and custom.get("Custom") == 6016:
                return True
    return False

def analyse_winner_tx(tx):
    """Extract strategy details from a winning transaction."""
    if not tx:
        return None

    meta = tx["meta"]
    msg = tx["transaction"]["message"]
    logs = meta.get("logMessages", [])

    signers = [a["pubkey"] for a in msg["accountKeys"] if a.get("signer")]
    outer_ix_count = len(msg["instructions"])

    # Detect strategy from logs
    has_flash_borrow = any("FlashBorrow" in l for l in logs)
    has_flash_repay = any("FlashRepay" in l for l in logs)
    has_flash_loan = has_flash_borrow and has_flash_repay

    # Detect swap programs
    swap_program = None
    for l in logs:
        if "invoke [1]" in l:
            if "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" in l:
                swap_program = "Jupiter"
            elif "Swap2Ex1jbDx4c2kDVgCrUX3GWCHZsFn4n8vecY5ycf" in l:
                swap_program = "Swap2Ex (custom)"

    # Detect batch vs individual refreshes
    has_batch_refresh = any("RefreshReservesBatch" in l for l in logs)
    has_price_list = any("RefreshPriceList" in l for l in logs)
    individual_refreshes = sum(1 for l in logs if "Instruction: RefreshReserve" in l and "Batch" not in l)

    # Extract bonus
    bonus_bps = None
    for l in logs:
        m = re.search(r"liquidation bonus: (\d+)bps", l)
        if m:
            bonus_bps = int(m.group(1))

    # Extract PnL
    pnl_line = None
    for l in logs:
        if "pnl:" in l:
            pnl_line = l.strip()

    # Extract liquidation amount
    liq_amount = None
    for l in logs:
        m = re.search(r"liquidation amount \(rounded\): ([\d.]+)", l)
        if m:
            liq_amount = m.group(1)

    # Detect ALTs
    alts = msg.get("addressTableLookups", [])

    strategy = "Direct (holds debt)" if not has_flash_loan and not swap_program else ""
    if has_flash_loan and swap_program:
        strategy = f"Flash loan + {swap_program}"
    elif has_flash_loan:
        strategy = "Flash loan"
    elif swap_program:
        strategy = f"Direct + {swap_program}"

    return {
        "signer": signers[0] if signers else "?",
        "signer_short": signers[0][:8] + "..." if signers else "?",
        "slot": tx["slot"],
        "instructions": outer_ix_count,
        "cu": meta["computeUnitsConsumed"],
        "fee": meta["fee"],
        "strategy": strategy,
        "has_flash_loan": has_flash_loan,
        "swap_program": swap_program,
        "has_batch_refresh": has_batch_refresh,
        "has_price_list": has_price_list,
        "individual_refreshes": individual_refreshes,
        "bonus_bps": bonus_bps,
        "liq_amount": liq_amount,
        "pnl_line": pnl_line,
        "alts": len(alts),
    }

# ─── Detect our wallet from keypair path ───

our_wallet = None
keypair_path = None
prev_line_is_keypair = False

with open(LOG_FILE) as f:
    for line in f:
        line = line.strip()
        if prev_line_is_keypair:
            m = re.search(r'"([^"]+)"', line)
            if m:
                keypair_path = m.group(1)
            prev_line_is_keypair = False
            break
        if "keypair: Some(" in line:
            m = re.search(r'keypair: Some\(\s*"([^"]+)"', line)
            if m:
                keypair_path = m.group(1)
                break
            else:
                prev_line_is_keypair = True
        # Stop scanning after startup block
        if "LIQUIDATABLE:" in line:
            break

if keypair_path:
    # Resolve relative to log file directory (program likely ran from parent dir)
    log_dir = os.path.dirname(os.path.abspath(LOG_FILE))
    candidates = [
        os.path.join(log_dir, keypair_path),
        os.path.abspath(keypair_path),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                result = subprocess.run(
                    ["solana-keygen", "pubkey", path],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0:
                    our_wallet = result.stdout.strip()
                    break
            except FileNotFoundError:
                # solana-keygen not installed, try reading JSON directly
                try:
                    with open(path) as kf:
                        key_bytes = json.load(kf)
                    if isinstance(key_bytes, list) and len(key_bytes) == 64:
                        # Use nacl/ed25519 to derive pubkey if available
                        try:
                            from nacl.signing import SigningKey
                            sk = SigningKey(bytes(key_bytes[:32]))
                            import base58
                            our_wallet = base58.b58encode(bytes(sk.verify_key)).decode()
                        except ImportError:
                            pass
                except:
                    pass
            except:
                pass

# ─── Parse log file ───

liquidations = []
current = None

with open(LOG_FILE) as f:
    for line in f:
        line = line.strip()

        # Detect liquidatable
        m = re.search(r"(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s+.*LIQUIDATABLE: (\w+) LTV=([\d.]+) \(unhealthy=([\d.]+)\) slot=(\d+) queue=(\d+)ms", line)
        if m:
            if current:
                liquidations.append(current)
            current = {
                "detect_time": m.group(1),
                "obligation": m.group(2),
                "obligation_short": m.group(2)[:8],
                "ltv": float(m.group(3)),
                "unhealthy_ltv": float(m.group(4)),
                "slot": int(m.group(5)),
                "queue_ms": int(m.group(6)),
                "task_id": None,
                "build_ms": None,
                "jito_ms": None,
                "jito_result": None,
                "jito_429": False,
                "outcome": None,
                "total_ms": None,
                "profit": None,
                "tip_lamports": None,
                "multi_hop": False,
                "multi_hop_ms": None,
            }
            ltv_margin = (float(m.group(3)) - float(m.group(4))) / float(m.group(4)) * 100
            current["ltv_margin_pct"] = round(ltv_margin, 2)
            continue

        if not current:
            continue

        # Task ID
        m = re.search(r"\[T(\d+):", line)
        if m and current["task_id"] is None:
            current["task_id"] = int(m.group(1))

        # Parallel build time
        m = re.search(r"Parallel build completed in (\d+)ms", line)
        if m:
            current["build_ms"] = int(m.group(1))

        # Profitable
        m = re.search(r"PROFITABLE:.*profit = (\d+) debt tokens \(swap_out=(\d+) - repay=(\d+)\)", line)
        if m:
            current["profit"] = int(m.group(1))

        # Jito submitted (with tip extraction)
        m = re.search(r"Jito bundle submitted via (https://\S+) in (\d+)ms \(tip: (\d+) lamports\)", line)
        if m:
            current["jito_result"] = f"submitted ({m.group(1).split('//')[1].split('.')[0]})"
            current["jito_ms"] = int(m.group(2))
            current["tip_lamports"] = int(m.group(3))
            current["outcome"] = "jito_submitted"
            current["total_ms"] = current["queue_ms"] + (current["build_ms"] or 0) + current["jito_ms"]

        # Jito 429 — track separately so later outcomes don't erase it
        if "429 Too Many Requests" in line:
            current["jito_result"] = "429 rate limited"
            current["jito_429"] = True
            if current["outcome"] is None:
                current["outcome"] = "jito_429"

        # Lost race
        if "already liquidated" in line or "Lost race" in line:
            if current["outcome"] != "jito_429":
                current["outcome"] = "lost_race"

        # No swap route
        m = re.search(r"No swap route found from (\w+) to (\w+)", line)
        if m:
            current["outcome"] = "no_swap_route"
            current["no_swap_from"] = m.group(1)[:8]
            current["no_swap_to"] = m.group(2)[:8]

        # RPC tx sent
        m = re.search(r"Liquidation tx sent: (\w+)", line)
        if m:
            current["outcome"] = "rpc_sent"
            current["rpc_sig"] = m.group(1)

        # Collateral not tradable
        if "not tradable" in line.lower() or "TOKEN_NOT_TRADABLE" in line:
            current["outcome"] = "not_tradable"

        # Transaction too large
        m = re.search(r"Transaction too large: (\d+) bytes", line)
        if m:
            current["outcome"] = "tx_too_large"
            current["tx_size"] = int(m.group(1))

        # Reserve ineligible (Max LTV = 0)
        if "Max LTV of the withdraw reserve is 0" in line:
            current["outcome"] = "reserve_ineligible"

        # Multi-hop routing
        m = re.search(r"Multi-hop route found(?: in (\d+)ms)?", line)
        if m:
            current["multi_hop"] = True
            if m.group(1):
                current["multi_hop_ms"] = int(m.group(1))

if current:
    liquidations.append(current)

if not liquidations:
    print("No liquidation attempts found in log file.")
    sys.exit(0)

# ─── Look up winning transactions ───

print(f"\n{'='*80}")
print(f"LIQUIDATION ANALYSIS: {len(liquidations)} attempts")
print(f"Log: {LOG_FILE}")
if our_wallet:
    print(f"Our wallet: {our_wallet}")
print(f"{'='*80}\n")

winners_by_signer = {}
same_slot_count = 0
winner_count = 0
position_recovered_count = 0
no_activity_count = 0

for liq in liquidations:
    tid = liq.get("task_id", "?")
    obl_short = liq["obligation_short"]
    flags = []
    if liq.get("multi_hop"):
        flags.append("MULTI-HOP")
    flag_str = f" [{', '.join(flags)}]" if flags else ""
    print(f"--- T{tid}: {obl_short} (slot {liq['slot']}, LTV margin {liq['ltv_margin_pct']:.2f}%){flag_str} ---")

    # Our timing
    build = liq.get("build_ms", "?")
    jito = liq.get("jito_ms")
    print(f"  Our build: {build}ms | Jito: {liq.get('jito_result', 'N/A')}", end="")
    if jito:
        print(f" ({jito}ms)", end="")
    if liq.get("tip_lamports") is not None:
        tip_sol = liq["tip_lamports"] / 1_000_000_000
        print(f" | Tip: {liq['tip_lamports']} lamports ({tip_sol:.6f} SOL)")
    else:
        print()
    outcome_str = liq['outcome'] or "unknown"
    if liq.get("jito_429") and liq["outcome"] != "jito_429":
        outcome_str += " (after Jito 429)"
    if liq.get("tx_size"):
        outcome_str += f" ({liq['tx_size']} bytes)"
    print(f"  Outcome: {outcome_str}")
    if liq.get("profit"):
        print(f"  Expected profit: {liq['profit']} debt tokens")

    # Skip on-chain lookup for skipped liquidations
    if liq["outcome"] in ("no_swap_route", "not_tradable"):
        print(f"  (skipped swap: {liq.get('no_swap_from','?')} -> {liq.get('no_swap_to','?')})")

    # Look up winner on-chain
    print(f"  Looking up on-chain winner...")
    sigs = get_signatures(liq["obligation"])
    winner_sig = None

    # Find the closest successful tx near our detection slot
    # Use a wider window (±20 slots ≈ 8-10 seconds) to catch winners
    # that landed slightly later due to Jito/leader schedule
    candidates = []
    failed_6016 = []
    for sig_info in sigs:
        sig_slot = sig_info["slot"]
        sig_err = sig_info.get("err")
        if abs(sig_slot - liq["slot"]) <= 20:
            if sig_err is None:
                candidates.append(sig_info)
            elif is_error_6016(sig_err):
                failed_6016.append(sig_info)

    # Pick the closest successful tx to our detection slot
    if candidates:
        candidates.sort(key=lambda s: abs(s["slot"] - liq["slot"]))
        winner_sig = candidates[0]["signature"]

    if winner_sig:
        tx = get_transaction(winner_sig)
        if tx:
            info = analyse_winner_tx(tx)
            if info:
                short_signer = info["signer_short"]
                is_us = our_wallet and info["signer"] == our_wallet
                us_tag = " (US!)" if is_us else ""
                print(f"  WINNER: {short_signer}{us_tag} at slot {info['slot']}")
                print(f"    Strategy: {info['strategy']}")
                print(f"    Instructions: {info['instructions']} | CU: {info['cu']} | Fee: {info['fee']} lamports")
                print(f"    Bonus: {info['bonus_bps']}bps | Batch refresh: {info['has_batch_refresh']}")
                if info["pnl_line"]:
                    pnl_short = info["pnl_line"].split("pnl: ")[1] if "pnl: " in info["pnl_line"] else info["pnl_line"]
                    print(f"    PnL: {pnl_short}")

                # Track competitor stats
                signer = info["signer"]
                if signer not in winners_by_signer:
                    winners_by_signer[signer] = {
                        "short": short_signer,
                        "wins": [],
                        "strategy": info["strategy"],
                        "fees": [],
                        "has_batch_refresh": info["has_batch_refresh"],
                        "is_us": is_us,
                    }
                winners_by_signer[signer]["wins"].append(f"T{tid}")
                winners_by_signer[signer]["fees"].append(info["fee"])

                is_same_slot = info["slot"] == liq["slot"]
                same_slot = "YES" if is_same_slot else f"NO (winner slot {info['slot']}, delta={info['slot'] - liq['slot']})"
                print(f"    Same slot as detection: {same_slot}")

                liq["winner"] = info
                winner_count += 1
                if is_same_slot:
                    same_slot_count += 1
        else:
            print(f"  Winner TX found but couldn't fetch details")
    else:
        # No successful tx found — classify why
        if failed_6016:
            liq["winner_status"] = "position_recovered"
            position_recovered_count += 1
            print(f"  Position recovered (no winner) — {len(failed_6016)} attempt(s) failed with 6016 (obligation healthy)")
        else:
            # Check if there were any txs at all near this slot
            any_nearby = [s for s in sigs if abs(s["slot"] - liq["slot"]) <= 20]
            if any_nearby:
                liq["winner_status"] = "no_winner"
                print(f"  No winning liquidation found near slot {liq['slot']} ({len(any_nearby)} txs, all failed)")
            else:
                liq["winner_status"] = "no_activity"
                no_activity_count += 1
                print(f"  No activity near slot {liq['slot']}")

    print()

# ─── Summary ───

print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}\n")

# Outcome breakdown
outcomes = {}
for liq in liquidations:
    o = liq["outcome"] or "unknown"
    outcomes[o] = outcomes.get(o, 0) + 1

print("Outcomes:")
outcome_labels = {
    "jito_submitted": "Jito submitted (may or may not have landed)",
    "jito_429": "Jito 429 rate limited",
    "lost_race": "Lost race (obligation already liquidated)",
    "no_swap_route": "No swap route available",
    "not_tradable": "Collateral not tradable",
    "rpc_sent": "Sent via RPC fallback",
    "tx_too_large": "Transaction too large (multi-hop overflow)",
    "reserve_ineligible": "Reserve ineligible (Max LTV = 0)",
}
for outcome, count in sorted(outcomes.items(), key=lambda x: -x[1]):
    label = outcome_labels.get(outcome, outcome)
    print(f"  {count}x {label}")

# On-chain results
if winner_count > 0 or position_recovered_count > 0 or no_activity_count > 0:
    print(f"\nOn-chain results:")
    print(f"  {winner_count} won by someone (competitor or us)")
    if position_recovered_count:
        print(f"  {position_recovered_count} position recovered (no winner)")
    if no_activity_count:
        print(f"  {no_activity_count} no on-chain activity")

# Same-slot ratio
if winner_count > 0:
    print(f"\nSame-slot winners: {same_slot_count}/{winner_count} landed in detection slot")

# Competitor analysis
if winners_by_signer:
    print(f"\nCompetitors:")
    print(f"  {'Signer':<12} {'Wins':<15} {'Strategy':<35} {'Avg Fee':<12} {'Batch'}")
    print(f"  {'-'*12} {'-'*15} {'-'*35} {'-'*12} {'-'*5}")
    for signer, info in sorted(winners_by_signer.items(), key=lambda x: -len(x[1]["wins"])):
        avg_fee = sum(info["fees"]) // len(info["fees"])
        wins = ", ".join(info["wins"])
        batch = "Yes" if info["has_batch_refresh"] else "No"
        label = info["short"]
        if info.get("is_us"):
            label += " (us)"
        print(f"  {label:<12} {wins:<15} {info['strategy']:<35} {avg_fee:<12} {batch}")

# Timing analysis
build_times = [l["build_ms"] for l in liquidations if l.get("build_ms") is not None]
if build_times:
    print(f"\nOur build times: min={min(build_times)}ms avg={sum(build_times)//len(build_times)}ms max={max(build_times)}ms")

jito_submitted = [l for l in liquidations if l["outcome"] == "jito_submitted"]
jito_429 = [l for l in liquidations if l.get("jito_429")]
print(f"Jito: {len(jito_submitted)} submitted, {len(jito_429)} rate limited (429)")

# Tip stats
tips = [l["tip_lamports"] for l in liquidations if l.get("tip_lamports") is not None]
if tips:
    avg_tip = sum(tips) // len(tips)
    print(f"Tips: min={min(tips)} avg={avg_tip} max={max(tips)} lamports "
          f"({min(tips)/1e9:.6f} / {avg_tip/1e9:.6f} / {max(tips)/1e9:.6f} SOL)")

# Multi-hop stats
multi_hop_attempts = [l for l in liquidations if l.get("multi_hop")]
if multi_hop_attempts:
    tids = ", ".join(f"T{l.get('task_id','?')}" for l in multi_hop_attempts)
    print(f"Multi-hop routing: {len(multi_hop_attempts)} attempts ({tids})")

# Value at risk (expected profit from attempts where we didn't win)
missed_profits = [l["profit"] for l in liquidations
                  if l.get("profit") and not (l.get("winner") and our_wallet and l["winner"]["signer"] == our_wallet)]
if missed_profits:
    print(f"\nValue at risk: {sum(missed_profits)} debt tokens total from {len(missed_profits)} attempts")

# Wins vs losses
our_wins = [l for l in liquidations if l.get("winner") and our_wallet and l["winner"]["signer"] == our_wallet]
if our_wallet:
    print(f"\nResult: {len(our_wins)} wins / {len(liquidations)} attempts (wallet: {our_wallet[:8]}...)")
else:
    print(f"\nResult: ? wins / {len(liquidations)} attempts (wallet not detected — install solana-keygen or check keypair path)")

print()
PYEOF
