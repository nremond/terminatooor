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

        # Jito submitted
        m = re.search(r"Jito bundle submitted via (https://\S+) in (\d+)ms", line)
        if m:
            current["jito_result"] = f"submitted ({m.group(1).split('//')[1].split('.')[0]})"
            current["jito_ms"] = int(m.group(2))
            current["outcome"] = "jito_submitted"
            # Calculate total from detect to jito submit
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

if current:
    liquidations.append(current)

if not liquidations:
    print("No liquidation attempts found in log file.")
    sys.exit(0)

# ─── Look up winning transactions ───

print(f"\n{'='*80}")
print(f"LIQUIDATION ANALYSIS: {len(liquidations)} attempts")
print(f"Log: {LOG_FILE}")
print(f"{'='*80}\n")

winners_by_signer = {}
results = []

for liq in liquidations:
    tid = liq.get("task_id", "?")
    obl_short = liq["obligation_short"]
    print(f"--- T{tid}: {obl_short} (slot {liq['slot']}, LTV margin {liq['ltv_margin_pct']:.2f}%) ---")

    # Our timing
    build = liq.get("build_ms", "?")
    jito = liq.get("jito_ms")
    print(f"  Our build: {build}ms | Jito: {liq.get('jito_result', 'N/A')}", end="")
    if jito:
        print(f" ({jito}ms)")
    else:
        print()
    outcome_str = liq['outcome'] or "unknown"
    if liq.get("jito_429") and liq["outcome"] != "jito_429":
        outcome_str += " (after Jito 429)"
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
    our_failed = []

    # Find the closest successful tx near our detection slot
    # Use a wider window (±20 slots ≈ 8-10 seconds) to catch winners
    # that landed slightly later due to Jito/leader schedule
    candidates = []
    for sig_info in sigs:
        sig_slot = sig_info["slot"]
        sig_err = sig_info.get("err")
        if sig_err is None and abs(sig_slot - liq["slot"]) <= 20:
            candidates.append(sig_info)

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
                print(f"  WINNER: {short_signer} at slot {info['slot']}")
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
                    }
                winners_by_signer[signer]["wins"].append(f"T{tid}")
                winners_by_signer[signer]["fees"].append(info["fee"])

                same_slot = "YES" if info["slot"] == liq["slot"] else f"NO (winner slot {info['slot']})"
                print(f"    Same slot as detection: {same_slot}")

                liq["winner"] = info
        else:
            print(f"  Winner TX found but couldn't fetch details")
    else:
        print(f"  No winning liquidation found near slot {liq['slot']}")

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
}
for outcome, count in sorted(outcomes.items(), key=lambda x: -x[1]):
    label = outcome_labels.get(outcome, outcome)
    print(f"  {count}x {label}")

# Competitor analysis
if winners_by_signer:
    print(f"\nCompetitors:")
    print(f"  {'Signer':<12} {'Wins':<15} {'Strategy':<35} {'Avg Fee':<12} {'Batch'}")
    print(f"  {'-'*12} {'-'*15} {'-'*35} {'-'*12} {'-'*5}")
    for signer, info in sorted(winners_by_signer.items(), key=lambda x: -len(x[1]["wins"])):
        avg_fee = sum(info["fees"]) // len(info["fees"])
        wins = ", ".join(info["wins"])
        batch = "Yes" if info["has_batch_refresh"] else "No"
        print(f"  {info['short']:<12} {wins:<15} {info['strategy']:<35} {avg_fee:<12} {batch}")

# Timing analysis
build_times = [l["build_ms"] for l in liquidations if l.get("build_ms") is not None]
if build_times:
    print(f"\nOur build times: min={min(build_times)}ms avg={sum(build_times)//len(build_times)}ms max={max(build_times)}ms")

jito_submitted = [l for l in liquidations if l["outcome"] == "jito_submitted"]
jito_429 = [l for l in liquidations if l.get("jito_429")]
print(f"Jito: {len(jito_submitted)} submitted, {len(jito_429)} rate limited (429)")

# Wins vs losses
wins = [l for l in liquidations if l.get("winner") and l["winner"]["signer"] == "TODO_OUR_WALLET"]
print(f"\nResult: 0 wins / {len(liquidations)} attempts")  # TODO: detect our wallet

print()
PYEOF