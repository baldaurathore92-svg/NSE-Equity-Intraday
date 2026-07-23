#!/bin/bash
#
# COMPARE.sh — Auto-run 10 backtest strategies on same recorded ticks
# and compare their PnL / Net Edge / Win Rate in a single table.
#
# USAGE:
#   1. First, record ticks:
#        python3 tick_recorder.py --config config.json --output-dir /root/nse_data
#   2. After market close, run this script:
#        bash COMPARE.sh
#   3. सारे 10 strategies parallelly / sequentially run होंगे
#      और अंत में एक comparison table print होगी।
#

set -e

DATA_DIR="${DATA_DIR:-/root/nse_data}"
CODE_DIR="${CODE_DIR:-/root/NSE-Equity-Intraday}"
RESULTS_DIR="${RESULTS_DIR:-/root/backtest_results}"

# Colors
BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

step()  { echo -e "\n${BLUE}▶ $1${NC}"; }
ok()    { echo -e "${GREEN}  ✓ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }

echo -e "${BLUE}"
cat <<'BANNER'
╔══════════════════════════════════════════════════════════╗
║   BACKTEST COMPARISON — 10 Strategies vs Same Data      ║
╚══════════════════════════════════════════════════════════╝
BANNER
echo -e "${NC}"

# --- Sanity checks ---
if [ ! -d "$DATA_DIR" ] || [ -z "$(ls -A $DATA_DIR 2>/dev/null)" ]; then
    warn "No recorded ticks found in $DATA_DIR"
    echo "  First run: python3 tick_recorder.py --config config.json --output-dir $DATA_DIR"
    exit 1
fi

DATA_SIZE=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
FILE_COUNT=$(ls "$DATA_DIR" | wc -l)
ok "Data directory: $DATA_DIR"
ok "Recorded files: $FILE_COUNT (total $DATA_SIZE)"

mkdir -p "$RESULTS_DIR"
cd "$CODE_DIR"
source venv/bin/activate

# --- Define 10 strategies ---
declare -a STRATEGY_NAMES=(
    "01_baseline"
    "02_aggressive"
    "03_conservative"
    "04_regime_adaptive"
    "05_ultra_strict"
    "06_tight_SL"
    "07_wide_SL"
    "08_quick_TP"
    "09_long_hold"
    "10_best_of_best"
)

declare -a STRATEGY_FLAGS=(
    ""
    "--entry-score 3.5 --entry-evidence 25"
    "--entry-score 5.0 --entry-evidence 40"
    "--regime-adaptive"
    "--entry-score 5.0 --regime-adaptive"
    "--stop-loss-pct 0.0020"
    "--stop-loss-pct 0.0050"
    "--take-profit-pct 0.0030"
    "--max-hold-sec 600"
    "--entry-score 5.0 --entry-evidence 40 --regime-adaptive --stop-loss-pct 0.0025 --take-profit-pct 0.0060"
)

declare -a STRATEGY_DESCRIPTIONS=(
    "Baseline (default settings)"
    "Aggressive (score>=3.5, evid>=25)"
    "Conservative (score>=5.0, evid>=40)"
    "Regime-adaptive filter"
    "Ultra strict + regime"
    "Tight stop-loss (0.20%)"
    "Wide stop-loss (0.50%)"
    "Quick take-profit (0.30%)"
    "Long hold time (10 min)"
    "Best of Best combo"
)

# --- Run each strategy ---
step "Running 10 backtest strategies (~1-5 min each)..."

for i in "${!STRATEGY_NAMES[@]}"; do
    NAME="${STRATEGY_NAMES[$i]}"
    FLAGS="${STRATEGY_FLAGS[$i]}"
    DESC="${STRATEGY_DESCRIPTIONS[$i]}"

    LOG="$RESULTS_DIR/${NAME}.log"
    TRADES_LOG="$RESULTS_DIR/${NAME}_trades.jsonl"

    echo ""
    echo -e "${YELLOW}  [$((i+1))/10] ${NAME}${NC} — $DESC"
    echo -e "${YELLOW}    Flags: ${FLAGS:-(defaults)}${NC}"

    # Run backtest (redirect stdout to log; keep stderr on screen for progress)
    python3 historical_backtest.py \
        --data-dir "$DATA_DIR" \
        --trades-log "$TRADES_LOG" \
        $FLAGS > "$LOG" 2>&1 || {
            warn "    ⚠ Strategy failed (see $LOG for details)"
            continue
        }
    ok "    Completed → $LOG"
done

# --- Extract results and compare ---
step "Extracting comparison metrics..."

# Python script to parse each log and build comparison table
python3 <<PYEOF
import re
import os

results_dir = "$RESULTS_DIR"
strategies = [
    "01_baseline",
    "02_aggressive",
    "03_conservative",
    "04_regime_adaptive",
    "05_ultra_strict",
    "06_tight_SL",
    "07_wide_SL",
    "08_quick_TP",
    "09_long_hold",
    "10_best_of_best",
]
descriptions = [
    "Baseline (default)",
    "Aggressive entries",
    "Conservative entries",
    "Regime-adaptive",
    "Ultra strict + regime",
    "Tight SL (0.20%)",
    "Wide SL (0.50%)",
    "Quick TP (0.30%)",
    "Long hold (10min)",
    "Best of Best combo",
]

def parse_report(log_path):
    """Extract key metrics from backtest report."""
    if not os.path.exists(log_path):
        return None
    with open(log_path) as f:
        text = f.read()

    metrics = {
        "trades": None, "win_rate": None, "total_pnl": None,
        "pnl_pct": None, "profit_factor": None, "sharpe": None,
        "avg_return": None, "max_drawdown": None,
    }

    patterns = {
        "trades":         r"Total trades\s*:?\s*([\d,]+)",
        "win_rate":       r"Winners\s*:?\s*[\d,]+\s*\(([\d.]+)%",
        "total_pnl":      r"Net P&L\s*:?\s*₹\s*([+\-]?[\d,.]+)",
        "pnl_pct":        r"Net P&L\s*:?\s*₹.*?\(([+\-]?[\d.]+)%",
        "profit_factor":  r"Profit factor\s*:?\s*([\d.]+)",
        "sharpe":         r"Trade Sharpe.*?:?\s*([+\-]?[\d.]+)",
        "avg_return":     r"Avg return per trade\s*:?\s*([+\-]?[\d.]+)%",
        "max_drawdown":   r"Max drawdown\s*:?\s*([\d.]+)%",
    }
    for key, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            metrics[key] = m.group(1).replace(",", "")
    return metrics

# Print comparison table
print()
print("━" * 130)
print(f"  {'#':<3} {'Strategy':<24} {'Trades':>8} {'Win%':>7} {'PnL(₹)':>12} {'PnL%':>8} {'PF':>7} {'AvgRet%':>10} {'MaxDD%':>8} {'Sharpe':>8}")
print("━" * 130)

best_pnl = float('-inf')
best_idx = None
best_pf = float('-inf')
best_pf_idx = None

for i, (name, desc) in enumerate(zip(strategies, descriptions)):
    log = os.path.join(results_dir, f"{name}.log")
    m = parse_report(log)
    if m is None:
        print(f"  {i+1:<3} {desc:<24} {'(missing)':>8}")
        continue

    trades = m['trades'] or "-"
    win = f"{m['win_rate']}%" if m['win_rate'] else "-"
    pnl = f"₹{m['total_pnl']}" if m['total_pnl'] else "-"
    pnl_pct = f"{m['pnl_pct']}%" if m['pnl_pct'] else "-"
    pf = m['profit_factor'] or "-"
    avg_ret = f"{m['avg_return']}%" if m['avg_return'] else "-"
    mdd = f"{m['max_drawdown']}%" if m['max_drawdown'] else "-"
    sharpe = m['sharpe'] or "-"

    print(f"  {i+1:<3} {desc:<24} {trades:>8} {win:>7} {pnl:>12} {pnl_pct:>8} {pf:>7} {avg_ret:>10} {mdd:>8} {sharpe:>8}")

    try:
        pnl_num = float(m['total_pnl'])
        if pnl_num > best_pnl:
            best_pnl = pnl_num
            best_idx = i
    except: pass
    try:
        pf_num = float(m['profit_factor'])
        if pf_num > best_pf:
            best_pf = pf_num
            best_pf_idx = i
    except: pass

print("━" * 130)
if best_idx is not None:
    print()
    print(f"  🏆 Best PnL:            #{best_idx+1} {descriptions[best_idx]}  (₹{best_pnl:+,.2f})")
if best_pf_idx is not None:
    print(f"  🏆 Best Profit Factor:  #{best_pf_idx+1} {descriptions[best_pf_idx]}  (PF {best_pf:.2f})")
print()
print("  Full reports saved in: $RESULTS_DIR/")
print("  Trade-by-trade logs:   $RESULTS_DIR/*_trades.jsonl")
print()
PYEOF

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✓ Comparison complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Individual strategy reports: $RESULTS_DIR/*.log"
echo "  Trade-by-trade logs:         $RESULTS_DIR/*_trades.jsonl"
echo ""
echo "  Want to see a specific strategy in detail?"
echo "    less $RESULTS_DIR/10_best_of_best.log"
echo ""
