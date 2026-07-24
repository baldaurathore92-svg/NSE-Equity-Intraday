# NSE Equity Intraday — Real-Time Book Dynamics Scanner

**Production-ready order-flow / market-microstructure analytics** के लिए NSE Cash Market Level-2 SnapQuote data पर 100 symbols को parallel में scan करने वाला system।

> कोई candlestick नहीं, कोई RSI/MACD नहीं, कोई OHLC नहीं।
> केवल **real-time tick-by-tick book dynamics** — Angel One SmartAPI + BookDynamicsEngine पर।

---

## 🎯 वर्तमान Focus — Single-file Live Hit Rate Analyzer

अब यह repo सिर्फ एक tool पर focused है: **`live_hit_rate_analyzer.py`**।
`paper_trader.py`, `live_dual_analyzer.py`, `tick_recorder.py`,
`historical_backtest.py`, `COMPARE.sh` और `RECREATE_PROMPT.md` हटा दिए गए हैं।

### Runtime पर सिर्फ इन files की ज़रूरत है

- `live_hit_rate_analyzer.py` — CLI + rich UI + measurement engine
- `nse_book_scanner.py` — BookDynamicsEngine, Angel One WS adapter, session/RVOL gates
- `config.json` (users creates from `config.example.json`)
- `SETUP.sh` — one-command install + launch

### One-command run

```bash
bash SETUP.sh --full -- --strong-only \
    --entry-confirmation-sec 15 \
    --survival-check-sec 15 --survival-min-favor-pct 0.0001
```

नीचे बाकी दस्तावेज़ historical reference के तौर पर बना हुआ है; कुछ command
examples (paper_trader / tick_recorder / historical_backtest वाले) अब उपलब्ध
नहीं हैं और सिर्फ पुराने architecture का हवाला हैं।

---

## 🏗️ Architecture

```
┌─────────────────────────┐
│ Angel One SmartAPI      │  WebSocket V2 (SnapQuote mode)
│ Level-2 Top-5 depth     │
└───────────┬─────────────┘
            │
            ▼  ~5µs enqueue
┌─────────────────────────┐
│ Tick Queue (20k buffer) │  Producer-consumer decoupling
│   (backpressure-aware)  │
└───────────┬─────────────┘
            │
            ▼  ~100µs process
┌─────────────────────────┐
│ BookDynamicsEngine ×100 │  17 microstructure metrics/symbol
│  - L1 / Top-5 / Book-   │  - Rolling ROC (1s/5s/10s)
│    wide imbalance       │  - Spoof / Iceberg suspicion
│  - Weighted depth       │  - Kill switch (spread / circuit)
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Ranked Signal Output    │  JSONL log + Live rich UI
│  STRONG_LONG / LONG     │  Top-N bullish + bearish
│  WEAK_LONG / NEUTRAL    │  Evidence strength 0-100
│  WEAK_SHORT / SHORT     │
│  STRONG_SHORT           │
└─────────────────────────┘
```

---

## ⚡ Performance (measured on 1-core VPS)

| Load | Throughput | Latency p50 | Latency p99 | Drops |
|------|-----------|-------------|-------------|-------|
| NSE normal (500 tps) | 502 tps ✅ | 100 µs | 248 µs | 0 |
| Opening burst (3,000 tps) | 2,923 tps ✅ | 105 µs | 296 µs | 0 |
| Extreme (10,000 tps) | 7,321 tps | 106 µs | 2 ms | 0 (queue absorbs) |

---

## 🚀 Quick Start

### 1. Local machine पर test (कोई credentials नहीं चाहिए)

```bash
# Clone repo
git clone https://github.com/baldaurathore92-svg/NSE-Equity-Intraday.git
cd NSE-Equity-Intraday

# Dependencies
pip install -r requirements.txt

# Engine self-test (8 synthetic scenarios)
python3 nse_book_scanner.py --demo

# Simulate mode with fake ticks (100 symbols)
python3 nse_book_scanner.py --mode simulate

# Higher rate simulation
python3 nse_book_scanner.py --mode simulate --sim-rate 30
```

### 2. Live trading — Angel One credentials चाहिए

```bash
# Config template copy करें
cp config.example.json config.json
chmod 600 config.json

# Edit config.json — Angel One API key, client code, MPIN, TOTP secret भरें
nano config.json

# Live mode
python3 nse_book_scanner.py --mode live
```

### 3. VPS पर deploy (production)

```bash
# One-command auto-installer (Ubuntu 22.04+)
./deploy_vps.sh

# Systemd auto-restart service
./install_service.sh
sudo systemctl start nse-scanner
journalctl -u nse-scanner -f
```

Full VPS guide: see `deploy_vps.sh` output या project wiki।

---

## 📊 Signal Output Example

`logs/signals.jsonl` में हर actionable signal:

```json
{
  "ts": 1721544123.456,
  "symbol": "RELIANCE-EQ",
  "state": "STRONG_LONG",
  "raw_score": 8.34,
  "smoothed_score": 8.12,
  "evidence": 82.1,
  "reasons": [
    "Composite score +8.12/10 (bullish), feature agreement 100%",
    "  L1=+0.72 (w=1.0)",
    "  WeightedDepth=+0.68 (w=2.0)",
    "  ImbalanceROC5s=+0.85 (w=2.5)"
  ],
  "diagnostics": {
    "L1_imbalance": 0.72,
    "Top5_imbalance": 0.61,
    "Spread_bps": 4.2,
    "BuyerAggressorRatio_5s": 0.82,
    "Spoof_Susp": 0.05
  }
}
```

---

## 🧠 The 17 Microstructure Metrics

Analysed per tick, per symbol:

**Static Imbalances** ([-1, +1] range, +ve = bullish)
1. `book_wide_imbalance` — Full book TBQ vs TSQ
2. `l1_imbalance` — Best bid vs best ask qty
3. `top5_imbalance` — Sum Top-5 bids vs asks
4. `weighted_depth_imbalance` — Exponential distance-weighted

**Dynamics (ROC)**
5. `buy_book_roc_1s / 5s / 10s`
6. `sell_book_roc_1s / 5s / 10s`
7. `imbalance_roc_5s`

**Liquidity Flow**
8. `buy_added / buy_removed / sell_added / sell_removed`
9. `book_activity`

**Price Response**
10. `spread` / `normalized_spread_bps`
11. `mid_price_roc_5s`
12. `ltp_roc_5s`
13. `buyer_aggressor_ratio_5s` (tick rule)
14. `interval_volume`

**Suspicion Scores** ([0, 1])
15. `l1_vs_depth_divergence`
16. `execution_likelihood_ask / bid`
17. `spoofing_suspicion` + `iceberg_suspicion` + `replenishment_score`

Composite = Weighted sum → EMA smoothed → `[-10, +10]` score → Signal state.

---

## 🎯 Phase 1: PredictionTracker (Signal Accuracy Self-Validation)

Scanner **खुद ही measure करता है** कि उसके signals actually बाद में सही निकले या नहीं।
कोई अंदाज़ा नहीं, कोई backtest hype नहीं — live empirical proof।

**कैसे काम करता है:**
1. जब actionable signal fire हो (LONG/SHORT states), current LTP capture
2. Configured horizons (default: 30s / 60s / 120s) पर pending predictions create
3. उसी symbol के अगले ticks पर, horizon expire होते ही:
   - Current price vs signal-fire price → directional return
   - LONG signal + price up = ✓ hit
   - SHORT signal + price down = ✓ hit
   - Transaction cost (default 0.06%) deduct करके actual net edge
4. Per-state × horizon aggregated stats → live UI panel + JSONL audit trail

**UI Panel Example:**

```
📈  Prediction Accuracy @ 60s horizon  (cost model: −0.06% round-trip)
┏━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ Signal State ┃ Samples┃ Hit %  ┃ AvgRet  ┃ NetEdge ┃ Verdict         ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ STRONG_LONG  │    42  │  58.3% │  +0.12% │ +0.06%  │ ✓ EDGE          │
│ LONG         │   118  │  52.1% │  +0.04% │ -0.02%  │ ✗ break-even    │
│ WEAK_LONG    │   256  │  49.6% │  +0.01% │ -0.05%  │ ✗ noise (loss)  │
│ STRONG_SHORT │    38  │  57.8% │  -0.14% │ +0.08%  │ ✓ EDGE          │
└──────────────┴────────┴────────┴─────────┴─────────┴─────────────────┘
```

यह real-time proof देता है कि **कौन-सी signal states में असली edge है और कौन-सी में नहीं।**
अक्सर सिर्फ STRONG_LONG/STRONG_SHORT ही tradeable होंगे, weak signals noise होंगे।

**JSONL output** (`logs/predictions.jsonl`): हर evaluated prediction का पूरा record —
`ts_fired`, `ts_evaluated`, `symbol`, `state`, `score`, `evidence`,
`price_at_signal`, `price_at_horizon`, `directional_return_pct`, `net_return_pct`,
`is_hit`, `is_net_profitable`, `timed_out`.

**Config** (in `config.example.json`):
```json
"prediction_horizons_s": [30.0, 60.0, 120.0],
"transaction_cost_pct": 0.0006,
"prediction_display_horizon_s": 60.0,
"prediction_min_samples_for_verdict": 20
```

---

## 🌀 Phase 2: Regime Detector (`--regime-adaptive`)

Real markets change character throughout the day. What worked in trending regime
fails in mean-reverting. Phase 2 classifies current regime per-symbol on 3
dimensions and adapts trading behavior automatically.

### Regime Dimensions

| Dimension | Values | Detection Method |
|-----------|--------|------------------|
| **Volatility** | LOW / NORMAL / HIGH | Recent σ vs baseline σ ratio |
| **Trend** | TRENDING_UP / TRENDING_DOWN / MEAN_REVERTING / RANDOM | Lag-1 autocorrelation of tick returns |
| **Depth Bias** | BULL_STRUCTURAL / BEAR_STRUCTURAL / BALANCED | Rolling mean of book-wide imbalance |

### Adaptive Behavior

- **RANDOM regime** → Skip signal (no directional edge to exploit)
- **MEAN_REVERTING regime** → INVERT signal (LONG becomes SHORT, contrarian trade)
- **HIGH_VOL regime** → Widen entry threshold (1.3×) + halve position size
- **LOW_VOL regime** → Tighten entry threshold (0.85×) to catch more marginal moves
- **TRENDING regime** → Use signals as-is (normal directional trade)

### Live Paper Trading on Real Angel One Data

```bash
# Simulation with Phase 2 (no broker needed)
python3 paper_trader.py --duration-min 60 --regime-adaptive

# LIVE paper trading on real Angel One WebSocket (during NSE market hours)
python3 paper_trader.py --feed live --config config.json --duration-min 390 --regime-adaptive

# Aggressive tuning
python3 paper_trader.py --feed live --config config.json --regime-adaptive \
    --entry-score 3 --entry-evidence 25
```

### What Phase 2 Adaptive Does NOT Do

**⚠️ Important honesty:** Phase 2 is not a magic profit switch. In realistic
simulation, adding `--regime-adaptive` may make results WORSE if the base
scanner doesn't have real edge. Its actual value is:

1. **Observability** — See exact regime distribution during trading hours
2. **Risk management** — Auto-reduce size in HIGH_VOL periods
3. **Real-market alpha discovery** — On real NSE data, mean-reversion inversions
   may capture actual over-reaction alpha (simulator can't replicate this)

Run on REAL Angel One data for 5-10 days before drawing conclusions about
regime-adaptive value.

---

## 📊 Live Hit Rate Analyzer (`live_hit_rate_analyzer.py`)

**Real-time scanner + horizon-based statistical validator in one tool.**
No trade simulation, no capital tracking — pure predictive accuracy measurement.

### Two-Layer Tracking

**1. Real-Time Layer** — Every tick, every open signal:
- Current directional return (positive = signal is proving RIGHT NOW)
- MFE (Max Favorable Excursion) — best moment during signal life
- MAE (Max Adverse Excursion) — worst moment during signal life
- Live verdict: "X open / Y winning / Z losing right now"

**2. Statistical Layer** — At each configured horizon:
- Multi-dimensional bucketed hit rate (state × horizon × evidence × regime × hour × symbol)
- Cost-adjusted net edge
- Honest verdict per bucket

### What The UI Shows

```
╭─ ⚡ LIVE VERDICT (open signals): 24 open → 15 winning / 8 losing ─╮
│  Hit rate now: 62.5%  |  Avg current: +0.043%  |  Evaluated: 342 │
╰──────────────────────────────────────────────────────────────────╯

⚡ LIVE OPEN SIGNALS — Real-time score verdict
┏━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━┳━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━┓
┃ Symbol     ┃ State     ┃ Age ┃ Entry ┃ Now    ┃ Dir Ret┃ MFE    ┃ Status     ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━╇━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━┩
│ RELIANCE-EQ│ STRONG_LONG│ 12s │ 2530.15│ 2534.22│+0.161% │+0.180% │✓✓ PROFITABLE│
│ HDFCBANK-EQ│ LONG      │ 45s │ 3327.41│ 3325.02│-0.072% │+0.045% │✗ losing     │
│ ...        │ ...       │ ... │ ...    │ ...    │...     │ ...    │ ...         │
```

**यह जवाब देता है:** "अभी scanner सही जा रहा है या गलत? Market हमारे हिसाब से चल रही है या नहीं?"

### Multi-Horizon Statistical Analysis

Every actionable signal fire पर 6 horizons पर pending records बनते हैं:
5s / 15s / 30s / 60s / 120s / 300s। हर horizon expire होने पर actual price check।

### Multi-Dimensional Breakdown

| Dimension | What It Reveals |
|-----------|-----------------|
| **By State** (STRONG_LONG / LONG / WEAK_LONG / ...) | कौन-सा signal state predictive है? |
| **By Horizon** (5s to 300s) | Optimal holding period क्या है? |
| **By Evidence** (0-30 / 30-50 / 50-70 / 70+) | High-evidence signals बेहतर हैं? |
| **By Regime** (Phase 2 label) | कौन-सा market regime tradeable है? |
| **By Hour of Day** (IST) | Opening / mid / closing में accuracy अलग है? |
| **By Symbol** (top-10 ranked) | कौन-से stocks scanner पर predict करते हैं? |

### Usage

```bash
# Default 60-min session, rich UI
python3 live_hit_rate_analyzer.py --config config.json

# Full trading day headless (VPS tmux)
python3 live_hit_rate_analyzer.py --config config.json --duration-hours 6.5 --no-ui

# Custom horizons + symbols
python3 live_hit_rate_analyzer.py --config config.json \
    --horizons 10,30,60,180,600 --symbols RELIANCE-EQ,TCS-EQ,HDFCBANK-EQ

# Custom transaction cost (0.10% instead of default 0.06%)
python3 live_hit_rate_analyzer.py --config config.json --cost-pct 0.001
```

### 24/7 systemd Service (VPS)

```bash
./install_hitrate_service.sh
sudo systemctl start nse-hitrate-analyzer
journalctl -u nse-hitrate-analyzer -f
```

Auto-starts every trading day, auto-stops at 15:30 IST, auto-restarts on crash.

### Sample Output — Real Data EOD Report

```
══════════════════════════════════════════════════════════════════════════════
  🎯 HIT RATE BY STATE × HORIZON
──────────────────────────────────────────────────────────────────────────────
  State          Horizon    N   Hit %  NetProfit %  AvgRet %  NetEdge %  Verdict
  ────────────────────────────────────────────────────────────────────────────
  STRONG_LONG      30s     42   58.3%      52.4%    +0.14%    +0.08%   ✓ edge
  STRONG_LONG      60s     42   64.3%      59.5%    +0.19%    +0.13%   ✓✓ STRONG EDGE
  STRONG_LONG     120s     42   61.9%      54.8%    +0.22%    +0.16%   ✓✓ STRONG EDGE
  LONG             30s    186   51.6%      43.5%    +0.03%   -0.03%   ✗ break-even
  LONG             60s    186   53.2%      47.3%    +0.06%   +0.00%   ~ marginal
  WEAK_LONG        60s    412   49.5%      41.7%   -0.01%   -0.07%   ✗ noise
  ...

  📌 HONEST OVERALL VERDICT
  ──────────────────────────────────────────────────────────────────────────
  Total predictions evaluated: 8,432
  Overall directional hit rate: 51.8%
  Overall NET profit rate: 44.2% (after 0.06% cost)
  Average net edge per signal: +0.023%

  🟡 BREAK-EVEN: Signals slightly predictive but cost eats edge.
     Recommendation: Focus on STRONG_LONG/STRONG_SHORT only, or refine params.
```

### Output Files

- `logs/hit_rate_predictions.jsonl` — every evaluated prediction (audit trail)
- `logs/hit_rate_report.txt` — comprehensive EOD text report

**यह tool की USP:** अगर scanner में asli edge है, यह precisely बताएगा
**कौन-से** state + horizon + regime + hour में। अगर edge नहीं है, तो भी honestly बताएगा।

---

## 📼 Record→Replay Workflow (Real Tick Backtesting)

**यह सबसे important workflow है for honest strategy validation.**

Random-walk simulators can never replicate real NSE microstructure —
actual spoofing, iceberg orders, institutional flow, news reactions,
cross-symbol correlation. So the ONLY way to know if the scanner has
real alpha is to **record live NSE data and backtest on it**.

### Two-Tool Pipeline

```
   ┌──────────────────────────┐         ┌──────────────────────────┐
   │  tick_recorder.py         │         │  historical_backtest.py  │
   │  (Days 1-5, live market)  │────────►│  (Day 6+, offline)       │
   │                           │  data/  │                          │
   │  Angel One WebSocket      │ *.jsonl │  Read recorded ticks     │
   │  → gzip JSONL (hourly)    │  .gz    │  → BookDynamicsEngine    │
   │  ~500 MB/day compressed   │         │  → PaperExecutor         │
   │                           │         │  → EOD comprehensive     │
   │                           │         │     report               │
   └──────────────────────────┘         └──────────────────────────┘
```

### Step 1: Record Real NSE Ticks (5 trading days)

```bash
# On your VPS, during market hours:
tmux new -s recorder
cd NSE-Equity-Intraday
python3 tick_recorder.py --config config.json --output-dir data/
# Auto-stops at 15:30 IST. Ctrl+B, D to detach.
```

**Output:**
```
data/
├── 2026-07-22/
│   ├── ticks_2026-07-22_09.jsonl.gz    ← 45 MB (opening)
│   ├── ticks_2026-07-22_10.jsonl.gz    ← 38 MB
│   ├── ...
│   └── ticks_2026-07-22_15.jsonl.gz    ← 42 MB (closing burst)
├── 2026-07-23/
...
```

Total for 5 days × 100 symbols: **~2-3 GB compressed**.

### Step 2: Backtest on Recorded Real Data

```bash
# Default parameters
python3 historical_backtest.py --data-dir data/

# With Phase 2 regime adaptive
python3 historical_backtest.py --data-dir data/ --regime-adaptive

# Specific symbols
python3 historical_backtest.py --data-dir data/ --symbols RELIANCE-EQ,TCS-EQ

# Specific date range
python3 historical_backtest.py --data-dir data/ \
    --from-date 2026-07-22 --to-date 2026-07-26

# Tune parameters (multiple runs on SAME data — this is the value!)
python3 historical_backtest.py --data-dir data/ --entry-score 3   # aggressive
python3 historical_backtest.py --data-dir data/ --entry-score 5   # default
python3 historical_backtest.py --data-dir data/ --entry-score 7   # conservative
```

You'll get the same comprehensive report as paper_trader.py, but the numbers
are **based on real market data** — real hit rate, real edge or lack thereof.

### Step 3: systemd Service (Optional but Recommended)

For 24/7 recording without manual tmux management, install as systemd service:

```bash
# Create service file:
sudo tee /etc/systemd/system/nse-tick-recorder.service <<EOF
[Unit]
Description=NSE Tick Recorder (Angel One SnapQuote Capture)
After=network-online.target

[Service]
Type=simple
User=${USER}
WorkingDirectory=/home/${USER}/NSE-Equity-Intraday
Environment=PATH=/home/${USER}/NSE-Equity-Intraday/venv/bin:/usr/bin
ExecStart=/home/${USER}/NSE-Equity-Intraday/venv/bin/python3 \\
    /home/${USER}/NSE-Equity-Intraday/tick_recorder.py \\
    --config /home/${USER}/NSE-Equity-Intraday/config.json \\
    --output-dir /home/${USER}/nse_data
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Enable + start:
sudo systemctl daemon-reload
sudo systemctl enable nse-tick-recorder
sudo systemctl start nse-tick-recorder

# Monitor:
journalctl -u nse-tick-recorder -f
```

Recorder will now start automatically on VPS boot and restart on any crash.
Auto-stops at 15:30 IST daily; will restart next morning if VPS is on.

### Why This Approach is Right

| | Random Simulator | **Real Recorded Ticks** |
|---|---|---|
| Order flow intent | Fake (RNG) | **Real (institutional + retail)** |
| Spoofing patterns | None | **Actual operator behavior** |
| Iceberg orders | Fake | **Real hidden liquidity** |
| News reactions | None | **Real fat-tail moves** |
| Time-of-day effects | Approximated | **Genuine microstructure** |
| Cross-symbol correlation | Zero | **Real Nifty gravity** |
| **Backtest validity** | **Illusion** | **Ground truth** |

### 🔄 The Real Iteration Loop

```
1. Record 5 days of real data (once)
2. Try 20 different parameter combos in 1 hour  ← THIS is the payoff
3. Compare results side-by-side
4. Best config → paper trade with --feed live for 5 more days
5. If STILL profitable → very small real capital (₹10K max)
6. Scale slowly only after multi-week validation
```

---

## 🎯 How to Test on Real Data (Live Paper Trading)

The realistic simulator is our best offline approximation of NSE, but only
real data can prove/disprove profitability. Here's how:

### Prerequisites

1. Angel One SmartAPI account with API key + TOTP secret
2. VPS with 1+ vCPU, 2GB+ RAM in Mumbai region (for lowest latency)
3. Python 3.9+ and dependencies installed

### Live Paper Trading Setup

```bash
# 1. Deploy on VPS
git clone https://github.com/baldaurathore92-svg/NSE-Equity-Intraday.git
cd NSE-Equity-Intraday
./deploy_vps.sh   # installs deps, sets timezone, etc.

# 2. Configure credentials
cp config.example.json config.json
chmod 600 config.json
nano config.json   # fill Angel One api_key, client_code, pin, totp_secret

# 3. Start LIVE paper trading (during 9:15-15:30 IST)
python3 paper_trader.py --feed live --config config.json \
    --duration-min 390 --regime-adaptive

# Or run in background (tmux) for full trading day:
tmux new -s paper
python3 paper_trader.py --feed live --config config.json --duration-min 390 --regime-adaptive
# Ctrl+B, then D to detach. tmux attach -t paper to reattach.
```

### What You'll See in the EOD Report

After 6.5 hours of real NSE ticks:
- Total trades executed (virtual — no real orders placed)
- Actual hit rate on REAL market data
- Real signal→price attribution (was scanner right or wrong?)
- Regime distribution (what NSE actually looks like today)
- Per-state performance breakdown
- Comprehensive HONEST verdict (STRONG / MARGINAL / BREAKEVEN / LOSING)

### Interpretation Framework

After 5-10 trading days of live paper trading:

| Result | Meaning | Action |
|--------|---------|--------|
| Win rate < 45% | No edge. Realistic. | Do NOT deploy real money. Rebuild strategy. |
| Win rate 45-52% | Break-even before costs. | Improve signal quality (Phase 3/4). |
| Win rate 52-58% | Marginal edge exists. | Refine risk management, then test with small capital. |
| Win rate > 58% | Statistically likely real edge. | Cautiously deploy small capital, keep expanding paper set. |

**Remember:** Even with real edge, retail Level-2 feed has 50-200ms latency vs
institutional colo (<1ms). Some signals will already be "priced in" by the time
you see them.

---

## 🛡️ Safety Features

- **Kill switch** — Auto-suppress on spread widening > 3× median
- **Circuit filter detection** — Auto-suppress at upper/lower circuit
- **Signal deduplication** — Same state within 5s not repeated
- **Backpressure counter** — Alerts if worker falling behind WS
- **Rate limiting** — Log throttling to avoid disk spam
- **Rotating logs** — 10 MB × 5 backups

---

## 📋 Configuration Reference

`config.json` में तीन sections:

### `angel_one` — Broker credentials
```json
"angel_one": {
    "api_key":     "YOUR_SMARTAPI_KEY",
    "client_code": "YOUR_CLIENT_CODE",
    "pin":         "YOUR_4_DIGIT_MPIN",
    "totp_secret": "YOUR_BASE32_TOTP_SECRET"
}
```

### `scanner` — Runtime behavior
```json
"scanner": {
    "min_evidence_strength_to_log": 30,
    "log_signal_states": ["WEAK_LONG", "LONG", "STRONG_LONG",
                          "WEAK_SHORT", "SHORT", "STRONG_SHORT"],
    "signal_dedup_seconds": 5.0,
    "ui_refresh_ms": 500,
    "top_n_display": 10,
    "tick_queue_size": 20000
}
```

### `engine` — BookDynamicsEngine tuning
```json
"engine": {
    "history_seconds": 15,
    "depth_decay_frac": 0.005,
    "ema_alpha": 0.3,
    "threshold_strong": 8.0,
    "threshold_normal": 5.0,
    "threshold_weak": 2.0,
    "spoof_dampener_strength": 0.5,
    "kill_switch_spread_multiplier": 3.0
}
```

---

## 🗂️ Repository Structure

```
NSE-Equity-Intraday/
├── nse_book_scanner.py       (Main scanner — engine + scanner + UI, 2,588 lines)
├── config.example.json       (Config template with Nifty 100 symbols)
├── requirements.txt          (Python dependencies)
├── deploy_vps.sh             (One-command VPS auto-installer)
├── install_service.sh        (Systemd auto-restart service)
├── .gitignore                (Sensitive files excluded)
└── README.md                 (This file)
```

---

## ⚠️ Important Disclaimers

- **This is analytical infrastructure, not investment advice.** Signal output represents book dynamics observations; profit/loss depends entirely on your risk management and execution strategy.
- **Paper trade first.** Run in `--mode simulate` for at least 2-4 weeks before real capital.
- **SEBI compliance:** Algorithmic trading with retail brokers requires disclosure. Check Angel One's algo trading policy.
- **Data caveats:**
  - Angel One SnapQuote = book-update snapshots, NOT true per-trade tick-by-tick
  - Spoofing/iceberg detection uses probabilistic *_suspicion scores, never guaranteed
  - Cancel vs Execute inference is heuristic (Lee-Ready tick rule)

---

## 📜 License

MIT — for the user's own trading system। Attribution appreciated but not required.

---

## 🙏 Credits

Designed collaboratively — critical review + implementation + optimization iterations across:
- Order-flow theory (17 microstructure metrics)
- Production infrastructure (producer-consumer queue, systemd, VPS deployment)
- Performance tuning (bisect-based history, batched pruning, cached medians)
