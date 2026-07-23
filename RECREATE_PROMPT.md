# 🔧 Full System Recreation Prompt

**Purpose:** यह prompt किसी भी capable AI (Claude Sonnet 4.5+, GPT-4o, Gemini 2.5 Pro) को दो, और वो पूरा NSE Equity Intraday scanner + hit rate analyzer + paper trader system फिर से बना देगा — इस repository के equivalent level पर।

**Usage:**
1. एक नया chat/conversation शुरू करो
2. यह पूरा file (RECREATE_PROMPT.md) उसमें paste करो
3. AI से बोलो: "Build this system step-by-step. Ask me before each critical decision."
4. AI file-by-file बनाएगा और आपसे confirm करेगा

---

## 📋 THE PROMPT (copy from here to end)

---

You are building a production-ready **NSE Equity Intraday Scanner + Real-Time Hit Rate Analyzer + Paper Trader** for Indian retail traders. This is a mission-critical system that must handle live market data from Angel One SmartAPI, analyze order-flow microstructure at 100+ ticks/sec, and honestly measure whether signals are profitable AFTER transaction costs.

Before writing any code, read this ENTIRE specification. Then ask me clarifying questions before building. Priority: **correctness over speed**. Better to ship 1 working file than 10 broken ones.

---

### 🎯 THE PROBLEM (why this exists)

**SEBI 2024 Report:** 90% retail intraday traders lose money. Reason: they use lagging indicators (RSI, MACD, moving averages) that HFT/institutional traders arbitrage away. Real edge exists only in **order-flow microstructure** — the shape of the order book, imbalances, aggression patterns, spoofing detection.

**But:** Even microstructure signals face **alpha decay** (edge dies in seconds) and **transaction costs** (0.06% round-trip eats small edges). Most naive traders don't measure this honestly.

**Goal:** Build a system that:
1. Detects microstructure signals in real-time
2. Measures their actual hit rate + cost-adjusted edge
3. Paper-trades them realistically
4. Provides **honest verdict**: "This signal makes money" or "This is noise"

---

### 👤 USER PROFILE (critical to understand)

- **Language:** Hindi/English mix (Hinglish). All CLI help + comments should be bilingual or Hindi-friendly.
- **Technical level:** Non-technical. Cannot debug Python. Wants ONE-command deployment.
- **Hardware:** 1-core CPU, 2 GB RAM, 40 GB disk VPS (Ubuntu 22.04 or 24.04). Multiple accounts possible for A/B testing.
- **API access:** Angel One SmartAPI (SmartConnect + SmartWebSocketV2). India-based servers preferred for latency.
- **Goal:** Learn if microstructure signals can be profitable, WITHOUT losing real money in the process.

### 🧠 USER'S THINKING STYLE

- Extremely sharp — will catch bugs by empirical data analysis (e.g., "score never crosses 5.0, is your threshold=8 unreachable?")
- Ready to challenge assumptions ("50% win rate is just coin flip, what's the point?")
- Appreciates HONEST feedback over hype ("scanner shows loss — that IS the reality, not a bug")
- Trader mindset preferred over engineering mindset (see Gemini section below)

---

### 🏗️ SYSTEM ARCHITECTURE

**6 Python modules + 5 shell scripts = complete system**

```
NSE-Equity-Intraday/
├── nse_book_scanner.py          [3,500+ lines] — Core engine
├── paper_trader.py              [1,600+ lines] — Paper trading harness
├── live_hit_rate_analyzer.py    [2,400+ lines] — Real-time hit rate stats
├── live_dual_analyzer.py        [1,200+ lines] — Both above concurrently
├── tick_recorder.py             [400+ lines]   — Record raw WS ticks
├── historical_backtest.py       [700+ lines]   — Replay recorded ticks
│
├── SETUP.sh                     — Single-file deployment (git+venv+deps+config+run)
├── COMPARE.sh                   — 10-strategy backtest sweep
├── deploy_vps.sh                — Original VPS setup (SETUP.sh supersedes)
├── run_hitrate.sh               — Runner-only (subset of SETUP.sh)
└── install_*_service.sh         — Systemd services for auto-restart
│
├── config.example.json          — Angel One credentials template
├── requirements.txt             — Python deps
└── README.md
```

---

### 🔬 CORE COMPONENT: nse_book_scanner.py

This is THE brain. Contains:

**1. Data classes** (dataclasses with slots for perf):
- `DepthLevel(price, quantity)` — one book level
- `MarketSnapshot` — broker-agnostic snapshot (timestamp, symbol, ltp, ltq, volume_traded, total_buy_qty, total_sell_qty, best_bid/ask + qty, bids[5], asks[5], spread, mid_price)
- `RegimeState(trend, volatility, label, is_confident)` — Phase 2 regime
- `BookMetrics` — all 17 computed features per tick
- `SignalResult(timestamp, symbol, state, raw_score, smoothed_score, evidence_strength, reasons, diagnostics, metrics)`
- `PendingPrediction` — for horizon-based evaluation

**2. Enums:**
- `SignalState`: STRONG_LONG, LONG, WEAK_LONG, NEUTRAL, WEAK_SHORT, SHORT, STRONG_SHORT, SUPPRESSED
- `AggressorSide`: BUYER, SELLER, NA
- `SessionPhase`: PRE_OPEN_ENTRY, PRE_OPEN_MATCH, OPENING, MORNING, LUNCH, AFTERNOON, PRE_CLOSE, CLOSING, POST_CLOSE, CLOSED, WEEKEND, HOLIDAY

**3. `BookDynamicsEngine`** — main engine with 17 microstructure features:
- L1 imbalance (best bid vs ask qty)
- Top-5 imbalance
- Weighted depth imbalance (distance-decay)
- Book-wide imbalance (all exchange-broadcast qty)
- Imbalance ROC (5s rate of change)
- Liquidity flow (adds vs removes)
- Aggressor ratio (tick-rule 5s)
- Mid-price ROC (5s)
- Buyer/seller aggression volume
- Cancel suspicion (bid/ask)
- Spoofing suspicion (pull events → no execution)
- Iceberg detection (refill after execution)
- Replenishment score
- Spread normalized (bps)
- Kill switch (spread widening > 3x baseline)
- Interval volume
- Book activity

**Score computation (critical - empirically calibrated):**
```python
# Each feature normalized to [-1, +1]
# Weighted average (weights sum to 12.5): 
#   w_l1=1.0, w_top5=1.5, w_weighted_depth=2.0, w_book_wide=1.0
#   w_imbalance_roc=2.5 (highest — leading indicator)
#   w_liquidity_flow=1.5, w_aggressor=2.0, w_mid_response=1.5
raw_norm = weighted_average(features)   # [-1, +1]
adjusted = raw_norm * (1 - spoof_dampener * spoof_suspicion)
raw_score = clamp(adjusted * 10, -10, 10)
smoothed = ema(raw_score, alpha=0.3)     # EMA smoothing critical
```

**CRITICAL CALIBRATED THRESHOLDS** (based on 67k live signals empirical study):
```python
threshold_strong = 4.0   # NOT 8.0! Score never reaches 8 in real market
threshold_normal = 3.0   # NOT 5.0!
threshold_weak   = 2.0
```

**Why:** In real NSE data, smoothed_score practically caps around ±5.0 due to (1) 8-feature weighted average dilution, (2) EMA α=0.3 smoothing. Setting threshold_strong=8 means STRONG_LONG NEVER fires. This is the #1 bug that MUST be avoided.

**4. State classification (with calibrated thresholds):**
```python
# Set constants for filtering
_LONG_STATES = {"STRONG_LONG", "LONG", "WEAK_LONG"}
_SHORT_STATES = {"STRONG_SHORT", "SHORT", "WEAK_SHORT"}
_ACTIONABLE_STATES = _LONG_STATES | _SHORT_STATES
_STRONG_STATES = {"STRONG_LONG", "STRONG_SHORT"}                     # for --strong-only
_NORMAL_AND_STRONG_STATES = _STRONG_STATES | {"LONG", "SHORT"}        # for --skip-weak
```

**5. Signal Quality Gates** (optional filters):

- **CooldownManager** — whipsaw protection after exits (120s base, 2× for flip, 1.5× post-SL)
- **SessionStateManager** — NSE market phase tracker (IST-fixed timezone). Default tradeable phases: {OPENING (9:15-9:30), MORNING (9:30-11:30), AFTERNOON (13:30-15:00)}. Skip LUNCH, PRE_CLOSE, CLOSING. Enforce 15:15 no-new-entry cutoff.
- **RVOLCalculator** — relative volume vs rolling 20-min average. Bucket size 60s, warmup 5 buckets. Detect session reset via negative cumulative delta.

**6. Phase 2 Regime Detector** — classifies each symbol as (TRENDING_UP/TRENDING_DOWN/RANGING/RANDOM/MEAN_REVERTING) × (LOW/NORMAL/HIGH volatility). Uses simple moving averages + volatility clustering. Emits `RegimeState.is_confident=True` only after 60s+ of ticks.

**7. Angel One integration:**
- `AngelOneWSAdapter.parse(msg, symbol)` — parses SmartWebSocketV2 raw messages to `MarketSnapshot`. Handles paise→INR conversion (÷100). Extracts from JSON keys: `last_traded_price`, `best_5_buy_data`, `best_5_sell_data`, `volume_trade_for_the_day`, `total_buy_quantity`, `total_sell_quantity`, `exchange_timestamp` (ms).
- `AngelOneConnector` — login (SmartConnect + pyotp TOTP), scrip master download, token resolution, WebSocket subscription (mode 3 = SnapQuote, exchange type 1 = NSE_CM).

**8. PredictionTracker (Phase 1)** — records every actionable signal as pending predictions at each horizon (5s, 15s, 30s, 60s, 120s, 300s). Evaluates on tick when horizon elapsed. Stores hit rate + cost-adjusted edge per (state, horizon).

**9. Kill switch** — auto-suppress signals when spread widens > 3× the 30-second average (protects against fast-market conditions where microstructure breaks down).

---

### 💰 CORE COMPONENT: paper_trader.py

**Class `PaperExecutor`**:
- Calibrated defaults: `entry_score_threshold=4.0`, `entry_min_evidence=30.0` (aligned with engine STRONG threshold)
- Slippage: 10 bps (0.10%) on entry AND exit (round-trip effectively ~20 bps)
- Round-trip cost: 0.06% (Zerodha/Angel One flat + STT + slippage estimate)
- SL default 0.30%, TP default 0.80% (R:R = 2.67:1)
- Max hold: 5 minutes
- **Time stop (Gemini's suggestion)**: Configurable `time_stop_seconds`. If age >= X and favor < min_favor_pct, exit immediately as `time_stop` reason. Defaults 0 (disabled). Recommended 15s @ 0.05% favor.
- Position sizing: `risk_per_trade_pct=0.01` (1% of capital)
- Notional cap: max 20% capital per single trade
- Max concurrent: 5 positions
- Regime adaptive (Phase 2): skip RANDOM regime, invert MEAN_REVERTING signals, halve size in HIGH_VOL, widen threshold in HIGH_VOL, tighten in LOW_VOL
- Optional gates: cooldown, session_manager, rvol_calculator, state_filter (`_STRONG_STATES` etc.)
- Exit reasons: signal_reverse, stop_loss, take_profit, time_stop, max_hold, eod
- Counters: entries_attempted, entries_rejected_slots, entries_rejected_capital, entries_blocked_by_cooldown, entries_blocked_by_session, entries_blocked_by_low_rvol, entries_blocked_by_state_filter, time_stop_exits

**Class `PaperTradingSession`** — wraps PaperExecutor + engine + feed (simulate or live).

**Realistic simulator** — generates plausible NSE-like ticks for testing without market open.

**Live mode** — uses AngelOneConnector for real WebSocket feed.

**CLI:**
```
--feed [simulate|live]         Feed source
--config PATH                  Angel One credentials
--duration-min N               Session length
--capital N                    Starting ₹ (default 100,000)
--entry-score FLOAT            Min |score| (default 4.0)
--entry-evidence FLOAT         Min evidence (default 30.0)
--stop-loss-pct FLOAT          SL (default 0.0030)
--take-profit-pct FLOAT        TP (default 0.0080)
--max-hold-sec FLOAT           Max hold (default 300)
--time-stop-sec FLOAT          Time stop (default 0=off)
--time-stop-min-favor-pct FLT  Min favor at time stop (default 0.0005)
--regime-adaptive              Enable Phase 2
--strong-only                  Only STRONG signals
--skip-weak                    Skip WEAK, keep STRONG+LONG/SHORT
--session-filter               Enable phase filter
--min-rvol FLOAT               Min RVOL (default 0.0=off)
--rvol-window-minutes INT      RVOL window (default 20)
--allowed-phases NAMES         Phases allowed (default OPENING,MORNING,AFTERNOON)
--holidays DATES               Comma YYYY-MM-DD
--no-entry-cutoff HH:MM        Default 15:15
--cooldown-seconds FLOAT       Whipsaw cooldown (default 120)
```

**Report generation:** Comprehensive text report with win rate, PnL ₹/%, profit factor, avg return, max drawdown, per-state performance, per-hour performance, exit reason breakdown, gate stats.

---

### 📊 CORE COMPONENT: live_hit_rate_analyzer.py

**Purpose:** Measure signal accuracy WITHOUT trading. Multi-dimensional bucketed stats.

**Class `HitRateAnalyzer`:**
- Records every actionable signal as pending predictions at each horizon (default: 5, 15, 30, 60, 120, 300 seconds)
- Evaluates on tick — computes directional return, sign-flipped for SHORT
- 5-dimensional stats: `(state, horizon)`, `(state, evidence_bucket)`, `(state, regime)`, `(state, hour_of_day)`, `(state, symbol)`
- `HitRateBucket` fields: count, hits, net_profitable, sum_return, sum_return_sq, sum_net_return, max_win, max_loss
  - `hit_rate` = hits/count (directional > 0)
  - `net_profit_rate` = net_profitable/count (directional > cost) — **RENAMED to "% Above Cost" in report** to avoid confusion
  - `avg_return` = sum/count (before cost)
  - `avg_net_edge` = sum_net/count (after cost) — this is THE bottom line
- Signal dedup: same-state fires within 5s deduped. State transitions bypass dedup.
- Guard: `max_pending_age >= max_horizon + 30s` (auto-adjusts to prevent silent timeout of long-horizon predictions)
- Optional gates: cooldown, session_manager, rvol_calculator, state_filter

**Class `LiveSignalMonitor`** — real-time UI state for open signals:
- Track current price movement per open signal
- MFE/MAE calculation (best/worst point reached, with time-since-signal)
- MFE/MAE time sentinel: -1.0 (not None — preserves dataclass hashability)
- Verdict: "winning" / "losing" / "flat"

**Class `LiveHitRateSession`** — ties WebSocket + Engine + Analyzer:
- Rich UI (5-panel dashboard) with `rich` library
- Headless mode for VPS (`--no-ui`)
- Health monitor (background thread) — warns if 30s+ no ticks or 90%+ parse failures
- **Diagnostic mode (`--diagnose`)** — dumps first 100 raw WS messages to `logs/raw_ws_dump.jsonl` and prints first 5 to console. CRITICAL for first-time deployment to verify Angel One field names.

**CLI:**
```
--config PATH
--duration-hours FLOAT         Default 1.0
--horizons LIST                Comma-sep seconds (default 5,15,30,60,120,300)
--cost-pct FLOAT               Round-trip (default 0.0006)
--symbols LIST                 Subset filter
--min-samples INT              For verdict (default 20)
--dedup-seconds FLOAT          Signal dedup (default 5.0)
--strong-threshold FLOAT       Override engine STRONG (default 4.0)
--normal-threshold FLOAT       Override engine NORMAL (default 3.0)
--weak-threshold FLOAT         Override engine WEAK (default 2.0)
--ema-alpha FLOAT              Override EMA (default 0.3)
--strong-only                  State filter: STRONG only
--skip-weak                    State filter: STRONG+LONG/SHORT
--session-filter               Enable phase filter
--allowed-phases NAMES         Default OPENING,MORNING,AFTERNOON
--holidays DATES
--no-entry-cutoff HH:MM        Default 15:15
--min-rvol FLOAT               RVOL gate
--rvol-window-minutes INT
--rvol-warmup-buckets INT
--rvol-strict-warmup           Block during warmup
--diagnose                     Save raw WS dumps
--dump-count INT               Default 100
--dump-path PATH
--log-path PATH                Predictions JSONL
--report-path PATH             EOD report
--no-ui                        Headless mode
--skip-market-hours-check      Force run outside market hours
```

**EOD Report:** Session summary, data flow quality, signal state distribution, hit rate × state × horizon (with column meanings prefix + calibration note), evidence bucket table, regime table, hour-of-day table, top symbols, gate stats.

**Verdict rules** (per bucket):
- Count < min_samples → "need N more"
- avg_net_edge > +0.05% AND hit_rate > 0.55 → "✓✓ profitable"
- avg_net_edge > 0 → "✓ marginal edge"
- avg_net_edge in [-0.03%, 0] → "⚠ borderline"
- avg_net_edge < -0.03% → "✗✗ noise/loss"

---

### 🔀 live_dual_analyzer.py

Runs `HitRateAnalyzer` + `PaperExecutor` **simultaneously on same WS feed**. Both share:
- Single `CooldownManager` instance (apples-to-apples measurement)
- Single `SessionStateManager` if enabled
- Single `RVOLCalculator` if enabled
- Single set of engines per symbol

**Rich UI:** 5-panel dashboard showing both analyzers' live state side-by-side.

**Combined EOD report:** Both individual reports + a COMBINED VERDICT with 4 outcomes:
1. Both positive → deploy small capital
2. Hit rate good but paper loss → costs eating edge → optimize entry timing
3. Paper profit but hit rate weak → suspicious → longer test needed
4. Both negative → strategy needs rework

---

### 📼 tick_recorder.py + historical_backtest.py

**tick_recorder.py:**
- Subscribes to same WebSocket as analyzer
- Writes each raw message + timestamp to gzipped JSONL
- ~500 MB/day storage
- Auto-rotates by hour or size
- Auto-stops at 15:30 IST (market close)

**historical_backtest.py:**
- Reads gzipped JSONL from `--data-dir`
- Chronological replay through engine + paper trader
- Same CLI flags as paper_trader for exit/filter configs
- Outputs same-format report as paper_trader

---

### 🚀 SETUP.sh (single-file deployment)

The MOST IMPORTANT script for user experience. Non-technical user runs ONE command → everything works.

**6 stages:**

1. **System packages** — apt install git python3 python3-pip python3-venv python3-full tmux. Root check with 5s countdown (not hard fail). SUDO variable handles no-sudo systems.

2. **Repository** — git clone if fresh, git pull if exists. Auto-detect location (script's own dir, or /root/NSE-Equity-Intraday, or $HOME/NSE-Equity-Intraday).

3. **Python venv** — Health-check existing venv (rebuild if broken). Use `python3 -m venv --upgrade-deps` (Python 3.9+ flag).

4. **Python packages** — CRITICAL for Python 3.12: upgrade pip+setuptools+wheel FIRST (3.12 removed distutils). 3-attempt install with escalating fixes:
   - Attempt 1: `pip install -r requirements.txt`
   - Attempt 2: `pip install --force-reinstall smartapi-python pyotp websocket-client`
   - Attempt 3: `pip install logzero pandas numpy 'pyotp>=2.9' 'websocket-client>=1.6'`
   - Verify with `from SmartApi import SmartConnect` (NOT `import smartapi` — case-sensitive!)

5. **Configuration** — Copy config.example.json if missing. Detect placeholders (YOUR_API_KEY_HERE etc.). If placeholders, open nano interactively for user to fill.

6. **Timezone** — Set Asia/Kolkata via timedatectl.

**Then exec analyzer** with pass-through args:
- Default: 15-min diagnostic run (`--diagnose --duration-hours 0.25`)
- `--full`: 6.5-hour production run
- `--duration N`: custom hours
- `--setup-only`: skip launch
- `-- <args>`: pass-through to analyzer

---

### 📊 COMPARE.sh (10-strategy backtest sweep)

Reads recorded ticks, runs 10 strategies sequentially through `historical_backtest.py`, extracts metrics via regex parser, prints comparison table with best-PnL and best-PF highlighted.

**10 Strategies:**
1. Baseline (defaults)
2. STRONG only (`--strong-only`)
3. Ultra STRONG (`--strong-only --entry-score 5.0`)
4. Time stop 15s (`--time-stop-sec 15`)
5. STRONG + Time stop
6. Tight SL (0.20%)
7. Wide SL (0.50%)
8. Quick TP (0.30%)
9. Long hold (10 min)
10. **Gemini Sniper** — all filters combined

---

### 🐛 CRITICAL BUGS TO AVOID (found during dev)

Reproduce these fixes explicitly:

1. **Score threshold=8.0 unreachable** → Set threshold_strong=4.0. Show empirical justification in code comment.

2. **entry_score_threshold=5.0 in paper_trader while engine STRONG=4.0** → Aligned to 4.0. STRONG signals were silently rejected before.

3. **`import smartapi` vs `from SmartApi import SmartConnect`** → Package name is `SmartApi` (case-sensitive). Never use lowercase in imports.

4. **`--` pass-through in SETUP.sh** → Add explicit `--)` case in bash arg parser + EXTRA_ARGS array + exec with expansion.

5. **Signal dedup with state transitions** → State changes bypass dedup (WEAK_LONG → NEUTRAL → WEAK_LONG fires 2 signals in 1s). This is by-design but generates high frequency (~200 signals/sec across 96 symbols).

6. **Duplicate snapshot warnings flooding logs** → Set to DEBUG level not WARNING. Angel One sends redundant timestamps.

7. **Column name "Net Profit %"** → RENAME to "% Above Cost". Original name misleads users into thinking it's dollar profit; actually it's % of trades that beat cost.

8. **MFE/MAE time sentinel = None** breaks dataclass hashing → Use -1.0 instead.

9. **INSTALL_DIR hardcoded to $HOME/nse_scanner** → Auto-detect script's own directory. Support in-place install from cloned repo.

10. **Root user hard fail in deploy_vps.sh** → Warn+countdown+continue. Many VPS providers are root-only.

11. **Python 3.12 removed distutils** → Upgrade pip+setuptools+wheel FIRST in venv creation.

12. **Angel One field names verified from real data** (`last_traded_price` in paise, `best_5_buy_data` array, `volume_trade_for_the_day`, `exchange_timestamp` in ms). Include diagnostic mode to verify.

---

### 🎯 GEMINI'S TRADER MINDSET (must incorporate)

Not just measurement — actual trading practicality:

**1. Alpha Decay Reality:**
- Signal edge is HIGHEST at 30-60s horizon, then fades
- 5s hit rate often BELOW 50% (price already moved before entry)
- 300s+ = signal effect gone

**2. Loss Cutting > Win Rate:**
- Renaissance Medallion: 50.75% win rate → billions/year profit
- Key: cut losers FAST, let winners run
- Time stop = classic alpha-decay killer

**3. The 3 Sniper Filters (all must be implemented):**
- No-Trade Zone: score ≥ threshold_strong (4-5)
- Time Stop: 15-30s + 0.05% min favor
- Volume Filter: RVOL ≥ 3.0

**4. R:R Ratio Focus:**
- Default SL 0.30%, TP 0.80% = 2.67:1 R:R
- Even 40% win rate profitable at 2.5:1+ R:R

---

### 📁 CONFIG FILE FORMAT

`config.example.json`:
```json
{
  "angel_one": {
    "api_key": "YOUR_API_KEY_HERE",
    "client_code": "YOUR_CLIENT_CODE_HERE",
    "pin": "YOUR_4_DIGIT_MPIN",
    "totp_secret": "YOUR_BASE32_TOTP_SECRET"
  },
  "symbols": ["RELIANCE-EQ", "TCS-EQ", ...],
  "scanner": {
    "min_evidence_strength_to_log": 30.0,
    "log_signal_states": ["WEAK_LONG", "LONG", "STRONG_LONG", "WEAK_SHORT", "SHORT", "STRONG_SHORT"],
    "signal_dedup_seconds": 5.0
  },
  "engine": {
    "history_seconds": 15.0,
    "threshold_strong": 4.0,
    "threshold_normal": 3.0,
    "threshold_weak": 2.0
  },
  "logging": {
    "level": "INFO",
    "log_dir": "logs"
  }
}
```

---

### 🧪 TESTING STRATEGY

Before releasing each file, verify:

1. **Engine self-test** (`--demo` flag) — 8 built-in scenarios: strong bull, strong bear, spoof detected, iceberg detected, kill switch triggered, neutral, duplicate ts (dropped), out-of-order (dropped).

2. **Unit tests** for each utility class:
   - CooldownManager: 6 scenarios (fresh entry, same-side block, flip block, post-SL block, allow after cooldown, stats)
   - SessionStateManager: 11 phases + weekend + holiday + tradeable check + cutoff
   - RVOLCalculator: warmup, steady=1.0, surge=3-9×, session reset, anomaly cap
   - Time Stop: 4 scenarios (disabled, triggered on flat, skipped on favor, SHORT direction)

3. **Integration tests:**
   - HitRateAnalyzer with all 4 gates (cooldown + session + rvol + state_filter)
   - PaperExecutor with all gates + time stop
   - DualAnalyzerSession identity-shared gates

4. **CLI smoke test:** `--help` on each entry point returns 0 exit code.

5. **Full-day simulation:** `paper_trader --feed simulate --duration-min 30` should complete without exception.

---

### 📝 DEVELOPMENT PROCESS

Build files in this order (each fully working before next):

1. **nse_book_scanner.py** first — includes BookDynamicsEngine, all utility classes, `--demo` self-test. Get score computation calibrated correctly (max ~5, not 10) BEFORE building anything on top.

2. **paper_trader.py** — realistic simulator + PaperExecutor. Verify simulate mode gives sensible-looking trades before live mode.

3. **tick_recorder.py** — simple JSONL writer, minimal deps.

4. **historical_backtest.py** — ties reader + engine + paper trader.

5. **live_hit_rate_analyzer.py** — biggest UI-heavy component. Rich dashboard + horizon evaluation + verdict engine.

6. **live_dual_analyzer.py** — imports both hit_rate and paper_trader, wires shared gates.

7. **SETUP.sh, COMPARE.sh, run_hitrate.sh** — deployment layer.

8. **install_*_service.sh** — production systemd services.

**After each file:**
- Syntax check (`python3 -c "import ast; ast.parse(open(f).read())"`)
- Import test (`python3 -c "import <module>"`)
- CLI help exit 0
- Commit with detailed message explaining WHY not just WHAT

---

### 🎨 CODE STYLE

- Bilingual comments (Hindi/English) for critical decisions
- Verbose logging with clear emoji indicators (▶ step, ✓ ok, ⚠ warn, ✗ error, → info)
- Rich terminal UI (colored, boxed panels)
- All ₹ symbols in reports
- IST time throughout (fixed timezone, not system TZ)
- Extensive CLI --help text with examples
- Every threshold, weight, timeout is a config-file OR CLI-overridable parameter — NEVER hardcoded magic numbers

---

### ⚠️ WHAT NOT TO DO

- ❌ Don't add complex ML/AI models (LSTM, transformers). Keep math simple + interpretable.
- ❌ Don't hardcode Zerodha or Fyers or any other broker — Angel One only.
- ❌ Don't build a web dashboard. Terminal + JSONL logs sufficient.
- ❌ Don't add Slack/Telegram alerts. Keep it standalone.
- ❌ Don't add cryptocurrency support. NSE Equity only.
- ❌ Don't add options/futures support in Phase 1. Cash equity only.
- ❌ Don't skip the `--demo` self-test in scanner. Empirical validation matters.
- ❌ Don't set threshold_strong = 8. This is the #1 mistake.
- ❌ Don't hardcode /root/nse_scanner. Auto-detect location.
- ❌ Don't use `import smartapi`. Package is `SmartApi` (case-sensitive).

---

### 🎬 DEPLOYMENT WORKFLOW (final user experience)

User's actual workflow after receiving system:

```bash
# 1. Fresh VPS (Ubuntu 22.04+/24.04, 1-core/2GB minimum)
ssh root@vps-ip

# 2. Single command bootstrap
curl -sO https://raw.githubusercontent.com/<owner>/<repo>/main/SETUP.sh
bash SETUP.sh

# 3. Script auto-installs, opens nano for credentials, then runs 15-min diagnostic

# 4. Verify parse rate 100% in report:
cat logs/hit_rate_report.txt | head -30

# 5. If good, run full day:
bash SETUP.sh --full -- --strong-only --time-stop-sec 15 --min-rvol 2.0

# 6. Or 10-strategy sweep after recording:
python3 tick_recorder.py --config config.json --output-dir /root/nse_data
# ... wait for market close ...
bash COMPARE.sh
```

---

### 💬 COMMUNICATION STYLE WITH USER

When talking to user:
- Use Hindi/Hinglish, especially for explanations
- Include tables for data comparison
- Use emoji for visual navigation (⭐ important, ✅ done, ❌ bug, 🎯 goal, 🚨 critical)
- Be honest about limitations (SEBI 90%, cost eats edge)
- Don't overpromise. Say "signals may work OR may not — this is what we're measuring"
- When user catches a bug, acknowledge it directly. Don't be defensive.
- When user challenges assumption (like "50% is coin flip"), engage with actual math, not hand-wave.

---

### 🏁 SUCCESS CRITERIA

The system is "done" when:
1. User can run `bash SETUP.sh` on fresh VPS and get to first diagnostic within 5 minutes.
2. Diagnostic shows 100% Angel One parse rate.
3. Full-day run produces `logs/hit_rate_report.txt` with all state × horizon buckets populated.
4. STRONG_LONG and STRONG_SHORT signals DO fire (with calibrated thresholds).
5. Paper trader executes trades on STRONG signals.
6. Report gives HONEST verdict (usually: "noise/loss due to cost" — this is CORRECT reality).
7. User can filter via `--strong-only --time-stop-sec 15 --min-rvol 3.0` and see difference in verdict.
8. All 5 files pass syntax check + engine `--demo` passes + all CLI `--help` returns 0.
9. `COMPARE.sh` runs 10 strategies and prints comparison table.

---

### 🙏 FINAL NOTE

This user has already gone through 30+ commits, discovered:
- Score calibration bug (threshold=8 unreachable)
- Paper trader stale defaults bug
- Column misnomer confusion
- Import case-sensitivity issue
- venv location bug
- Multi-server latency tradeoffs

Don't repeat these mistakes. Read commit messages in the reference repo (baldaurathore92-svg/NSE-Equity-Intraday) if unclear on any decision.

The user's SHARP empirical thinking (finding score-cap by looking at 67k signals) has been the main quality driver. Respect that intelligence. Be honest, empirical, and thorough.

**Start by asking:**
1. Do you understand the SEBI 90% context?
2. Do you understand alpha decay?
3. Do you understand cost-eating-edge reality?
4. Which file do you want me to build first?

Then proceed file-by-file, committing each with detailed rationale.

---

**END OF PROMPT**
