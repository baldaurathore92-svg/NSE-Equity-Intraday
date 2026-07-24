# NSE Live Hit-Rate Analyzer — Complete Recreation Prompt

Copy the entire block below into a new Claude session to rebuild the
whole project from scratch. Every architectural decision, every design
trade-off, and every bug that was found and fixed is documented so the
rebuild doesn't repeat the same journey.

---

## PROMPT START — paste everything below this line

You are rebuilding an NSE intraday order-book scanner + hit-rate analyzer
that runs on a low-resource VPS (1 vCPU / 2 GB RAM) against live Angel
One SmartAPI data. Follow these constraints and design decisions exactly.

### Goal

Two self-contained Python files that together let a trader:

1. Watch live order-book signals fire in real time (**scanner**)
2. Measure how often those signals actually predict price direction over
   multiple time horizons — no orders placed, virtual measurement only
   (**hit-rate analyzer**)

Both files must be independently runnable. No cross-imports between them.

### Files to create

```
NSE-Equity-Intraday/
├── live_hit_rate_analyzer.py   # ~7000 lines, full measurement pipeline
├── nse_book_scanner.py         # ~4100 lines, scanner-only extract
├── SETUP.sh                    # unified installer + runner + systemd
├── config.example.json         # Angel One creds + symbol list + engine tuning
├── requirements.txt            # smartapi-python, pyotp, rich, requests
├── README.md                   # quickstart guide
└── .gitignore                  # exclude logs/, venv/, config.json, __pycache__
```

Do NOT create these (they exist in an earlier design and were consolidated):
- paper_trader.py, live_dual_analyzer.py, tick_recorder.py, historical_backtest.py
- COMPARE.sh, deploy_vps.sh, install_service.sh, install_recorder_service.sh
- run_hitrate.sh, install_hitrate_service.sh

### Runtime environment (VPS constraints)

- Ubuntu/Debian VPS with 1 CPU, 2 GB RAM
- Python 3.9+, virtualenv-based install
- ~100 symbols streamed via one SmartWebSocketV2 connection
- Signal computation must stay < 100 ms per tick per symbol
- Memory budget < 1 GB across all engines + analyzer + logs

### Broker specifics (Angel One SmartAPI)

- Login: `SmartConnect` with api_key + client_code + pin + TOTP (pyotp)
- Scrip master: fetch from
  `https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json`
  and cache locally (24 hour TTL). Format: symbol string like `RELIANCE-EQ`
- WebSocket: `SmartWebSocketV2`, exchange type 1 (NSE Cash),
  subscription mode 3 (SNAP_QUOTE = full top-5 depth + LTP)
- Prices arrive as integer paise (multiply by 0.01 for INR floats)
- Every payload carries: LTP, LTQ, cumulative volume_traded,
  total_buy_qty, total_sell_qty, top-5 buy/sell depth levels,
  exchange_timestamp, sequence_number

## Architecture

### Data flow

```
Angel One WS  →  AngelOneWSAdapter.parse()  →  MarketSnapshot
                                              ↓
                    BookDynamicsEngine.update(snap) → SignalResult (carries BookMetrics)
                    (one engine per symbol)          ↓
                                                    ┌────────────────┬─────────────────┐
                                                    ↓                ↓                 ↓
                                            Scanner: print   HitRateAnalyzer   LiveSignalMonitor
                                            (nse_book_       (record + evaluate  (per-signal
                                             scanner.py)      at horizons)         P&L view)
```

### Core data classes (in this exact order in the file)

**SignalState** (Enum):
```
STRONG_LONG, LONG, WEAK_LONG, NEUTRAL, WEAK_SHORT, SHORT, STRONG_SHORT, SUPPRESSED
```

**AggressorSide** (Enum): `BUYER`, `SELLER`, `NA`

**DepthLevel** (@dataclass): `price: float`, `quantity: int`. Enforce
`__post_init__` type coercion from broker payloads.

**MarketSnapshot** (@dataclass):
```
timestamp: float
symbol: str
ltp: float
ltq: int
volume_traded: int
total_buy_qty: int
total_sell_qty: int
bids: List[DepthLevel]   # top-5, best-first (highest bid at index 0)
asks: List[DepthLevel]   # top-5, best-first (lowest ask at index 0)
sequence_number: Optional[int] = None
exchange_timestamp: Optional[float] = None
received_timestamp: Optional[float] = None
```
Provide `best_bid`, `best_ask`, `mid_price`, `spread` properties.

**ExecutionCostModel** (@dataclass frozen=True): the single source of
truth for "what would this signal actually fill at?" LONG entry crosses
ASK, LONG exit lifts BID; SHORT reverse. Spread is captured directly by
using bid/ask executable quotes — `transaction_cost_pct` represents ONLY
explicit charges (STT/GST/brokerage, default 0.0006 = 0.06% round-trip),
NOT spread. Optional `latency_slippage_bps` for stress testing.

Method contract:
- `fill_price(side, is_entry, ltp, best_bid, best_ask) -> float`
- `gross_directional_return(side, entry_price, exit_price) -> float`
  (sign-flipped for SHORT so positive = correct direction)
- `charge_return(entry, exit) -> float` (round-trip charges)
- `evaluate(side, entry, exit) -> (gross, charges, net)`

**RegimeState** (@dataclass):
```
volatility: str = "NORMAL"        # LOW / NORMAL / HIGH
trend: str = "RANDOM"             # TRENDING_UP/DOWN, MEAN_REVERTING, RANDOM
depth_bias: str = "BALANCED"      # BULL_STRUCTURAL/BEAR_STRUCTURAL/BALANCED
volatility_ratio: float = 1.0     # spread CoV in new design
autocorr_lag1: float = 0.0        # deprecated, kept for JSONL schema
depth_imbalance_mean: float = 0.0
is_confident: bool = False
```
Methods: `label` (e.g. `"N·T↑·B"`), `is_tradeable()`,
`should_invert_signal()` (True in confirmed MEAN_REVERTING).

**RegimeDetector**: FULLY TIME-BASED, order-book only. See "Design
lessons" section below for rationale — do NOT use tick-count deques or
lag-1 autocorrelation of tick returns. Use TimeSeriesBuffer for every
feature, fixed wall-clock windows (default 5.0 s), 30 s warm-up,
recompute at 250 ms cadence.

Update signature (order-book features ONLY, no LTP returns):
```python
update(ts, l1_imbalance, book_wide_imbalance,
       bid_added, ask_added, bid_removed, ask_removed,
       spread_bps, mid_price) -> RegimeState
```

Classification logic:
- TRENDING_UP/DOWN: mid-price direction AND book imbalance agree AND
  |book_mean| >= depth_bias_thr (default 0.15)
- MEAN_REVERTING: strong-magnitude book imbalance (mean(|book|) >=
  depth_bias_thr) AND (high sign-flip rate OR low persistence)
- RANDOM: everything else (includes small noise around 0)
- Volatility via spread coefficient of variation (stdev / mean of
  spread_bps buffer)

**BookMetrics** (@dataclass): output of BookDynamicsEngine, carries
20+ derived fields. Key fields:

```
# Imbalances [-1, +1], positive = bullish
book_wide_imbalance, l1_imbalance, top5_imbalance, weighted_depth_imbalance

# ROC over 5s / 10s
buy_book_roc_5s, sell_book_roc_5s, imbalance_roc_5s

# Liquidity flow (integer quantities)
buy_added, buy_removed, sell_added, sell_removed
book_activity  # total added+removed

# Aggressor + mid response
buyer_aggressor_ratio_5s: float = 0.5   # baseline 0.5, [0, 1]
mid_price_roc_5s: float = 0.0
ltp_roc_5s: float = 0.0
interval_volume: int = 0

# Spread + book integrity
spread, mid_price, ltp
normalized_spread_bps
spoofing_suspicion: float = 0.0
cancellation_suspicion_bid, cancellation_suspicion_ask
execution_likelihood_bid, execution_likelihood_ask
iceberg_suspicion: float = 0.0
iceberg_side: str = ""   # "bid", "ask", or ""  (new — was dead code before)
replenishment_score: float = 0.0

# Regime (RegimeState instance) + kill switch
regime: RegimeState = ...
kill_switch_active: bool = False
kill_switch_reason: str = ""
```

**SignalResult**: what BookDynamicsEngine.update() returns
```
timestamp: float
symbol: str
state: SignalState
raw_score: float               # [-10, +10] before EMA
smoothed_score: float          # EMA-smoothed, [-10, +10]
evidence_strength: float       # 0..100 (NOT a probability)
reasons: List[str]             # human-readable
diagnostics: Dict[str, Any]
metrics: BookMetrics
```

### EngineConfig — every tunable in one dataclass

```python
@dataclass
class EngineConfig:
    history_seconds: float = 60.0
    depth_decay_frac: float = 0.005    # normalized (0.5% of mid = 50 bps)

    # Feature weights (all tunable via config + CLI)
    w_l1_imbalance:        float = 1.0
    w_top5_imbalance:      float = 1.5
    w_weighted_depth:      float = 2.0
    w_book_wide_imbalance: float = 1.0
    w_imbalance_roc:       float = 2.5   # leading indicator, highest weight
    w_liquidity_flow:      float = 1.5
    w_aggressor_ratio:     float = 0.0   # ⚠ Lee-Ready — see Design lesson #6
    w_mid_response:        float = 1.5
    w_iceberg:             float = 0.0   # default 0; set 1.0 to enable

    # Spoof dampener
    spoof_dampener_strength: float = 0.5
    spoof_pull_threshold_pct: float = 0.4
    spoof_pull_window_s: float = 1.5
    spoof_max_delta_qty: int = 0   # 0 = disabled; 10000 for Nifty50

    # Aggressor volume window (was hardcoded 5s, now tunable)
    aggressor_window_s: float = 5.0

    # Iceberg detection
    iceberg_price_hold_bps: float = 5.0   # widen to 10.0 for fast sweeps

    # EMA
    ema_alpha: float = 0.3
    ema_warmup_ticks: int = 50   # suppress signals until EMA converges

    # State thresholds (empirically calibrated on 67k live signals)
    threshold_strong: float = 4.0
    threshold_normal: float = 3.0
    threshold_weak:   float = 2.0

    # Regime gate (opt-in)
    regime_gate_enabled: bool = False
    regime_invert_mean_reverting: bool = False

    # Kill switch (spread widening)
    kill_switch_spread_multiplier: float = 3.0
    kill_switch_spread_lookback_s: float = 30.0

    min_qty_floor: int = 1000    # denominator floor for ROC math
```

### BookDynamicsEngine core loop

For each MarketSnapshot:
1. Dedup by sequence_number or (exchange_timestamp, LTP-tuple) hash;
   drop out-of-order events.
2. Compute BookMetrics (all 20+ fields).
3. Compute weighted composite score:
   ```
   f_l1 = clamp(l1_imbalance, -1, 1)
   f_bw = clamp(book_wide_imbalance, -1, 1)
   f_iroc = tanh_scale(imbalance_roc_5s, k=5.0)
   f_flow = tanh_scale(net_flow / max(book_activity, min_qty_floor), k=1.5)
   f_agg = clamp((buyer_aggressor_ratio_5s - 0.5) * 2.0, -1, 1)
   f_mid = tanh_scale(mid_price_roc_5s * 100, k=1.0)
   f_ice = +iceberg_suspicion if iceberg_side=="bid" else -iceberg_suspicion
                                                     else 0.0
   raw_score = clamp(weighted_avg(features) * 10.0, -10, 10)
   ```
4. Apply spoof dampener: `adjusted = raw * (1 - dampener_strength * spoof_susp)`
5. Apply regime gate (opt-in):
   - If `regime_invert_mean_reverting` and regime should invert:
     `smoothed = -smoothed`
   - If `regime_gate_enabled` and NOT `regime.is_tradeable()`:
     `smoothed = 0` (forces state to NEUTRAL)
6. EMA smoothing:
   ```
   if _ema_score is None: _ema_score = 0.0   # NOT raw_score
   _ema_score = alpha * raw_score + (1-alpha) * _ema_score
   ```
7. Apply warm-up gate: if `_warmup_ticks < ema_warmup_ticks`, force
   state = NEUTRAL regardless of score.
8. Map score to state via threshold table.

### Composite feature list (in this order):

```python
weighted = [
    (cfg.w_l1_imbalance,        f_l1,   "L1"),
    (cfg.w_top5_imbalance,      f_t5,   "Top5"),
    (cfg.w_weighted_depth,      f_wd,   "WeightedDepth"),
    (cfg.w_book_wide_imbalance, f_bw,   "BookWide"),
    (cfg.w_imbalance_roc,       f_iroc, "ImbalanceROC5s"),
    (cfg.w_liquidity_flow,      f_flow, "LiqFlow"),
    (cfg.w_aggressor_ratio,     f_agg,  "Aggressor5s"),   # weight 0 by default
    (cfg.w_mid_response,        f_mid,  "MidROC5s"),
    (cfg.w_iceberg,             f_ice,  "Iceberg"),        # weight 0 by default
]
```

### HitRateAnalyzer

Records each actionable signal, evaluates at 6 default horizons
(5, 15, 30, 60, 120, 300 seconds). Uses `ExecutionCostModel` for
executable entry/exit prices with bid/ask crossing.

Buckets:
- `_stats_state_horizon`: primary bucket (executable bid/ask)
- `_stats_state_horizon_ltp`: parallel diagnostic bucket (LTP-only,
  for comparison with pre-P0 reports)
- `_stats_evidence`, `_stats_regime`, `_stats_hour`, `_stats_symbol`

Gates (all opt-in):
- Signal state filter (default all actionable; `--strong-only`,
  `--skip-weak`)
- Cooldown manager (whipsaw protection)
- Session phase gate (skip LUNCH, PRE_OPEN, CLOSING)
- RVOL gate (relative volume threshold)
- Entry confirmation ("15-second sniper" — score must persist N sec)
- Survival exit (close signal if MFE < threshold at N sec)
- Contrarian mode (`invert_signals`) — LONG↔SHORT swap at record time

Dedup: same (symbol, state) fires within `signal_dedup_seconds`
(default 5.0) are dropped.

Timeout: pending predictions past `max_pending_age_s` (default 600.0)
get timed-out evaluation.

JSONL audit log at `logs/hit_rate_predictions.jsonl` with per-signal:
```
ts_fired, ts_evaluated, target_horizon_s, actual_horizon_s,
symbol, state, score, evidence, evidence_bucket, regime, hour,
price_at_signal, ltp_at_signal, bid_at_signal, ask_at_signal,
bid_qty_at_signal, ask_qty_at_signal, spread_bps_at_signal,
price_at_horizon, ltp_at_horizon, bid_at_horizon, ask_at_horizon,
bid_qty_at_horizon, ask_qty_at_horizon,
raw_return_pct, directional_return_pct, charges_pct, net_return_pct
```

### CLI structure (both files)

Common flags:
```
--config CONFIG                config.json path
--duration-hours FLOAT         session max duration (default 1.0)
--symbols A,B,C                subset filter
--engine-demo                  8-scenario self-test, no config/network
--stale-feed-sec 90.0          exit code 75 after N seconds silence (systemd)
--no-ui                        headless mode
--skip-market-hours-check      allow after-hours testing
--diagnose --dump-count 100    save first N raw WS messages

# Signal filtering
--strong-only                  STRONG_LONG + STRONG_SHORT only
--skip-weak                    skip WEAK signals

# Score threshold overrides
--strong-threshold 4.0
--normal-threshold 3.0
--weak-threshold   2.0
--ema-alpha 0.3

# Feature weight overrides
--w-book-wide FLOAT            book-wide imbalance weight
--w-aggressor FLOAT            Lee-Ready aggressor weight (default 0.0)
--w-iceberg   FLOAT            iceberg feature weight (default 0.0)

# EMA warmup + regime gate + spoof + kill switch
--ema-warmup-ticks 50
--regime-gate                  drop signals in RANDOM regime
--regime-invert                flip LONG↔SHORT in MEAN_REVERTING
--spoof-dampener-strength 0.5
--spoof-max-delta-qty 10000    protect large-cap block trades (0 = OFF)
--kill-switch-spread-mult 3.0
--iceberg-hold-bps 5.0
--aggressor-window-sec 5.0

# 15-second sniper policy (OPT-IN, defaults 0)
--entry-confirmation-sec 15    signal must persist 15s
--entry-score 4.0              min score during confirmation
--entry-evidence 30.0
--survival-check-sec 15        one-shot MFE check at 15s
--survival-min-favor-pct 0.0001

# Session + RVOL + cooldown gates (analyzer only)
--session-filter
--allowed-phases OPENING,MORNING,AFTERNOON
--holidays 2026-08-15,2026-10-02
--no-entry-cutoff 15:15
--min-rvol 0.0
--rvol-window-minutes 20
--rvol-strict-warmup

# Analyzer-only
--horizons 5,15,30,60,120,300
--cost-pct 0.0006
--latency-slippage-bps 0.0
--min-samples 20
--dedup-seconds 5.0
--log-path logs/hit_rate_predictions.jsonl
--report-path logs/hit_rate_report.txt
--invert-signals               EXPERIMENTAL contrarian mode

# Post-hoc audit (no config/network needed)
--verify-horizons PATH         per-signal 5s vs 300s breakdown
--verify-limit 20
```

## Design lessons (do not repeat these bugs)

### Lesson 1: LONG/SHORT executable price contract

- LONG entry crosses ASK, LONG exit hits BID (crosses spread twice)
- SHORT entry hits BID, SHORT exit crosses ASK
- Return sign is flipped for SHORT so positive = correct direction
  regardless of side
- Transaction cost is charges ONLY (STT/GST/brokerage). Spread is
  captured automatically by the bid/ask crossing.

### Lesson 2: 5s hit-rate = 0%, 300s hit-rate = 100% is legitimate

Not a bug. Each of the 6 horizons is an INDEPENDENT evaluation of the
SAME signal:
- At 5s, spread cost (~10 bps) dominates the tiny early price move
- At 300s, price has had time to develop past the spread
Report must clearly explain this or the trader will (rightly) suspect
bug. Add prominent interpretation notes to state × horizon table.

### Lesson 3: Sample-size warning

Rows with `count < min_samples_for_verdict` (default 20) must be
prefixed with `⚠` in the report. A 100% hit rate from N=2 is noise,
not signal.

### Lesson 4: EMA cold-start trap (9:15 AM losses)

Old code:
```python
if self._ema_score is None:
    self._ema_score = raw_score  # ✗ first noise tick pins EMA for 2-3 min
```

Fix:
```python
if self._ema_score is None:
    self._ema_score = 0.0        # ✓ neutral seed
# + warm-up counter that forces NEUTRAL for the first N ticks
```

Default `ema_warmup_ticks = 50` (~10 seconds at 5 tps).

### Lesson 5: Iceberg was dead code

`iceberg_suspicion` used to be computed but never contributed to
composite score. Add `iceberg_side` field ("bid"/"ask"/"") and wire as
a signed feature: bid → LONG contribution, ask → SHORT contribution.
Default weight `w_iceberg = 0.0` (backwards compat), user sets 1.0 to
enable.

### Lesson 6: Lee-Ready tick rule 25-35% wrong on NSE

Tick-rule aggressor classification (LTP > prev_mid → BUYER, LTP <
prev_mid → SELLER) is only ~65-75% accurate on NSE cash. The error
rate propagates directly into composite score sign flips.

Fix: `w_aggressor_ratio = 0.0` by default. The metric is still
computed and stored in BookMetrics for diagnostics/audit, but does
NOT contribute to composite score, regime detection, or any decision
logic. Users can opt back in via `--w-aggressor 2.0` if desired.

### Lesson 7: RegimeDetector must be TIME-based, not tick-count

Old design used `Deque(maxlen=500)` for recent, `Deque(maxlen=5000)`
for baseline. This has a severe scale mismatch:
- 500 ticks at market open (50 tps) = 10 seconds
- 500 ticks at lunch (1 tps) = 8+ minutes

Fix: use `TimeSeriesBuffer` for every feature, fixed wall-clock
windows.

### Lesson 8: NO lag-1 autocorrelation of tick returns

On NSE cash, bid-ask bounce produces spurious strongly-negative
autocorrelation (-0.20 to -0.50) even in obviously trending markets.
That signal was a mathematical artifact, not a trading edge.

Fix: RegimeDetector uses only ORDER-BOOK features (imbalance,
add/remove rates, spread CoV, mid-price direction). No LTP return
sequences, no autocorrelation.

### Lesson 9: Book-wide imbalance can be spoofed

Deep-book aggregate quantities (levels 5-10) are the easiest to
manipulate. Default weight 1.0 is fine, but expose it as
`--w-book-wide` so users can zero it out per symbol/regime.

### Lesson 10: Spoof detection kills legit block trades

40% pull threshold flags 25k-share institutional executions on
Reliance/HDFC Bank as spoofs, dampening real signals. Add
`spoof_max_delta_qty` config (default 0 = OFF). Above this absolute
share threshold, treat as execution not spoof. Recommended value
10000 for Nifty50 large caps, 0 for small/mid caps.

### Lesson 11: Aggressor window should be tunable

The 5-second buy/sell volume aggregation window was hardcoded. In
slow afternoon markets an old institutional print can poison the
ratio for 5 seconds. Expose `aggressor_window_s` (default 5.0), users
shorten to 2.0 for slow markets.

### Lesson 12: Stale-feed watchdog for systemd

If the WebSocket stops delivering ticks during NSE market hours but
the process doesn't crash, systemd never restarts it. Fix: exit with
code 75 (EX_TEMPFAIL) if no valid tick arrives for N seconds during
market hours. Default `--stale-feed-sec 90.0`. systemd unit's
`Restart=always` will restart the service.

### Lesson 13: Sniper policy defaults must be OPT-IN

Old defaults `entry-confirmation-sec=15` and `survival-check-sec=15`
silently blocked ALL signals in the observability build. Change to
default 0 (OFF). systemd unit passes 15 explicitly for production.

### Lesson 14: Sequence-based dedup

Broker sometimes resends stale packets. Dedup by:
1. `sequence_number` if broker provides it (drop lower seq for same-second)
2. Otherwise `(exchange_timestamp, LTP, bid_qty, ask_qty)` hash
3. Never accept an event with `exchange_timestamp < last_accepted_exchange_ts`

### Lesson 15: `--verify-horizons` diagnostic

Traders will (rightly) doubt aggregate numbers. Add a post-hoc audit
mode that reads `logs/hit_rate_predictions.jsonl` and prints, for each
recorded signal, its 6 horizon evaluations side-by-side. Rank by
5s-vs-300s divergence so the "suspicious" rows come first. Also print
an aggregate DIRECTIONAL SANITY CHECK: LONG hit% vs SHORT hit% per
horizon. If both fall below 50% at every 60s+ horizon, fire a
DIAGNOSIS block suggesting operator trap / wrong regime / contrarian
mode.

### Lesson 16: Warmup gate makes regime confidence time-based

Old confidence check: `len(baseline_returns) >= 500`. Broken by same
tick-count scale-mismatch. New: `is_confident` = at least
`warmup_seconds` elapsed AND >= 20 samples in buffer.

## SETUP.sh contract

One file handles all lifecycle actions:
```
bash SETUP.sh                          # install + 15-min diagnostic
bash SETUP.sh --full                   # install + 6.5-hour full session
bash SETUP.sh --duration N             # install + N-hour custom
bash SETUP.sh --run                    # skip install, launch analyzer
bash SETUP.sh --setup-only             # install only
bash SETUP.sh --engine-demo            # 8-scenario self-test

bash SETUP.sh --install-service        # register systemd auto-start
bash SETUP.sh --service-status         # systemctl status
bash SETUP.sh --service-logs           # journalctl -f
bash SETUP.sh --service-start
bash SETUP.sh --service-stop
bash SETUP.sh --uninstall-service

bash SETUP.sh --full -- --strong-only --entry-confirmation-sec 15
     # passes everything after -- straight to the analyzer
```

Systemd unit's `ExecStart` should include as the baseline production
hardening:
```
--strong-only --entry-confirmation-sec 15 --entry-score 4.0
--entry-evidence 30 --survival-check-sec 15 --survival-min-favor-pct 0.0001
--ema-warmup-ticks 50 --stale-feed-sec 90 --no-ui --duration-hours 6.5
```

`Restart=always`, `RestartSec=60`, `MemoryMax=1G`, `LimitNOFILE=65536`.

## config.example.json structure

```json
{
  "angel_one": {
    "api_key": "YOUR_API_KEY",
    "client_code": "YOUR_CLIENT_CODE",
    "pin": "YOUR_4_DIGIT_MPIN",
    "totp_secret": "YOUR_BASE32_TOTP_SECRET"
  },
  "symbols": [ "RELIANCE-EQ", "TCS-EQ", "..." ],
  "scanner": {
    "min_evidence_strength_to_log": 30,
    "signal_dedup_seconds": 5.0,
    "signal_log_path": "logs/signals.jsonl",
    "system_log_path": "logs/scanner.log",
    "scrip_master_cache_path": "logs/scrip_master.json",
    "scrip_master_ttl_hours": 24,
    "prediction_horizons_s": [30.0, 60.0, 120.0],
    "transaction_cost_pct": 0.0006,
    "prediction_min_samples_for_verdict": 20
  },
  "engine": {
    "history_seconds": 60,
    "depth_decay_frac": 0.005,
    "ema_alpha": 0.3,
    "threshold_strong": 4.0,
    "threshold_normal": 3.0,
    "threshold_weak": 2.0,
    "w_l1_imbalance": 1.0,
    "w_top5_imbalance": 1.5,
    "w_weighted_depth": 2.0,
    "w_book_wide_imbalance": 1.0,
    "w_imbalance_roc": 2.5,
    "w_liquidity_flow": 1.5,
    "w_aggressor_ratio": 0.0,
    "w_mid_response": 1.5,
    "w_iceberg": 0.0,
    "regime_gate_enabled": false,
    "regime_invert_mean_reverting": false,
    "ema_warmup_ticks": 50,
    "spoof_dampener_strength": 0.5,
    "spoof_max_delta_qty": 0,
    "aggressor_window_s": 5.0,
    "iceberg_price_hold_bps": 5.0,
    "kill_switch_spread_multiplier": 3.0
  }
}
```

Load contract: `load_config()` reads all engine-section fields via a
whitelist and type-coerces each field to its EngineConfig type.

## Testing strategy

Every code change must pass all four of:
```bash
python3 -m py_compile live_hit_rate_analyzer.py   # compile check
python3 -m py_compile nse_book_scanner.py

python3 live_hit_rate_analyzer.py --engine-demo    # 8-scenario self-test
python3 nse_book_scanner.py --engine-demo

# After a live run:
python3 live_hit_rate_analyzer.py --verify-horizons \
    logs/hit_rate_predictions.jsonl
```

The 8 engine-demo scenarios must include:
1. Buyer aggression + growing depth
2. Seller aggression + depleting depth
3. Spoof-and-pull pattern (spoof suspicion triggers)
4. Iceberg replenishment (hidden liquidity)
5. Crossed/locked book (kill switch)
6. Blowout spread (kill switch)
7. Neutral book (no signal)
8. Sequence/timestamp dedup contract (a-d1-d2-d3 sub-cases)

## Common regressions to guard against

- Aggregator sum(_values) is NOT time-weighted (older values count equally
  to newer). If precision matters, prefer shorter fixed windows.
- `MIN_QTY_FLOOR = 1000` biases ROC math: small-caps suppressed, large-caps
  amplify noise. Document but don't blindly change — users have calibrated.
- `depth_decay_frac = 0.005` is already normalized by mid; it's a fraction
  (0.5% of mid = 50 bps), not an absolute rupee value. Do NOT change to
  "absolute-tick-based" without verifying the math.
- Direct `git push` fails in some sandbox environments — use the sandbox
  GitHub tool `push_to_remote` instead.
- Both `live_hit_rate_analyzer.py` and `nse_book_scanner.py` carry
  independent copies of the engine after the extraction. Every engine
  bug fix must be applied to BOTH files.
- Two `logger = logging.getLogger(...)` calls exist in
  `live_hit_rate_analyzer.py` (one for engine, one for session-layer).
  Keep both — reassignment is intentional.

## Deliverables

Produce these files, in this order, with the exact contents documented
above:

1. `requirements.txt` — pinned versions of smartapi-python, pyotp, rich,
   requests
2. `.gitignore` — logs/, venv/, config.json, __pycache__, *.pyc,
   *.jsonl, *.egg-info
3. `config.example.json` — creds template + full engine section
4. `live_hit_rate_analyzer.py` — the complete measurement pipeline
5. `nse_book_scanner.py` — signal generator only (extract of #4, no
   hit-rate layer)
6. `SETUP.sh` — unified installer + runner + systemd + service manager
7. `README.md` — 30-line quickstart

After each file, run the compile check and the engine-demo. Do not
proceed to the next file until the previous one passes.

## Post-build sanity check

```bash
# 1. Files exist and compile
python3 -m py_compile live_hit_rate_analyzer.py && echo OK
python3 -m py_compile nse_book_scanner.py && echo OK

# 2. Engine demos pass
python3 live_hit_rate_analyzer.py --engine-demo 2>&1 | tail -3
python3 nse_book_scanner.py --engine-demo 2>&1 | tail -3

# 3. Config parses
python3 -c "from live_hit_rate_analyzer import load_config; \
            print(load_config('config.example.json').symbols[:3])"

# 4. Both files independent — nse_book_scanner MUST NOT import from
#    live_hit_rate_analyzer or vice versa
grep -n "from live_hit_rate_analyzer" nse_book_scanner.py    # should be empty
grep -n "from nse_book_scanner"       live_hit_rate_analyzer.py  # should be empty

# 5. Lee-Ready weight is 0 by default (Lesson 6)
python3 -c "from live_hit_rate_analyzer import EngineConfig; \
            assert EngineConfig().w_aggressor_ratio == 0.0, 'FAIL'"

# 6. RegimeDetector is time-based (Lesson 7)
python3 -c "from live_hit_rate_analyzer import RegimeDetector; \
            rd = RegimeDetector(); \
            assert hasattr(rd, '_mid_price'), 'not time-based'; \
            assert hasattr(rd, '_book_imb'), 'not time-based'"

# 7. All 6 synthetic regime scenarios pass (paste the test block
#    from Lesson 7's classification into a script)
```

If any check fails, fix before proceeding.

## PROMPT END
