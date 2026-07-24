# NSE Equity Intraday — Live Hit-Rate Analyzer

**एक single-file, Hindi-friendly tool जो Angel One की live NSE Cash Market
feed पर scanner के signals का real hit-rate माप कर बताता है — कोई real order
भेजे बिना, कोई capital risk किए बिना।**

> यह prediction accuracy measurement है, trading advice नहीं। ना कोई
> candlestick / RSI / MACD / OHLC — केवल **tick-by-tick order-book
> microstructure** पर 17 metrics से composite score, plus configurable
> "sniper" entry/exit rules। Real edge है या noise — data बताएगा।

---

## 🚀 Quickstart — तीन commands

Fresh Ubuntu 22.04+ VPS पर zero-to-running:

```bash
# 1. Repo clone
git clone https://github.com/baldaurathore92-svg/NSE-Equity-Intraday.git
cd NSE-Equity-Intraday

# 2. सब कुछ install + Angel One credentials भरो (nano खुलेगा) + engine test
bash SETUP.sh --setup-only

# 3. Live analyzer चलाओ — 15-second sniper policy default में लगी हुई
bash SETUP.sh --full -- --strong-only \
    --entry-confirmation-sec 15 \
    --survival-check-sec 15 \
    --survival-min-favor-pct 0.0001
```

**Zero-config credential test (no broker needed):**
```bash
bash SETUP.sh --engine-demo   # 8 synthetic scenarios: engine sanity check
```

**24/7 auto-restart on VPS (systemd):**
```bash
bash SETUP.sh --install-service
bash SETUP.sh --service-start
bash SETUP.sh --service-logs   # live journalctl tail
```

---

## 📁 File Layout — बस 5 files हैं

```
NSE-Equity-Intraday/
├── live_hit_rate_analyzer.py   ← SINGLE Python (~5,700 lines) — पूरा system
├── SETUP.sh                    ← SINGLE shell — install + run + systemd
├── config.example.json         ← credentials template (Nifty 100 symbols)
├── requirements.txt            ← Python deps
└── README.md                   ← यह file
```

Runtime पर आप बस दो files से interact करते हैं — `SETUP.sh` (operator के लिए)
और `config.json` (credentials भरने के लिए)। बाकी सब automatic है।

---

## 🎯 यह tool क्या करता है (और क्या नहीं)

### करता है ✅

- Angel One SmartWebSocketV2 से Level-2 top-5 depth (SnapQuote mode) पर 100
  symbols parallel में subscribe करता है
- हर tick पर 17 microstructure metrics compute करके composite `[-10, +10]`
  score निकालता है → EMA smoothed → State (STRONG_LONG / LONG / WEAK_LONG /
  NEUTRAL / WEAK_SHORT / SHORT / STRONG_SHORT)
- हर actionable signal को multiple horizons (default 5s / 15s / 30s / 60s /
  120s / 300s) पर track करता है — real price movement से hit rate + net edge
- 15-second **sniper policy** enforce करता है:
  - **Entry confirmation:** signal तभी record होगा जब score लगातार 15 seconds
    तक qualify करता रहे (fake spike/flash reject)
  - **Survival exit:** entry के 15s बाद अगर MFE ≥ 0.01% नहीं है, signal तुरंत
    square-off (breakeven exit)
- Session phase gate, RVOL gate, cooldown gate, state filter — सब optional
- Real-time rich UI (live winning/losing verdict) OR headless mode (VPS)
- EOD comprehensive text report + JSONL audit trail

### नहीं करता है ❌

- कोई real order Angel One पर नहीं भेजता — **zero financial risk**
- कोई candlestick / RSI / MACD / OHLC नहीं
- Backtest नहीं करता (record→replay pipeline इस repo में नहीं है)
- कोई paper-trading P&L simulation नहीं (सिर्फ hit-rate measurement)
- कोई ML/AI / LSTM / transformer — पूरा math interpretable है

---

## 🎯 15-Second Sniper Policy (Gemini-style)

बाज़ार में **alpha decay** होता है — जो signal 60 seconds पहले relevant था वो
अब नहीं। साथ ही **fake spikes** से 35% win rate आती है। इन दोनों को defeat
करने के लिए दो हार्ड rules:

### Rule 1: 15-Second Entry Confirmation

Signal fire होते ही तुरंत entry मत लो। पहले देखो — क्या यह score अगले
**15 seconds** तक टिका रहता है?

```
t=0s   : score crosses +4.0 (STRONG_LONG threshold) → START confirmation
t=5s   : score अभी भी ≥ 4.0 → pending qualifying
t=10s  : score अभी भी ≥ 4.0 → pending qualifying
t=15s  : score अभी भी ≥ 4.0 → CONFIRMED → record signal
```

अगर बीच में score threshold से गिर जाए, या direction flip हो, तो pending
cancel + rearm। Same-side re-arming के लिए पहले state को leave zone करना ज़रूरी।

**Flags:**
- `--entry-confirmation-sec 15` (0 से disable)
- `--entry-score 4.0` (calibrated STRONG threshold — 67k live signals पर
  empirically tested; पुराने 8.0 default में STRONG_LONG कभी fire ही नहीं होता)
- `--entry-evidence 30` (feature agreement × |score| × 10)

### Rule 2: 15-Second Survival Exit

Confirmed entry के **15 seconds** के अंदर अगर MFE (Max Favorable Excursion) ≥
`0.01%` नहीं हुआ, तो signal उसी वक्त square-off। "अगर पहले 15s में मूव नहीं
हुई, तो यह fake signal था — cost eat करने से पहले exit।"

```
Entry @ ask 100.05 (LONG STRONG signal)
Survival check @ t=15s:
  - Best MFE during 0-15s window = +0.008%  (< 0.01% threshold)
  → FAIL → close at current bid → policy bucket में record
  - Best MFE during 0-15s window = +0.025%  (≥ 0.01%)
  → PASS → signal continues to max horizon (300s)
```

**Flags:**
- `--survival-check-sec 15` (0 से disable)
- `--survival-min-favor-pct 0.0001` (= 0.01%)

### Two-Table Reporting

EOD report में दो अलग tables:

1. **HIT RATE BY STATE × HORIZON** — diagnostic view: बिना survival rule के
   हर horizon पर क्या होता (5s/15s/30s/60s/120s/300s)
2. **🎯 15-SECOND POLICY OUTCOME** — actual view: policy rules apply करने के बाद
   per-state एक outcome per signal (survival_exit @ 15s OR max_horizon @ 300s)

यह comparison दिखाता है — policy से कितना बेहतर या बदतर हुआ।

---

## 🛠️ `SETUP.sh` — सारे modes

| Command | काम |
|---|---|
| `bash SETUP.sh` | Install (as needed) + 15-min diagnostic run |
| `bash SETUP.sh --full` | Install + 6.5-hour full trading day |
| `bash SETUP.sh --duration N` | Install + N-hour custom run |
| `bash SETUP.sh --setup-only` | सिर्फ install; analyzer मत चलाओ |
| `bash SETUP.sh --run` | Skip install; बस analyzer launch करो |
| `bash SETUP.sh --engine-demo` | 8-scenario engine self-test (no config) |
| `bash SETUP.sh --install-service` | Systemd unit register + enable auto-start |
| `bash SETUP.sh --uninstall-service` | Systemd unit remove |
| `bash SETUP.sh --service-status` | `systemctl status` |
| `bash SETUP.sh --service-logs` | `journalctl -u ... -f` |
| `bash SETUP.sh --service-start` | `systemctl start` |
| `bash SETUP.sh --service-stop` | `systemctl stop` |
| `bash SETUP.sh --help` | पूरी help |

**Pass-through** any analyzer args after `--`:
```bash
bash SETUP.sh --full -- --strong-only --min-rvol 1.5 --session-filter
```

Systemd unit default में यह ExecStart use करता है:
```
--strong-only --entry-confirmation-sec 15 --entry-score 4.0 --entry-evidence 30
--survival-check-sec 15 --survival-min-favor-pct 0.0001 --stale-feed-sec 90
--no-ui --duration-hours 6.5
```

---

## 🎛️ Analyzer CLI Flags Reference

`bash SETUP.sh -- <args>` या directly `python3 live_hit_rate_analyzer.py <args>`।

### Session
- `--config PATH` — Angel One credentials file (default `config.json`)
- `--duration-hours N` — session length (default 1.0)
- `--symbols A,B,C` — subset filter (default all from config)
- `--skip-market-hours-check` — force run outside 9:15–15:30 IST
- `--no-ui` — headless mode (VPS friendly, prints status every 10s)
- `--report-path PATH` — EOD report file location

### Signal Filters
- `--strong-only` — record ONLY `STRONG_LONG` + `STRONG_SHORT`
- `--skip-weak` — record STRONG + LONG/SHORT (skip WEAK)
- `--session-filter` — enable NSE phase gate (block LUNCH/PRE_CLOSE/CLOSING)
- `--allowed-phases OPENING,MORNING,AFTERNOON` — customize allowed phases
- `--no-entry-cutoff 15:15` — no new entries after this IST time
- `--holidays 2026-01-26,...` — comma-separated NSE holidays (YYYY-MM-DD)
- `--min-rvol 1.5` — require this relative volume vs 20-min average
- `--rvol-window-minutes 20` — RVOL rolling window
- `--rvol-warmup-buckets 5` — need this many 1-min buckets before RVOL valid
- `--rvol-strict-warmup` — block signals during RVOL warmup instead of allow

### 15-Second Sniper Policy
- `--entry-confirmation-sec 15` — continuous-qualification window (0 disable)
- `--entry-score 4.0` — minimum |score| during confirmation
- `--entry-evidence 30` — minimum evidence during confirmation
- `--survival-check-sec 15` — one-shot MFE check after entry (0 disable)
- `--survival-min-favor-pct 0.0001` — 0.01% MFE required to keep signal alive

### Engine Overrides
- `--strong-threshold N` — override calibrated 4.0
- `--normal-threshold N` — override calibrated 3.0
- `--weak-threshold N` — override calibrated 2.0
- `--ema-alpha 0.3` — score smoothing factor

### Cost Model
- `--cost-pct 0.0006` — explicit round-trip charges (spread already modeled
  via bid/ask executable fills)
- `--latency-slippage-bps 0` — optional adverse latency slippage per fill

### Horizons + Verdict
- `--horizons 5,15,30,60,120,300` — comma-separated seconds
- `--min-samples 20` — bucket needs this many samples before verdict shown
- `--dedup-seconds 5.0` — same-state signal dedup window

### Diagnostics + Reliability
- `--diagnose` — dump first N raw WS messages to `logs/raw_ws_dump.jsonl`
- `--dump-count 100` — how many raw messages to dump
- `--dump-path PATH` — dump file location
- `--stale-feed-sec 90` — auto-exit with code 75 if no ticks for this many
  seconds during market hours (systemd `Restart=always` will restart process).
  `--stale-feed-sec 0` disables the guard.
- `--log-path PATH` — predictions JSONL log location
- `--engine-demo` — run 8-scenario engine self-test and exit

---

## 📊 Sample EOD Report (annotated)

```
══════════════════════════════════════════════════════════════════════════════
  📶 DATA FLOW QUALITY (from real-market run)
──────────────────────────────────────────────────────────────────────────────
  Raw messages received :    288,640
  Parsed successfully   :    288,640  (100.0%)
  Parse failures        :          0  (0.0%)
  Symbols with data     :         96 of 100 expected
  Time to first tick    :        1.4 s
──────────────────────────────────────────────────────────────────────────────
  ✅ Data flow healthy: 288,640 valid ticks parsed.
══════════════════════════════════════════════════════════════════════════════

  📊 END-OF-DAY HIT RATE REPORT
══════════════════════════════════════════════════════════════════════════════
  Session duration    : 06:30:00
  Symbols tracked     : 96
  Total ticks         : 288,640
  Signals computed    : 180,617
  Signals recorded    : 3,412
  Signals deduped     : 12,458  (same state within 5s)
  Predictions evaluated: 20,472
  Execution model     : bid/ask executable + 0.0600% charges + 0.00 bps/fill latency
  ...

  🎯 HIT RATE BY STATE × HORIZON
──────────────────────────────────────────────────────────────────────────────
  Column meanings (CHARGES = 0.060% round-trip; spread via bid/ask):
    Hit %       = % of signals that went in predicted direction
    %AboveCost  = % of signals where profit > cost (COUNT, not amount)
    AvgRet %    = average per-trade return BEFORE cost (small = noise)
    NetEdge %   = average per-trade return AFTER cost (BOTTOM LINE)
    → +ve NetEdge = profitable | -ve NetEdge = loss-making

  State          Horizon    N   Hit %   %AboveCost   AvgRet %  NetEdge %  Verdict
  ────────────────────────────────────────────────────────────────────────────
  STRONG_LONG      15s     56   58.9%      52.7%    +0.041%   +0.031%   ✓ marginal edge
  STRONG_LONG      30s     56   56.2%      49.1%    +0.014%   -0.046%   ✗ break-even
  STRONG_LONG      60s     56   52.1%      41.3%    -0.006%   -0.066%   ✗ noise
  ...

  🎯 15-SECOND POLICY OUTCOME (one row per confirmed signal)
──────────────────────────────────────────────────────────────────────────────
  Entry confirmation : 15s continuous qualification (ON)
  Survival exit      : 15s MFE ≥ 0.010% (ON)
  Confirmations      : started 4,821  passed 3,412  cancelled 1,409
  Survival check     : passed 1,205  failed 2,207
  Policy exits       : survival 2,207  max_horizon 1,205

  State          N   Hit %   AvgRet %   NetEdge %   Verdict
  ────────────────────────────────────────────────────────────────
  STRONG_LONG   56   64.3%   +0.089%    +0.029%    ✓ marginal edge
  STRONG_SHORT  38   60.5%   +0.072%    +0.012%    ~ borderline
  ────────────────────────────────────────────────────────────────

  📌 HONEST OVERALL VERDICT
══════════════════════════════════════════════════════════════════════════════
  Total predictions evaluated: 20,472
  Overall directional hit rate: 51.8%
  Overall NET profit rate: 44.2% (after 0.06% cost)
  Average net edge per signal: +0.023%

  🟡 BREAK-EVEN: Signals slightly predictive but cost eats edge.
     Recommendation: Focus on STRONG_LONG/STRONG_SHORT only, or refine params.
```

**Interpretation guide:**

| `NetEdge %` | Verdict | कहा जा रहा है |
|---|---|---|
| `> +0.05%` and hit_rate > 55% | ✓✓ profitable | Deploy tiny capital के लिए ready |
| `+0.01% to +0.05%` | ✓ marginal edge | और days चाहिए |
| `-0.03% to 0%` | ~ borderline | Cost eating the edge |
| `< -0.03%` | ✗ noise / loss | Rebuild strategy |

---

## 💾 Output Files

`bash SETUP.sh` के बाद अपने-आप बनते हैं:

| File | Content |
|---|---|
| `logs/hit_rate_predictions.jsonl` | Every evaluated prediction (audit trail, one JSON per line) |
| `logs/hit_rate_report.txt` | Comprehensive EOD text report (जो terminal पर print होता है) |
| `logs/scanner.log` | System-level rotating logs (10 MB × 5 backups) |
| `logs/scrip_master.json` | Angel One scrip master cache (24-hour TTL) |
| `logs/raw_ws_dump.jsonl` | First N raw WS messages (only with `--diagnose`) |

**Audit trail row example** (`hit_rate_predictions.jsonl`):
```json
{
  "ts_fired": 1721544123.456,
  "ts_evaluated": 1721544153.463,
  "actual_horizon_s": 30.007,
  "target_horizon_s": 30.0,
  "symbol": "RELIANCE-EQ",
  "state": "STRONG_LONG",
  "score": 4.532,
  "evidence": 81.4,
  "regime": "N·T↑·B",
  "hour": 10,
  "price_at_signal": 2530.15,
  "ltp_at_signal": 2530.10,
  "bid_at_signal": 2530.05,
  "ask_at_signal": 2530.15,
  "bid_qty_at_signal": 3120,
  "ask_qty_at_signal": 2840,
  "spread_bps_at_signal": 3.95,
  "price_at_horizon": 2532.80,
  "ltp_at_horizon": 2532.85,
  "raw_return_pct": 0.1046,
  "directional_return_pct": 0.1046,
  "charges_pct": 0.0601,
  "net_return_pct": 0.0445,
  "is_hit": true,
  "is_net_profitable": true,
  "timed_out": false
}
```

---

## ⚙️ `config.json` Reference

Copy the template और नीचे wale 4 credentials भरो (बाकी सब defaults OK हैं):

```json
{
    "angel_one": {
        "api_key":     "YOUR_SMARTAPI_KEY",
        "client_code": "A1234567",
        "pin":         "1234",
        "totp_secret": "JBSWY3DPEHPK3PXP"
    },
    "symbols": [
        "RELIANCE-EQ", "TCS-EQ", "HDFCBANK-EQ", "INFY-EQ", "ICICIBANK-EQ",
        "..."
    ],
    "scanner": {
        "signal_dedup_seconds": 5.0,
        "ui_refresh_ms": 500,
        "system_log_path": "logs/scanner.log",
        "scrip_master_cache_path": "logs/scrip_master.json",
        "scrip_master_ttl_hours": 24
    },
    "engine": {
        "history_seconds": 60,
        "ema_alpha": 0.3,
        "threshold_strong": 4.0,
        "threshold_normal": 3.0,
        "threshold_weak":   2.0,
        "spoof_dampener_strength": 0.5,
        "kill_switch_spread_multiplier": 3.0
    }
}
```

**Credentials कहाँ से मिलेगी:**
- **api_key** — smartapi.angelbroking.com → login → My Apps → नया app बनाओ
- **client_code** — Angel One login ID (जैसे `A1234567`)
- **pin** — Angel One 4-digit trading MPIN
- **totp_secret** — Google Authenticator में QR scan करते समय "Manual entry" पर
  tap करके base32 secret copy करो (जैसे `JBSWY3DPEHPK3PXP`)

**Symbols format:** Angel One convention में `SYMBOL-EQ` (जैसे `RELIANCE-EQ`)।

---

## 🧠 How It Works Internally (compact overview)

```
Angel One SmartWebSocketV2 (SnapQuote mode, mode=3)
        │  Level-2 Top-5 depth + LTP + total buy/sell qty
        ▼
AngelOneWSAdapter.parse
        │  Bid/ask arrays + sequence_number + exchange_ts + receive_ts
        ▼
BookDynamicsEngine ×N (per-symbol, thread-safe RLock)
        │
        ├─ Validate (sequence-based dedup — NOT timestamp-only)
        ├─ 17 Metrics compute:
        │   • L1 / Top-5 / weighted / book-wide imbalance
        │   • Buy/sell ROC (1s/5s/10s)
        │   • Liquidity flow (adds vs removes)
        │   • Aggressor ratio (tick rule, 5s)
        │   • Mid-price ROC + LTP ROC
        │   • Spoofing / iceberg / replenishment suspicion
        │   • Spread / kill switch (>3× median spread)
        ├─ Composite: weighted-avg of 8 features → [-1, +1]
        ├─ Spoof dampener → * (1 - k * spoof_susp)
        ├─ Scale to [-10, +10] → EMA smooth (α=0.3)
        └─ State: STRONG_LONG (|s|≥4) / LONG (≥3) / WEAK_LONG (≥2) / NEUTRAL
        │
        ▼
HitRateAnalyzer.record_signal (via LiveHitRateSession._on_tick)
        │
        ├─ 15s Entry Confirmation Gate (continuous qualification)
        ├─ State filter (--strong-only / --skip-weak)
        ├─ Dedup gate (5s same-state)
        ├─ Cooldown gate (opt-in, default off)
        ├─ Session phase gate (opt-in)
        ├─ RVOL gate (opt-in)
        ├─ Executable entry price: LONG=ask, SHORT=bid
        └─ Add to LiveSignalMonitor + per-horizon pending buckets
        │
        ▼
LiveSignalMonitor.on_tick (every subsequent tick for the symbol)
        │
        ├─ Update current directional return using executable exit price
        │   (LONG exits bid, SHORT exits ask)
        ├─ Update MFE / MAE + horizon snapshots
        ├─ 15s Survival Exit Check (one-shot):
        │   - If MFE ≥ 0.01% within window → PASS, keep to max horizon
        │   - Otherwise → FAIL, close signal now @ current exit,
        │     record in policy bucket
        └─ Max horizon reached → close signal, record in policy bucket
        │
        ▼
Multi-Dimensional Stats (thread-safe with _stats_lock)
        │
        ├─ state × horizon (main diagnostic)
        ├─ state × evidence bucket (0-30 / 30-50 / 50-70 / 70+)
        ├─ state × regime label
        ├─ state × hour_of_day (IST)
        ├─ state × symbol
        └─ policy bucket (one outcome per confirmed signal)
        │
        ▼
EOD Report (comprehensive text output + JSONL audit trail)
```

**Thread model:** 1 worker thread (Angel One WS callback) does everything —
parse, engine update, signal recording, monitor updates. UI thread (rich) reads
snapshots via locks. Health thread (5s cadence) checks stale-feed and warns.

**Key production feature — stale-feed auto-exit:** If WebSocket dies silently
mid-session (network hiccup, broker-side disconnect), the health thread detects
"no valid ticks for 90+ seconds during market hours" and requests shutdown with
exit code 75. Systemd's `Restart=always` restarts the process. Silent-failure
recovery without manual intervention.

---

## 🎓 Trader's Interpretation Guide

### After 1-day diagnostic run
- **Parse rate < 100%?** Adapter field mismatch — run with `--diagnose`, share
  the raw dump with maintainer.
- **`Time to first tick > 30s`?** WebSocket subscription issue or wrong market
  hours.
- **Zero signals with `--strong-only`?** Score threshold might be too high for
  today's conditions. Try `--strong-threshold 3.5` for verification.

### After 5-10 day sample
Every table's **`NetEdge %`** column is the bottom line:

| Net Edge | Meaning |
|---|---|
| `> +0.05%` and hit rate > 55% | Real edge. Deploy small capital (₹10-25K max) with proper stops. |
| `+0.01% to +0.05%` | Marginal. Longer test (2-4 weeks). Refine gates. |
| `-0.03% to 0%` | Break-even. Cost eating edge. Try tighter filters. |
| `< -0.03%` | Noise / loss. **Do NOT deploy.** Fundamental rework needed. |

### Common patterns worth investigating
- **STRONG signals profitable, LONG/WEAK losses:** turn on `--strong-only`
- **Losses concentrated in specific hour:** check hour breakdown, add
  `--allowed-phases MORNING,AFTERNOON` to skip volatile openings
- **Losses when regime = RANDOM:** you don't have a regime-adaptive strategy
  yet, but you can filter — `--session-filter` at minimum
- **High hit rate but negative net edge:** transaction costs (`--cost-pct`)
  exceed your actual charges → recalibrate

### Reality Check
- **Renaissance Medallion Fund win rate: 50.75%.** Yet billions profit yearly.
  It's about payoff ratio, not win rate.
- **Retail Level-2 latency is 50-200ms** vs institutional colocation (<1ms).
  Some signals will be "priced in" before you see them. Real for anyone not
  colocated.
- **SEBI 2024 report:** 90% of retail intraday traders lose money.
  Microstructure tools do NOT change this by themselves — they measure whether
  YOUR specific approach has edge or not.

---

## 🔧 Troubleshooting

### `SmartApi import failed`
```bash
bash SETUP.sh --setup-only   # rebuilds venv + reinstalls smartapi-python
```

### `venv activation failed / bin/activate missing`
```bash
rm -rf ~/NSE-Equity-Intraday/venv   # या /root/... अगर root user
bash SETUP.sh --setup-only
```

### `No symbols resolved. Check config.symbols.`
Angel One scrip master में कुछ symbols के names बदल गए होंगे। Check करो:
```bash
python3 -c "
import json
data = json.load(open('logs/scrip_master.json'))
nse_eq = [i['symbol'] for i in data if i.get('exch_seg')=='NSE' and i['symbol'].endswith('-EQ')]
print(f'{len(nse_eq)} NSE-EQ symbols in scrip master')
# Check specific ones from your config:
for s in ['TATAMOTORS-EQ', 'ZOMATO-EQ', 'LTIM-EQ']:
    print(f'  {s}: {\"found\" if s in nse_eq else \"MISSING\"}')"
```

### `Login failed`
- Check `totp_secret` — यह base32 secret है, current TOTP code नहीं
- Check current TOTP works: `python3 -c "import pyotp; print(pyotp.TOTP('YOUR_SECRET').now())"`
- Angel One dashboard पर API access enabled है या नहीं

### WebSocket disconnects during session
Systemd unit `Restart=always` + `--stale-feed-sec 90` से 90s stale feed पर
process auto-restart होगा। Manual restart:
```bash
bash SETUP.sh --service-stop
bash SETUP.sh --service-start
```

### Config credentials nano में नहीं भरे
```bash
nano ~/NSE-Equity-Intraday/config.json   # या /root/...
chmod 600 ~/NSE-Equity-Intraday/config.json
bash SETUP.sh --run   # skip install, बस launch
```

### 100% parse failure at startup
Angel One SmartAPI का message format बदल गया होगा। `--diagnose` से raw dump
capture करो:
```bash
bash SETUP.sh -- --diagnose --duration-hours 0.05   # 3 min diagnostic
cat logs/raw_ws_dump.jsonl | head -1 | python3 -m json.tool
```
Share `logs/raw_ws_dump.jsonl` with maintainer.

---

## 🛡️ Safety + Disclaimers

- **यह analytical infrastructure है, investment advice नहीं।** Signal output book
  dynamics observations हैं; profit/loss पूरी तरह आपके risk management + execution
  पर depend करता है।
- **कोई real order नहीं जाता।** यह पूरी तरह measurement-only tool है।
- **SEBI compliance:** Algorithmic trading with retail brokers requires
  disclosure. Angel One की algo trading policy check करें।
- **Data caveats:**
  - Angel One SnapQuote = book-update snapshots (~5-10 per second per symbol),
    NOT true per-trade tick-by-tick
  - Spoofing / iceberg detection uses probabilistic *_suspicion scores,
    never guaranteed
  - Cancel vs Execute inference is heuristic (Lee-Ready tick rule)
- **Retail latency reality:** 50-200ms delay vs institutional colo (<1ms).
  Some signals will be already priced-in.
- **Fresh capital risk warning:** SEBI 2024 report — **90% of retail intraday
  traders lose money.** Do NOT deploy capital before at least 2 weeks of
  live paper-trading data with positive net edge across multiple sessions.

---

## 🧾 Technical Details (for developers)

### Performance (1-core / 2 GB VPS)

| Load | Throughput | Latency p50 | Latency p99 |
|---|---:|---:|---:|
| NSE normal (~500 tps) | ~110 tps sustained | 100 µs | 250 µs |
| Opening burst (3000 tps) | 2900+ tps | 105 µs | 300 µs |

### Sequence-Based Dedup (P0 correctness)

MarketSnapshot captures:
- `sequence_number` — Angel One's per-message ID (primary ordering key)
- `exchange_timestamp` — broker's exchange clock (second-resolution, for audit)
- `received_timestamp` — local receive clock (sub-second, for analytics)

Engine drops updates with `sequence <= last_sequence`, EXCEPT when exchange
time advanced substantially (reconnect / new session reset). Legacy feeds
without sequence fall back to exact-content fingerprint + strict-less-than
event time.

**यह क्यों matter करता है:** पुराने code में `snap.timestamp <= last_ts` सिर्फ
second-resolution timestamp check करता था — genuine same-second book updates
drop हो रहे थे। Sequence-based ordering से यह fix हुआ।

### Executable Bid/Ask Cost Model

`ExecutionCostModel` shared class:
- LONG enters at `ask`, exits at `bid` (spread crossed = real cost)
- SHORT enters at `bid`, exits at `ask`
- `--cost-pct` = explicit round-trip charges only (spread already captured)
- `--latency-slippage-bps` = optional adverse adjustment per fill (default 0)

पुराने code में LTP-to-LTP return minus 6 bps calculation था — spread completely
ignore हो रहा था। अब यह fix है।

### Systemd Unit (auto-generated)

```ini
[Unit]
Description=NSE Live Hit Rate Analyzer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/NSE-Equity-Intraday
Environment="PATH=/root/NSE-Equity-Intraday/venv/bin:/usr/local/bin:/usr/bin:/bin"

ExecStart=/root/NSE-Equity-Intraday/venv/bin/python3 \
    /root/NSE-Equity-Intraday/live_hit_rate_analyzer.py \
    --config /root/NSE-Equity-Intraday/config.json \
    --duration-hours 6.5 --no-ui \
    --strong-only \
    --entry-confirmation-sec 15 --entry-score 4.0 --entry-evidence 30 \
    --survival-check-sec 15 --survival-min-favor-pct 0.0001 \
    --stale-feed-sec 90 \
    --log-path /root/NSE-Equity-Intraday/logs/hit_rate_predictions.jsonl \
    --report-path /root/NSE-Equity-Intraday/logs/hit_rate_report.txt

Restart=always
RestartSec=60
LimitNOFILE=65536
MemoryMax=1G

[Install]
WantedBy=multi-user.target
```

### Requirements

Python 3.9+ (tested on 3.10, 3.11, 3.12):
```
smartapi-python>=1.4.0
websocket-client>=1.6.0
pyotp>=2.9.0
requests>=2.31.0
rich>=13.0.0
```

---

## 📜 License

MIT — for your own trading system. Attribution appreciated but not required.

---

## 🙏 Credits + Design Notes

Designed collaboratively with iterative review across:
- Order-flow theory (17 microstructure metrics from academic literature)
- Production infrastructure (single-file architecture, systemd auto-restart,
  stale-feed guard, sequence-based dedup)
- Trader-level pragmatism (Gemini's "Sniper Bot" 15-second policy, calibrated
  thresholds from 67k+ live signals, executable bid/ask cost model)

**Non-goals (deliberately excluded):**
- ML/AI models (LSTM, transformers) — keep math simple + interpretable
- Multiple broker support — Angel One only
- Web dashboard — terminal + JSONL sufficient
- Slack/Telegram alerts — standalone
- Options / futures — Cash Equity only
- Real order routing — this is measurement, not trading
