# NSE Equity Intraday — Real-Time Book Dynamics Scanner

**Production-ready order-flow / market-microstructure analytics** के लिए NSE Cash Market Level-2 SnapQuote data पर 100 symbols को parallel में scan करने वाला system।

> कोई candlestick नहीं, कोई RSI/MACD नहीं, कोई OHLC नहीं।
> केवल **real-time tick-by-tick book dynamics** — Angel One SmartAPI + BookDynamicsEngine पर।

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
