#!/usr/bin/env python3
"""
live_hit_rate_analyzer.py
=========================
NSE Cash Market — Single-file Real-Time Hit-Rate Analyzer.

यह एक ही Python file में पूरा system:
  1. BookDynamicsEngine — 17 microstructure metrics engine
  2. Angel One WebSocket adapter + SmartAPI connector
  3. SessionStateManager + RVOLCalculator + CooldownManager gates
  4. HitRateAnalyzer + LiveSignalMonitor (15-second entry/exit rules)
  5. Rich console UI + headless mode + JSONL audit log + EOD report

===================================================================
QUICK START
===================================================================

1. Dependencies install करें:
       pip install -r requirements.txt

2. Engine का self-test (5-सेकंड, कोई config नहीं चाहिए):
       python3 live_hit_rate_analyzer.py --engine-demo

3. Live diagnostic (15 मिनट, Angel One credentials चाहिए):
       cp config.example.json config.json    # credentials भरें
       python3 live_hit_rate_analyzer.py --config config.json \\
           --diagnose --duration-hours 0.25
   सारे filters OFF by default — हर actionable signal record होगा।

4. Full trading day with sniper policy (opt-in via explicit flags):
       python3 live_hit_rate_analyzer.py --config config.json \\
           --duration-hours 6.5 --no-ui \\
           --strong-only \\
           --entry-confirmation-sec 15 --entry-score 4.0 \\
           --survival-check-sec 15 --survival-min-favor-pct 0.0001

===================================================================
ANGEL ONE CREDENTIALS
===================================================================
- smartapi.angelbroking.com → login → "My Apps" → नया app create करें
- API Key मिलेगा (config.json में डालें)
- 2FA setup: Google Authenticator में QR scan करते समय "Manual entry" पर
  tap करके base32 secret copy करें (यह totp_secret में डालें)
- MPIN = आपका 4-digit trading PIN

===================================================================
FILE OUTPUTS
===================================================================
- logs/hit_rate_predictions.jsonl — हर evaluated prediction (audit trail)
- logs/hit_rate_summary.txt        — EOD comprehensive report
- logs/scanner.log                 — System logs (rotating 10MB × 5)
- logs/scrip_master.json           — Angel One scrip master cache (24hr TTL)
- logs/raw_ws_dump.jsonl           — First N raw messages (with --diagnose)

===================================================================
"""

from __future__ import annotations

import bisect
import logging
import math
import queue as _queue_mod
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timedelta, timezone
from enum import Enum
from typing import Any, Deque, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# 1. Module-level constants + logger + core enums
# ---------------------------------------------------------------------------
# ये constants पूरी file में shared हैं और performance-critical inner loops
# में hardcoded literal की जगह use होते हैं ताकि tuning एक जगह हो।

# Divide-by-zero guard for ratio math. Values below EPS are treated as zero
# whenever they appear in a denominator to keep the composite score bounded.
EPS = 1e-9

# Minimum denominator for Rate-of-Change (ROC) computations on total buy/sell
# quantities. Very small books (illiquid symbols, opening ticks) would otherwise
# produce huge ROC spikes from a tiny absolute change. Anchoring the denominator
# to at least 1000 gives a stable, bounded ROC signal.
MIN_QTY_FLOOR = 1000

# Default window (seconds) for the per-symbol snapshot history buffer. Kept
# just large enough to serve the longest ROC lookback (10s) with headroom;
# increasing this raises memory + CPU per tick.
DEFAULT_HISTORY_SEC = 60.0

# NSE Cash Market minimum price increment. Used by the iceberg tracker to
# canonicalise price levels (two prices are the same "level" if they round
# to the same tick) and by the demo/paper-trade helpers.
NSE_TICK_SIZE = 0.05

# Single module-level logger used across engine, adapter, session, analyzer.
# Downstream setup_logging() configures handlers + rotating file output.
logger = logging.getLogger("BookDynamicsEngine")


class SignalState(Enum):
    """
    All possible per-symbol signal states emitted by BookDynamicsEngine.

    Directional intent (LONG / SHORT) is derived from the sign of the
    smoothed composite score; strength (STRONG / normal / WEAK) from its
    magnitude vs the calibrated thresholds in `EngineConfig`.

    NEUTRAL      = |score| below the WEAK threshold — no view.
    SUPPRESSED   = kill switch active (spread blew out, circuit hit, or
                   the book crossed/locked) — signal generation intentionally
                   halted regardless of what the score says.
    """
    STRONG_LONG  = "STRONG_LONG"
    LONG         = "LONG"
    WEAK_LONG    = "WEAK_LONG"
    NEUTRAL      = "NEUTRAL"
    WEAK_SHORT   = "WEAK_SHORT"
    SHORT        = "SHORT"
    STRONG_SHORT = "STRONG_SHORT"
    SUPPRESSED   = "SUPPRESSED"


class AggressorSide(Enum):
    """
    Lee-Ready tick-rule classification of who initiated the most recent
    trade (aggressive market order). Used to accumulate a rolling 5-second
    buyer-vs-seller aggression ratio inside the engine.

    BUYER  = last trade printed at or above the prior mid → buy-side aggressor
    SELLER = last trade printed at or below the prior mid → sell-side aggressor
    NA     = insufficient prior data (very first tick, or crossed book)
    """
    BUYER  = 1
    SELLER = -1
    NA     = 0


# ---------------------------------------------------------------------------
# 2. Core data classes — book snapshots and derived state
# ---------------------------------------------------------------------------
# ये dataclasses पूरे pipeline में flow करते हैं:
#   AngelOneWSAdapter.parse  →  MarketSnapshot
#   BookDynamicsEngine.update(snap)  →  SignalResult (carrying BookMetrics)
#   HitRateAnalyzer + LiveSignalMonitor consume both above.


@dataclass
class DepthLevel:
    """
    A single price/quantity level from the top-N order book depth.

    NSE SnapQuote delivers top-5 buy and top-5 sell levels per tick. Each
    level is one instance of this class. The `__post_init__` coercion makes
    the object tolerant of raw broker payloads where price arrives as int
    (paise) or string.
    """
    price: float
    quantity: int

    def __post_init__(self):
        # Defensive normalisation — broker payloads sometimes carry ints
        # (paise integer) or numeric strings; we normalise to Python floats/ints
        # here so downstream engine math never has to worry about types.
        self.price = float(self.price)
        self.quantity = int(self.quantity)


@dataclass
class MarketSnapshot:
    """
    Broker-agnostic snapshot of a symbol's market state at one instant.

    This is the canonical unit that flows through the pipeline. The adapter
    layer (AngelOneWSAdapter) is responsible for producing a MarketSnapshot
    from whatever raw broker payload it receives; every downstream component
    operates purely on this contract.

    Ordering / identity contract:
      * `bids` MUST be sorted best-first (highest bid price at index 0)
      * `asks` MUST be sorted best-first (lowest ask price at index 0)
      * `timestamp` is the event/receive clock used by analytics
        (sub-second precision preferred — see `received_timestamp`)
      * `exchange_timestamp` preserves the broker's exchange clock
        (usually only second-resolution — kept for audit + reconnect logic)
      * `sequence_number` is the PRIMARY ordering + dedup key when the
        broker provides it. The engine drops any snapshot whose sequence
        is <= the last one seen (except recognised session resets).
    """
    # Event/analytics clock. Sub-second when available; falls back to the
    # exchange timestamp for legacy/simulated feeds.
    timestamp: float
    symbol: str
    ltp: float                       # last traded price
    ltq: int                         # last traded qty (0 = unknown/OK)
    volume_traded: int               # cumulative day volume (monotonic)
    total_buy_qty: int               # exchange-broadcast BOOK-WIDE buy qty
    total_sell_qty: int              # exchange-broadcast BOOK-WIDE sell qty
    bids: List[DepthLevel]           # top-N buy levels, best-first
    asks: List[DepthLevel]           # top-N sell levels, best-first

    # ---- Market-data identity / clocks (added by the P0 correctness pass) ---
    # These three fields let the engine order and de-duplicate correctly even
    # when the exchange clock is only second-resolution. Simulator/demo callers
    # can leave them at None; the engine falls back to content-fingerprint
    # dedup + event-time ordering in that case.
    sequence_number: Optional[int] = None
    exchange_timestamp: Optional[float] = None
    received_timestamp: Optional[float] = None

    # ---- Optional context fields (informational only, not used by engine) ---
    open_price:       Optional[float] = None
    high_price:       Optional[float] = None
    low_price:        Optional[float] = None
    close_price_prev: Optional[float] = None
    upper_circuit:    Optional[float] = None
    lower_circuit:    Optional[float] = None

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def best_bid_qty(self) -> int:
        return self.bids[0].quantity if self.bids else 0

    @property
    def best_ask_qty(self) -> int:
        return self.asks[0].quantity if self.asks else 0

    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid


@dataclass(frozen=True)
class ExecutionCostModel:
    """
    Single source of truth for "what price would this signal actually fill at?"
    Shared by HitRateAnalyzer, LiveSignalMonitor, and the audit-log writers.

    Design philosophy:
        Spread crossing is captured directly by using bid/ask executable
        quotes — LONG orders cross the ASK on entry and lift the BID on
        exit; SHORT orders do the opposite. This means the bid-ask spread
        is *automatically* part of every P&L number, without any separate
        "slippage" adjustment.

        `transaction_cost_pct` therefore represents ONLY the explicit
        round-trip charges (STT + brokerage + GST + stamp duty + exchange
        fees) — NOT spread. Default 0.0006 ≈ 0.06% is the typical Zerodha /
        Angel One flat charge for liquid Nifty stocks.

        `latency_slippage_bps` is an OPTIONAL adverse adjustment applied
        per fill — useful for stress-testing "what if my orders reach the
        exchange 100 ms late and the price has moved?" Default 0 keeps
        the model bid/ask-only. Set to something like 2-5 bps to model a
        typical retail Level-2 latency handicap.

    यह design क्यों:
        पुराने code में LTP-to-LTP return से 6 bps घटाया जाता था — spread
        पूरी तरह ignore हो रहा था। Small-cap के लिए spread अक्सर 20-50 bps
        होता है — बिना उसे model किए hit-rate misleading था। अब LONG=ask
        entry, LONG=bid exit से spread अपने-आप cost में आ जाता है।
    """
    transaction_cost_pct: float = 0.0006     # explicit round-trip charges
    latency_slippage_bps: float = 0.0        # optional adverse per-fill slip

    def __post_init__(self) -> None:
        # Configuration guardrails — the model must never be constructed
        # with negative charges or negative slippage, as those flip the
        # sign of every downstream P&L calculation silently.
        if self.transaction_cost_pct < 0:
            raise ValueError("transaction_cost_pct cannot be negative")
        if self.latency_slippage_bps < 0:
            raise ValueError("latency_slippage_bps cannot be negative")

    def fill_price(
        self,
        side: str,
        is_entry: bool,
        ltp: float,
        best_bid: Optional[float],
        best_ask: Optional[float],
    ) -> float:
        """
        Return the price at which a marketable order for `side` would fill
        RIGHT NOW, including any configured latency slippage.

        Truth table:
            side=LONG,  is_entry=True   →  cross the ASK  (we are buying)
            side=LONG,  is_entry=False  →  lift the BID   (we are selling)
            side=SHORT, is_entry=True   →  lift the BID   (we are selling)
            side=SHORT, is_entry=False  →  cross the ASK  (we are covering/buying)

        Falls back to LTP if the relevant quote is missing (legacy feeds,
        pre-open state, or paused market data). Raises ValueError only if
        BOTH the quote AND the LTP are unusable — that is a data-quality
        error worth surfacing to the caller.
        """
        side = side.upper()
        if side not in {"LONG", "SHORT"}:
            raise ValueError(f"Unsupported side: {side}")

        # "Are we buying at this fill?" → yes for LONG-entry OR SHORT-exit.
        is_buy = (side == "LONG" and is_entry) or (side == "SHORT" and not is_entry)
        quote = best_ask if is_buy else best_bid

        # Graceful fallback: if the best bid/ask is missing, treat LTP as
        # a best-effort mid-market estimate. Only fail loudly when neither
        # a quote nor an LTP is usable.
        if quote is None or quote <= 0:
            quote = ltp
        if quote is None or quote <= 0:
            raise ValueError("No positive executable quote or LTP")

        # Latency slippage is ALWAYS adverse — buys fill above the quote,
        # sells fill below the quote. This is intentional; a favourable
        # slippage would let the model report unearned profit.
        slip = self.latency_slippage_bps / 10000.0
        return quote * (1.0 + slip if is_buy else 1.0 - slip)

    @staticmethod
    def gross_directional_return(side: str, entry_price: float, exit_price: float) -> float:
        """
        Signed return where +ve means the signal was directionally correct,
        regardless of long/short. Sign-flipping keeps every downstream
        aggregate (hit rate, average, standard deviation) directionally
        consistent so we can pool longs and shorts into the same buckets.
        """
        if entry_price <= 0:
            return 0.0
        if side.upper() == "SHORT":
            # For a short: price down (exit < entry) is a win → positive return.
            return (entry_price - exit_price) / entry_price
        # For a long: price up (exit > entry) is a win → positive return.
        return (exit_price - entry_price) / entry_price

    def charge_return(self, entry_price: float, exit_price: float) -> float:
        """
        Round-trip explicit charges expressed as a return on entry notional.
        Uses the average of entry and exit price so the charge tracks
        realistic INR outflow (STT/GST etc. are computed on both sides).
        """
        if entry_price <= 0:
            return 0.0
        avg_price = (entry_price + exit_price) / 2.0
        return self.transaction_cost_pct * avg_price / entry_price

    def evaluate(self, side: str, entry_price: float, exit_price: float) -> Tuple[float, float, float]:
        """
        One-shot P&L attribution: (gross, charges, net).

        gross    — directional return at the executable quotes (spread already in)
        charges  — round-trip explicit charges as a return
        net      — gross minus charges = the number that decides "did this signal
                   actually make money after everything?"
        """
        gross = self.gross_directional_return(side, entry_price, exit_price)
        charges = self.charge_return(entry_price, exit_price)
        return gross, charges, gross - charges

    def description(self) -> str:
        """Human-readable description printed in reports / logs for audit."""
        return (
            f"bid/ask executable + {self.transaction_cost_pct * 100:.4f}% charges"
            f" + {self.latency_slippage_bps:.2f} bps/fill latency"
        )


@dataclass
class RegimeState:
    """
    Per-symbol market-regime classification (Phase 2 addition).

    Real markets rotate through very different behavioural regimes throughout
    a single trading day: a trending open, a mean-reverting mid-session, a
    random lunch drift, an anxious pre-close. A signal that was profitable
    in one regime can be pure noise (or actively harmful) in another.

    We classify each symbol on THREE orthogonal dimensions:

        Volatility     LOW / NORMAL / HIGH
                       Ratio of recent σ to a longer baseline σ.
        Trend          TRENDING_UP / TRENDING_DOWN / MEAN_REVERTING / RANDOM
                       Sign of the mean return plus lag-1 autocorrelation
                       of tick returns.
        Depth Bias     BULL_STRUCTURAL / BEAR_STRUCTURAL / BALANCED
                       Rolling mean of book-wide imbalance (buy vs sell qty).

    Trader interpretation cheat-sheet:
        TRENDING       → directional signals are meaningful, trade them
        MEAN_REVERTING → invert signals (LONG becomes SHORT, contrarian)
        RANDOM         → NO edge in either direction, DO NOT trade
        HIGH_VOL       → widen entry thresholds + reduce position size
        LOW_VOL        → tighten thresholds to catch marginal moves

    Note: HitRateAnalyzer records the regime label with every signal, so the
    EOD report can bucket hit-rate BY regime — you'll see exactly which
    regimes your signals actually work in.
    """
    volatility: str = "NORMAL"        # LOW / NORMAL / HIGH
    trend: str = "RANDOM"             # TRENDING_UP / TRENDING_DOWN / MEAN_REVERTING / RANDOM
    depth_bias: str = "BALANCED"      # BULL_STRUCTURAL / BEAR_STRUCTURAL / BALANCED
    volatility_ratio: float = 1.0     # recent_σ / baseline_σ
    autocorr_lag1: float = 0.0        # lag-1 autocorrelation of tick returns
    depth_imbalance_mean: float = 0.0 # rolling mean of book_wide_imbalance
    # Only True after ~500 baseline samples are accumulated. Downstream
    # code MUST check this before trusting regime — an unconfident regime
    # is effectively "unknown" and should default to conservative behaviour.
    is_confident: bool = False

    @property
    def label(self) -> str:
        """
        Compact human-readable regime label used in reports + JSONL logs.
        Format: "<Vol>·<Trend>·<Depth>"  e.g. "N·T↑·B" = Normal volatility,
        Trending Up, Bull-Structural depth.
        """
        v = self.volatility[0]  # L/N/H first letter
        t = {"TRENDING_UP": "T↑", "TRENDING_DOWN": "T↓",
             "MEAN_REVERTING": "MR", "RANDOM": "R"}.get(self.trend, "?")
        d = {"BULL_STRUCTURAL": "B", "BEAR_STRUCTURAL": "S",
             "BALANCED": "N"}.get(self.depth_bias, "?")
        return f"{v}·{t}·{d}"

    def is_tradeable(self) -> bool:
        """
        Conservative decision helper — returns True only when we have
        enough data to be confident AND the trend regime has a directional
        edge. Warm-up periods and RANDOM regime return False.
        """
        if not self.is_confident:
            return False   # not enough baseline samples yet
        if self.trend == "RANDOM":
            return False   # no directional edge in this regime
        return True

    def should_invert_signal(self) -> bool:
        """
        Contrarian mode flag — in a confirmed mean-reverting regime, a
        book-level LONG signal is more likely to be a fade opportunity
        (short) and vice-versa. Only True once regime is confident.
        """
        return self.is_confident and self.trend == "MEAN_REVERTING"


class RegimeDetector:
    """
    Cheap, per-symbol regime classifier that runs alongside BookDynamicsEngine
    on every tick.

    Performance requirements (this runs 500-3000 times per second across
    100 symbols on a 1-core VPS):
      * Called on every tick, but heavy statistics only recomputed every
        `update_every_n_ticks` (default 100 ticks ≈ once per 3-20 seconds
        per symbol depending on activity).
      * Uses fixed-size deques for O(1) append and bounded memory.
      * Total memory: ~5000 floats × 100 symbols ≈ 4 MB — negligible.

    All thresholds are tunable, but the defaults were calibrated against
    Nifty stock behaviour: a lag-1 autocorrelation > +0.10 is a plausible
    trending signature, < -0.08 is mean reversion, and vol ratios below 0.7
    or above 1.5 reliably mark calm / stormy regimes respectively.
    """

    def __init__(
        self,
        update_every_n_ticks: int = 100,
        recent_window: int = 500,          # ~last few minutes at 5 tps
        baseline_window: int = 5000,       # much longer reference window
        vol_high_threshold: float = 1.5,   # recent σ / baseline σ upper cutoff
        vol_low_threshold: float = 0.7,    # recent σ / baseline σ lower cutoff
        autocorr_trending_threshold: float = 0.10,
        autocorr_reverting_threshold: float = -0.08,
        depth_bias_threshold: float = 0.15,
    ):
        # Configuration
        self.update_every = update_every_n_ticks
        self.vol_high = vol_high_threshold
        self.vol_low = vol_low_threshold
        self.trend_up = autocorr_trending_threshold
        self.trend_down = autocorr_reverting_threshold
        self.depth_bias_thr = depth_bias_threshold

        # Rolling buffers — fixed-size deques so old data drops automatically.
        self.recent_returns:    Deque[float] = deque(maxlen=recent_window)
        self.baseline_returns:  Deque[float] = deque(maxlen=baseline_window)
        self.recent_imbalances: Deque[float] = deque(maxlen=recent_window)

        # State machine
        self.last_price:  Optional[float] = None
        self.tick_count:  int = 0
        self.ticks_since_update: int = 0
        self.current_regime = RegimeState()

    def update(self, ltp: float, book_wide_imbalance: float) -> RegimeState:
        """
        Called by BookDynamicsEngine on every tick. Very cheap — just
        appends to rolling buffers and periodically triggers a batched
        recompute of the classification.

        Returns the CURRENT regime state (may be stale by up to
        `update_every_n_ticks` ticks if a recompute hasn't happened yet;
        this staleness is intentional to keep the per-tick cost bounded).
        """
        # Compute per-tick return whenever we have a previous price.
        # Skips the very first tick, and defensively any zero/negative price.
        if self.last_price is not None and self.last_price > 0:
            ret = (ltp - self.last_price) / self.last_price
            self.recent_returns.append(ret)
            self.baseline_returns.append(ret)
        self.last_price = ltp
        self.recent_imbalances.append(book_wide_imbalance)

        # Batched heavy-compute cadence: only run the O(N) statistics
        # (std / autocorr) once per `update_every` ticks. Called on the
        # WS worker thread — we cannot afford per-tick work here.
        self.tick_count += 1
        self.ticks_since_update += 1
        if self.ticks_since_update >= self.update_every:
            self.ticks_since_update = 0
            self._recompute()
        return self.current_regime

    def _recompute(self) -> None:
        """
        Batched heavy compute — runs once per `update_every` ticks. Reads
        the rolling buffers and produces a fresh RegimeState. Guarded so
        it silently no-ops until we have at least 50 recent samples (the
        very first minute of trading each day).
        """
        if len(self.recent_returns) < 50:
            return

        # ---- Volatility regime ----
        # Compare recent σ to a much longer baseline σ. If we don't yet
        # have a proper baseline (< 500 samples) we treat vol as NORMAL
        # by making the ratio ≈ 1.0.
        recent_std = self._std(self.recent_returns)
        baseline_std = (self._std(self.baseline_returns)
                        if len(self.baseline_returns) >= 500 else recent_std)
        vol_ratio = recent_std / max(baseline_std, 1e-9)
        if vol_ratio > self.vol_high:
            vol_regime = "HIGH"
        elif vol_ratio < self.vol_low:
            vol_regime = "LOW"
        else:
            vol_regime = "NORMAL"

        # ---- Trend regime via lag-1 autocorrelation of tick returns ----
        # Positive autocorr = returns cluster in the same direction = trend.
        # Negative autocorr = returns alternate = mean reversion.
        # We disambiguate up-vs-down trend using the sign of the mean.
        autocorr = self._autocorr_lag1(self.recent_returns)
        mean_return = sum(self.recent_returns) / len(self.recent_returns)
        if autocorr > self.trend_up:
            trend_regime = "TRENDING_UP" if mean_return > 0 else "TRENDING_DOWN"
        elif autocorr < self.trend_down:
            trend_regime = "MEAN_REVERTING"
        else:
            trend_regime = "RANDOM"

        # ---- Depth bias ----
        # Rolling mean of the book-wide imbalance (buy qty vs sell qty).
        # Structural imbalance persisting for many ticks implies genuine
        # one-sided flow (not just noise).
        depth_mean = sum(self.recent_imbalances) / len(self.recent_imbalances)
        if depth_mean > self.depth_bias_thr:
            depth_regime = "BULL_STRUCTURAL"
        elif depth_mean < -self.depth_bias_thr:
            depth_regime = "BEAR_STRUCTURAL"
        else:
            depth_regime = "BALANCED"

        # `is_confident` becomes True once we have a proper baseline σ
        # (500+ samples ≈ several minutes of live ticks per symbol). Until
        # then downstream code should treat regime as "unknown".
        self.current_regime = RegimeState(
            volatility=vol_regime, trend=trend_regime, depth_bias=depth_regime,
            volatility_ratio=vol_ratio, autocorr_lag1=autocorr,
            depth_imbalance_mean=depth_mean,
            is_confident=(len(self.baseline_returns) >= 500),
        )

    # -- Numerical helpers (pure functions, no state) --

    @staticmethod
    def _std(values) -> float:
        """Population standard deviation (biased). Returns 0.0 for n < 2."""
        n = len(values)
        if n < 2:
            return 0.0
        mean = sum(values) / n
        var = sum((x - mean) ** 2 for x in values) / n
        return var ** 0.5

    @staticmethod
    def _autocorr_lag1(values) -> float:
        """
        Pearson lag-1 autocorrelation coefficient of a sequence.
        Range [-1, +1]. Returns 0.0 for n < 3 or a degenerate (zero-variance)
        series (avoiding a divide-by-zero when the market is completely flat).
        """
        vals = list(values)
        n = len(vals)
        if n < 3:
            return 0.0
        mean = sum(vals) / n
        den = sum((x - mean) ** 2 for x in vals)
        if den < 1e-18:
            return 0.0
        num = sum((vals[i] - mean) * (vals[i-1] - mean) for i in range(1, n))
        return num / den


@dataclass
class BookMetrics:
    """
    Complete set of derived microstructure metrics for ONE snapshot.

    Produced by `BookDynamicsEngine._compute_metrics` and attached to every
    `SignalResult`. This is what feeds:
      * the composite signal score (via a weighted average of a subset of
        these metrics — see EngineConfig.w_* weights),
      * the diagnostics dict shipped in every JSONL log line, and
      * the reason strings shown in the live UI.

    Field-group conventions:
      * `*_imbalance`      → signed ratio in [-1, +1], positive = bullish
      * `*_roc_*`          → rate-of-change over the labelled window
      * `*_suspicion`      → probabilistic score in [0, 1], higher = more
                             suspicious
      * `*_likelihood_*`   → executable probability in [0, 1]
    """
    timestamp: float

    # ---- Static imbalances (each in [-1, +1], positive = bullish) ----
    book_wide_imbalance:      float = 0.0   # exchange-broadcast aggregate qty
    l1_imbalance:             float = 0.0   # best bid vs best ask qty only
    top5_imbalance:           float = 0.0   # sum of top-5 buy vs sell qty
    weighted_depth_imbalance: float = 0.0   # distance-weighted (near > far)

    # ---- Book dynamics (Rate of Change over 1s / 5s / 10s lookbacks) ----
    buy_book_roc_1s:  float = 0.0
    buy_book_roc_5s:  float = 0.0
    buy_book_roc_10s: float = 0.0
    sell_book_roc_1s:  float = 0.0
    sell_book_roc_5s:  float = 0.0
    sell_book_roc_10s: float = 0.0
    imbalance_roc_5s: float = 0.0   # highest-weighted feature — leading indicator

    # ---- Liquidity flow (Δ decomposition, absolute integer quantities) ----
    buy_added:      int = 0        # new buy qty appeared at any price level
    buy_removed:    int = 0        # buy qty disappeared (cancel or execute)
    sell_added:     int = 0
    sell_removed:   int = 0
    total_added:    int = 0
    total_removed:  int = 0
    book_activity:  int = 0        # sum of add + remove (raw "churn" measure)

    # ---- Spread + Mid + Price dynamics ----
    spread:                float = 0.0   # ask minus bid (absolute INR)
    normalized_spread_bps: float = 0.0   # spread as bps of mid — feed into kill switch
    spread_roc_5s:         float = 0.0
    mid_price:             float = 0.0
    mid_price_roc_5s:      float = 0.0
    ltp:              float = 0.0
    ltp_roc_5s:       float = 0.0
    interval_volume:  int   = 0
    buyer_aggressor_ratio_5s: float = 0.5   # [0, 1] via tick rule, baseline 0.5

    # ---- Cross-checks & book-integrity suspicions (each in [0, 1]) ----
    l1_vs_depth_divergence:    float = 0.0   # top-of-book vs deeper structure mismatch
    execution_likelihood_ask:  float = 0.0   # was ask consumed by trades or cancelled?
    execution_likelihood_bid:  float = 0.0
    cancellation_suspicion_ask: float = 0.0
    cancellation_suspicion_bid: float = 0.0
    spoofing_suspicion:        float = 0.0   # dampens composite score
    iceberg_suspicion:         float = 0.0   # hidden liquidity refill signature
    replenishment_score:       float = 0.0

    # ---- Phase 2 — market regime tag (per-symbol, updated batched) ----
    regime: RegimeState = field(default_factory=RegimeState)

    # ---- Kill switch (fast-market / halted-book protection) ----
    kill_switch_active: bool = False
    kill_switch_reason: Optional[str] = None


@dataclass
class SignalResult:
    """
    Final per-snapshot output of BookDynamicsEngine.update().

    Contains everything a downstream consumer needs:
      * `state`             — categorical signal (STRONG_LONG etc.)
      * `raw_score`         — composite in [-10, +10] BEFORE EMA smoothing
      * `smoothed_score`    — EMA(raw_score) — the number used for state
                              classification; also compared against
                              --entry-score in the confirmation gate
      * `evidence_strength` — 0..100 heuristic combining |score| and
                              feature agreement. This is NOT a probability;
                              it is a "how many features agree with this
                              direction" confidence proxy.
      * `reasons`           — short human-readable strings for logs + UI
      * `diagnostics`       — flat dict of every raw metric, for JSONL
      * `metrics`           — the full BookMetrics object (regime included)
    """
    timestamp:         float
    symbol:            str
    state:             SignalState
    raw_score:         float               # composite in [-10, +10] (pre-EMA)
    smoothed_score:    float               # EMA-smoothed, [-10, +10]
    evidence_strength: float               # 0..100 (NOT a probability)
    reasons:           List[str]
    diagnostics:       Dict[str, Any]
    metrics:           BookMetrics


# ---------------------------------------------------------------------------
# 3. Utility functions + TimeSeriesBuffer
# ---------------------------------------------------------------------------
# Small pure helpers used across the file. Kept module-level so they are
# both cheap to call (no attribute lookup) and easy to unit-test.


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    """
    Divide two numbers with an EPS-based zero guard. Returns `default`
    (0.0 by default) whenever the denominator is below the EPS threshold,
    so callers never have to try/except ZeroDivisionError inside hot loops.
    """
    if abs(den) < EPS:
        return default
    return num / den


def clamp(x: float, lo: float, hi: float) -> float:
    """Standard clamp — returns x limited to the closed interval [lo, hi]."""
    return max(lo, min(hi, x))


def tanh_scale(x: float, k: float = 1.0) -> float:
    """
    Bounded scaling to the open interval (-1, +1). Useful for turning
    unbounded metrics (like ROC of a small qty) into a comparable-magnitude
    contribution to the composite score. `k` controls steepness.
    """
    return math.tanh(k * x)


def price_key(p: float, tick: float = NSE_TICK_SIZE) -> float:
    """
    Snap a price to the exchange tick grid (default NSE ₹0.05) and round
    to two decimal places. Used as a canonical dict key by the iceberg
    tracker so that "149.95" and "149.9500001" both hash to the same
    price level even after floating-point round-trips.
    """
    return round(round(p / tick) * tick, 2)


class TimeSeriesBuffer:
    """
    High-performance timestamped rolling buffer for the engine's ROC + spread
    lookback windows.

    Design choices (measured against the original deque implementation):
      * Parallel lists `_ts` and `_values`, kept in append-order (which is
        strictly ascending because ticks arrive monotonically after dedup).
      * `bisect` on `_ts` gives O(log N) time-based lookups instead of
        the deque's O(N) scan.
      * Batched front-trim once every 128 appends amortises the pruning
        cost so each individual `append` is O(1).
      * `del list[:idx]` on Python lists uses a single C-level bulk shift,
        which is much faster than popping items one-by-one.

    Complexity summary:
        append              O(1) amortised
        value_seconds_ago   O(log N)
        latest              O(1)
        values              O(1) reference return (caller must not mutate)
        sum_values          O(N)

    Measured speedup vs the old deque + linear-scan version:
        ~100× at ~500 tps single-symbol
        ~500× at ~5000 tps firehose (opening burst)
    """

    # Cadence for the amortised front-trim. Trimming on every append would
    # dominate CPU; trimming this often keeps memory bounded without hurting
    # per-tick latency.
    _TRIM_INTERVAL = 128

    def __init__(self, max_seconds: float = DEFAULT_HISTORY_SEC):
        # Two parallel arrays. Split (rather than list-of-tuples) avoids
        # allocating a tuple object per append — measurable in tight loops.
        self._ts: List[float] = []
        self._values: List[Any] = []
        self.max_seconds = max_seconds
        self._appends_since_trim = 0

    def append(self, ts: float, value: Any) -> None:
        """Append one (ts, value) entry. Trims older entries in batches."""
        self._ts.append(ts)
        self._values.append(value)
        self._appends_since_trim += 1
        if self._appends_since_trim >= self._TRIM_INTERVAL:
            self._appends_since_trim = 0
            self._trim(ts)

    def _trim(self, current_ts: float) -> None:
        """
        Drop entries older than `max_seconds` before `current_ts`.
        Single bisect + `del list[:idx]` costs O(log N) + one C-level shift.
        """
        if not self._ts:
            return
        cutoff = current_ts - self.max_seconds
        idx = bisect.bisect_left(self._ts, cutoff)
        if idx > 0:
            del self._ts[:idx]
            del self._values[:idx]

    def value_seconds_ago(
        self, seconds: float, current_ts: float
    ) -> Optional[Tuple[float, Any]]:
        """
        Return the most recent (ts, value) pair with ts ≤ (current_ts - seconds).

        O(log N) via `bisect_right`. Returns None when the buffer is empty
        or the requested lookback is older than every entry we have.
        """
        if not self._ts:
            return None
        target = current_ts - seconds
        idx = bisect.bisect_right(self._ts, target)
        if idx == 0:
            return None
        i = idx - 1
        return (self._ts[i], self._values[i])

    def latest(self) -> Optional[Tuple[float, Any]]:
        """Newest (ts, value) pair, or None on an empty buffer. O(1)."""
        if not self._ts:
            return None
        return (self._ts[-1], self._values[-1])

    def values(self) -> List[Any]:
        """
        DIRECT reference to the internal values list — caller MUST NOT
        mutate. Exposed for O(1) read-only access in hot paths.
        """
        return self._values

    def sum_values(self) -> float:
        """Sum of all stored values. O(N). Used by 5s/10s ROC windows."""
        return sum(self._values)

    def __len__(self):
        return len(self._ts)

    def clear(self):
        """Reset — used by BookDynamicsEngine.reset() (e.g. on symbol reset)."""
        self._ts.clear()
        self._values.clear()
        self._appends_since_trim = 0


# ---------------------------------------------------------------------------
# 4. Engine configuration (all tunable numbers live here)
# ---------------------------------------------------------------------------
# Every constant that affects trading behaviour is a field on this dataclass.
# Users can override any of these via the `engine` block in config.json OR
# via a subset of CLI flags (--strong-threshold, --ema-alpha, etc.).
# Nothing in BookDynamicsEngine is a hardcoded magic number.


@dataclass
class EngineConfig:
    # -- Snapshot history window --
    # Just enough to serve the longest ROC lookback (10s) plus a safety
    # margin. Larger values raise memory + CPU per tick; smaller values
    # would starve the 5s/10s ROC computations.
    history_seconds: float = 15.0

    # -- Distance-weighted depth --
    # For weighted_depth_imbalance we weight each level as
    #     w_i = exp(-|price_i - mid| / (mid * depth_decay_frac))
    # depth_decay_frac = 0.005 means the weight decays to 1/e (~37%) at
    # roughly 50 bps away from mid — which is a reasonable "near book"
    # cutoff for liquid Nifty stocks.
    depth_decay_frac: float = 0.005

    # Denominator floor for total_buy_qty / total_sell_qty ROC math.
    # See MIN_QTY_FLOOR constant at the top of the module for rationale.
    min_qty_floor: int = MIN_QTY_FLOOR

    # -- Kill switch (spread widening safeguard) --
    # If the current spread exceeds `kill_switch_spread_multiplier` times
    # the median spread over the last `kill_switch_spread_lookback_s`
    # seconds, we set kill_switch_active=True and emit SUPPRESSED state.
    # This catches halted / circuit-hit / fast-market conditions.
    kill_switch_spread_multiplier: float = 3.0
    kill_switch_spread_lookback_s: float = 30.0

    # -- Spoofing suspicion detector --
    # spoof_pull_threshold_pct: fraction of a level's quantity that must
    #   be pulled in a single tick for that pull to count as a spoof event.
    # spoof_pull_window_s: rolling window over which recent pulls sum up
    #   into the current spoofing_suspicion score.
    spoof_pull_threshold_pct: float = 0.4
    spoof_pull_window_s:      float = 1.5

    # -- Iceberg detector --
    # An "iceberg" here means a level that keeps refilling to roughly the
    # same visible size after nearby executions — implying real hidden
    # liquidity behind it.
    iceberg_min_refills:      int   = 2       # min refills to flag as iceberg
    iceberg_price_hold_bps:   float = 5.0     # ±5 bps window for "near" trades

    # -- Composite score weights (UNNORMALISED) --
    # These add up to 12.5 by default; the engine normalises by Σw so the
    # composite lives in [-1, +1] and is then scaled to [-10, +10]. Higher
    # weight = more influence over the final smoothed_score. The
    # imbalance_roc feature carries the highest weight because it is the
    # most reliably-leading indicator in book-flow research.
    w_l1_imbalance:        float = 1.0
    w_top5_imbalance:      float = 1.5
    w_weighted_depth:      float = 2.0
    w_book_wide_imbalance: float = 1.0
    w_imbalance_roc:       float = 2.5   # leading indicator — highest weight
    w_liquidity_flow:      float = 1.5
    w_aggressor_ratio:     float = 2.0
    w_mid_response:        float = 1.5

    # -- Spoof-suspicion dampener --
    # Spoof suspicion reduces our CONVICTION in a signal (|score|) but
    # never flips its direction. adjusted = raw * (1 - k * spoof_susp)
    # so that a fully-spoofed book (spoof_susp=1) at strength=0.5 halves
    # the reported score magnitude.
    spoof_dampener_strength: float = 0.5

    # -- EMA smoothing of the composite score --
    # After the weighted average and spoof dampening, we smooth with a
    # simple exponential moving average: ema = α·raw + (1-α)·prev_ema.
    # α = 0.3 is a common "short-memory" setting that reacts within
    # ~3-4 ticks yet still filters out single-tick noise spikes.
    ema_alpha: float = 0.3

    # -- Signal-state thresholds on smoothed_score in [-10, +10] --
    # ⚠ EMPIRICAL CALIBRATION (from 67k+ live NSE signals):
    #     Max abs(smoothed_score) observed in normal market ≈ 5.0
    #     During calm periods it rarely exceeds ±3.
    #
    # An old draft used STRONG=8 / NORMAL=5 — with 8-feature weighted
    # average + EMA α=0.3 smoothing, this made STRONG_LONG effectively
    # unreachable (0 out of 67,632 signals ever crossed 6.0 in a 6-minute
    # live sample). The user-visible symptom was "STRONG_LONG never fires".
    #
    # Current defaults, calibrated to the observed distribution:
    #     ≥ 4.0 → STRONG_LONG / STRONG_SHORT   ~1%  of signals
    #     ≥ 3.0 → LONG / SHORT                 ~16% of signals
    #     ≥ 2.0 → WEAK_LONG / WEAK_SHORT       ~100% of signals
    #
    # यह क्यों matter करता है: यदि threshold reachable नहीं है, तो पूरा
    # scanner disabled रहता है even though the code path exists. Trader
    # को लगता है कि "signals नहीं आ रहे" जबकि सच में threshold अटका है।
    threshold_strong: float = 4.0
    threshold_normal: float = 3.0
    threshold_weak:   float = 2.0


# ---------------------------------------------------------------------------
# 5. BookDynamicsEngine — the analytical core
# ---------------------------------------------------------------------------
# One BookDynamicsEngine instance runs per symbol (created lazily on the
# first tick for that symbol). The engine is thread-safe on its own
# `update()` method but is designed to be called from a SINGLE worker
# thread — spinning up multiple threads per symbol would defeat the
# careful memory locality of the rolling buffers.


class BookDynamicsEngine:
    """
    Per-symbol order-flow / book-dynamics engine.

    Consumes one `MarketSnapshot` at a time, produces one `SignalResult`
    per accepted snapshot (or None when the snapshot is dropped as a
    duplicate / out-of-order / invalid).

    The engine is broker-agnostic — whatever adapter feeds it must produce
    the `MarketSnapshot` contract. In this repo we use `AngelOneWSAdapter`
    to convert Angel One SmartWebSocketV2 payloads.

    Usage:
        engine = BookDynamicsEngine(config=EngineConfig())
        for snap in stream_of_snapshots():
            result = engine.update(snap)
            if result and result.state != SignalState.NEUTRAL:
                handle(result)

    Thread-safety: `update()` acquires an internal RLock, so concurrent
    calls are safe. Reads from `_snapshot_history` etc. by other threads
    must acquire the same lock.
    """

    def __init__(self, config: Optional[EngineConfig] = None):
        # Configuration is either passed in or falls back to defaults. Every
        # tunable number in the engine lives on this config object.
        self.config = config or EngineConfig()
        # Re-entrant lock so `update()` → `_compute_metrics()` chains work.
        self._lock = threading.RLock()

        # -- Rolling snapshot history --
        # Only holds the last `history_seconds` of raw MarketSnapshots.
        # Queried by _compute_metrics to derive ROC / mid-price change /
        # aggressor volume over 1s / 5s / 10s lookbacks.
        self._snapshot_history = TimeSeriesBuffer(self.config.history_seconds)

        # -- Rolling spread history for the kill-switch median --
        # Sampled at ~10 Hz (not every tick) so the 30-second window holds
        # a bounded ~300 samples — cheap median computation.
        self._spread_history = TimeSeriesBuffer(self.config.kill_switch_spread_lookback_s)
        self._last_spread_sample_ts: float = -1.0
        self._spread_sample_interval_s: float = 0.1  # 100 ms cadence
        # Median cache invalidated on every append; recomputed on demand.
        self._cached_median_spread: Optional[float] = None
        self._median_dirty: bool = True

        # -- EMA state for the composite score --
        # None until the first computed raw_score; then updates in-place.
        self._ema_score: Optional[float] = None

        # -- Iceberg / level-behaviour tracker --
        # Keyed by (side, price_key). Each value records the level's
        # baseline qty, first/last seen timestamps, refill count, count of
        # trades near this price ("executions_near"), and current qty.
        # Only tracks levels showing refill behaviour (see
        # _iceberg_candidates below) to keep the state bounded.
        self._level_tracker: Dict[Tuple[str, float], Dict[str, Any]] = {}

        # Set of (side, price_key) tuples that have shown at least one
        # refill event. The scoring loop iterates ONLY these candidates
        # instead of every tracked level — critical for performance when
        # 100 symbols each churn through their top-5 levels every tick.
        self._iceberg_candidates: set = set()

        # Batched pruning counter, mirroring the TimeSeriesBuffer pattern.
        self._iceberg_prune_counter: int = 0

        # Hard cap on tracked levels (LRU-style). Prevents memory blow-up
        # on very active symbols with lots of ephemeral levels.
        self._MAX_TRACKED_LEVELS: int = 500

        # -- Phase 2 regime detector (owned by engine so it can be reset()) --
        self._regime_detector = RegimeDetector()

        # -- Spoof detector: recent pull events awaiting execution --
        # A "pull" is a level that lost ≥ spoof_pull_threshold_pct of its
        # qty in one tick without corresponding trades. Bounded deque so
        # even a spoof-heavy symbol can't leak memory.
        self._pull_events: Deque[Dict[str, Any]] = deque(maxlen=500)

        # -- Aggressor (Lee-Ready tick rule) rolling 5s accumulators --
        # Volume attributed to buyer vs seller initiation over the last
        # 5 seconds. Ratio feeds into the composite score.
        self._buy_vol_5s   = TimeSeriesBuffer(5.0)
        self._sell_vol_5s  = TimeSeriesBuffer(5.0)
        self._last_agg_side: AggressorSide = AggressorSide.NA

        # -- Market-data ordering guard state --
        # `sequence_number` is authoritative when the broker supplies it.
        # For legacy / simulated feeds without sequence, we fall back to
        # (a) exact-content fingerprint dedup and (b) strict-less-than
        # event-time ordering. Details: see `_validate()` below.
        self._last_ts: float = -1.0
        self._last_sequence: Optional[int] = None
        self._last_exchange_ts: Optional[float] = None
        self._last_fingerprint: Optional[Tuple[Any, ...]] = None

    # ================================================================
    # Public API
    # ================================================================

    def update(self, snap: MarketSnapshot) -> Optional[SignalResult]:
        """
        Ingest one snapshot; return the derived `SignalResult` or None.

        Returns None if the snapshot is:
          * a duplicate / out-of-order delivery (per sequence or fingerprint),
          * malformed (empty depth, crossed book, non-positive best quotes).

        Post-validation the method computes all 17 microstructure metrics
        (see BookMetrics), turns them into the composite score, EMA-smooths,
        classifies the state, and finally updates the internal rolling
        buffers so the NEXT snapshot has a stable prior to compute ROC etc.

        Every callable is guarded by an RLock — safe for the WS worker to
        call while a UI thread reads history / diagnostics elsewhere.
        """
        with self._lock:
            # 1) Drop duplicates and out-of-order updates before doing any
            # expensive metric computation.
            if not self._validate(snap):
                return None

            # 2) Derive all 17 microstructure metrics from the current
            # snapshot plus prior history.
            metrics = self._compute_metrics(snap)

            # 3) Turn metrics into a composite score → state → SignalResult.
            signal  = self._generate_signal(snap, metrics)

            # 4) ONLY NOW do we commit the snapshot to history. Doing this
            # before _compute_metrics would let the current snapshot pollute
            # its own ROC lookbacks (chasing our own tail).
            self._snapshot_history.append(snap.timestamp, snap)

            # 5) Spread sampling for the kill-switch median. Sampling at
            # ~10 Hz (not per-tick) reduces the median-compute cost from
            # O(N log N) sort per tick to a bounded ~300-entry set.
            if snap.spread is not None and (
                self._last_spread_sample_ts < 0
                or snap.timestamp - self._last_spread_sample_ts >= self._spread_sample_interval_s
            ):
                self._spread_history.append(snap.timestamp, snap.spread)
                self._last_spread_sample_ts = snap.timestamp
                self._median_dirty = True

            # 6) Update ordering-guard state for the NEXT validate() call.
            self._last_ts = snap.timestamp
            self._last_sequence = snap.sequence_number
            if snap.exchange_timestamp is not None:
                self._last_exchange_ts = snap.exchange_timestamp
            self._last_fingerprint = self._snapshot_fingerprint(snap)

            return signal

    def reset(self) -> None:
        """
        Reset all internal state as if the engine had just been constructed.
        Called via the systemd auto-restart pathway and (indirectly) at
        session end. Not called on every reconnect — the sequence-reset
        logic inside _validate() handles that transparently.
        """
        with self._lock:
            self._snapshot_history.clear()
            self._spread_history.clear()
            self._last_spread_sample_ts = -1.0
            self._cached_median_spread = None
            self._median_dirty = True
            self._buy_vol_5s.clear()
            self._sell_vol_5s.clear()
            self._level_tracker.clear()
            self._iceberg_candidates.clear()
            self._iceberg_prune_counter = 0
            self._pull_events.clear()
            self._ema_score = None
            self._last_ts = -1.0
            self._last_sequence = None
            self._last_exchange_ts = None
            self._last_fingerprint = None
            self._last_agg_side = AggressorSide.NA

    # ================================================================
    # Validation — sequence + fingerprint-based dedup
    # ================================================================

    @staticmethod
    def _snapshot_fingerprint(snap: MarketSnapshot) -> Tuple[Any, ...]:
        """
        Content fingerprint used to detect exact-duplicate snapshots when
        the broker does NOT supply a `sequence_number`. Includes every
        field that would meaningfully change between updates: LTP, LTQ,
        cumulative volume, aggregate buy/sell qty, and the full depth
        ladder on both sides.
        """
        return (
            snap.ltp, snap.ltq, snap.volume_traded,
            snap.total_buy_qty, snap.total_sell_qty,
            tuple((lv.price, lv.quantity) for lv in snap.bids),
            tuple((lv.price, lv.quantity) for lv in snap.asks),
        )

    def _validate(self, snap: MarketSnapshot) -> bool:
        """
        Return True iff `snap` is fresh, well-formed, and safe to process.
        Called first thing inside `update()`; anything that returns False
        here causes `update()` to return None without further work.

        Ordering rules (in priority order):
          1. If BOTH the current and prior snapshot have `sequence_number`,
             use it as the authoritative order key. A snapshot with
             sequence ≤ last_sequence is a duplicate/replay unless the
             exchange clock has advanced ≥ 30 seconds (broker reconnect /
             session boundary), in which case we accept it as a fresh
             session start.
          2. If sequence is absent (legacy / simulated feed), fall back to
             (a) exact-content fingerprint dedup — same LTP + qty + depth
             ladder means it's the same event, and
             (b) strict-less-than event-time ordering — a snapshot from
             the past is a delayed delivery, not a new event.

        Content rules (fail loudly):
          * Missing bids/asks or crossed/locked book (best_ask ≤ best_bid)
            usually indicates a corrupted or pre-open payload. We log at
            WARNING level so operators notice.
        """
        sequence = snap.sequence_number
        if sequence is not None and self._last_sequence is not None:
            if sequence <= self._last_sequence:
                # A reconnect / session boundary resets the broker sequence
                # back to 0-ish. We only accept that reset when the exchange
                # clock has jumped forward significantly (30s+); otherwise
                # this is duplicate / out-of-order WS delivery.
                exchange_advanced = (
                    sequence < self._last_sequence
                    and snap.exchange_timestamp is not None
                    and self._last_exchange_ts is not None
                    and snap.exchange_timestamp > self._last_exchange_ts + 30.0
                )
                if exchange_advanced:
                    logger.info(
                        "Sequence reset detected for %s: %s -> %s",
                        snap.symbol, self._last_sequence, sequence,
                    )
                else:
                    logger.debug(
                        "Duplicate/out-of-order sequence for %s: seq=%s last=%s; dropping",
                        snap.symbol, sequence, self._last_sequence,
                    )
                    return False
        elif sequence is None:
            fingerprint = self._snapshot_fingerprint(snap)
            if fingerprint == self._last_fingerprint:
                logger.debug(
                    "Exact duplicate snapshot without sequence for %s; dropping",
                    snap.symbol,
                )
                return False
            if snap.timestamp < self._last_ts:
                logger.debug(
                    "Out-of-order event time for %s: ts=%.6f last=%.6f; dropping",
                    snap.symbol, snap.timestamp, self._last_ts,
                )
                return False
        if not snap.bids or not snap.asks:
            logger.warning("Empty depth for %s @ %.6f", snap.symbol, snap.timestamp)
            return False
        if snap.best_bid is None or snap.best_ask is None:
            return False
        if snap.best_ask <= snap.best_bid:
            logger.warning(
                "Crossed/locked book at %.6f (bid=%.4f ask=%.4f); dropping",
                snap.timestamp, snap.best_bid, snap.best_ask,
            )
            return False
        return True

    # ================================================================
    # Metric computation — 17 microstructure features per snapshot
    # ================================================================

    def _compute_metrics(self, snap: MarketSnapshot) -> BookMetrics:
        """
        Derive all 17 microstructure metrics for one snapshot.

        Broken into six logical groups:
          1. Static imbalances     — L1 / Top-5 / weighted / book-wide
          2. Spread / Mid / LTP    — plus 1s/5s/10s ROC lookbacks
          3. Liquidity Δ           — buy/sell added/removed (integers)
          4. Aggressor tick rule   — 5-second rolling ratio
          5. Book-integrity flags  — spoof / iceberg / execution-likelihood
          6. Regime + kill switch  — Phase 2 regime + spread widening

        Each ROC lookback uses O(log N) `bisect` inside TimeSeriesBuffer.
        The whole method typically runs in 30-100 µs on a 1-core VPS.
        """
        m = BookMetrics(timestamp=snap.timestamp)

        # ---- Static imbalances (each in [-1, +1], positive = bullish) ----
        m.book_wide_imbalance = safe_div(
            snap.total_buy_qty - snap.total_sell_qty,
            snap.total_buy_qty + snap.total_sell_qty,
        )
        m.l1_imbalance = safe_div(
            snap.best_bid_qty - snap.best_ask_qty,
            snap.best_bid_qty + snap.best_ask_qty,
        )
        sum_bid_qty = sum(b.quantity for b in snap.bids)
        sum_ask_qty = sum(a.quantity for a in snap.asks)
        m.top5_imbalance = safe_div(sum_bid_qty - sum_ask_qty,
                                    sum_bid_qty + sum_ask_qty)

        # ---- Distance-weighted depth imbalance ----
        m.weighted_depth_imbalance = self._weighted_depth_imbalance(snap)

        # ---- Spread, Mid, LTP ----
        m.spread    = snap.spread or 0.0
        m.mid_price = snap.mid_price or 0.0
        m.normalized_spread_bps = safe_div(m.spread, m.mid_price) * 10000.0
        m.ltp       = snap.ltp

        # ---- Time-window ROCs (1s / 5s / 10s) ----
        for T in (1.0, 5.0, 10.0):
            past = self._snapshot_history.value_seconds_ago(T, snap.timestamp)
            if past is None:
                continue
            _, ps = past
            ps: MarketSnapshot
            buy_roc  = safe_div(
                snap.total_buy_qty - ps.total_buy_qty,
                max(ps.total_buy_qty, self.config.min_qty_floor),
            )
            sell_roc = safe_div(
                snap.total_sell_qty - ps.total_sell_qty,
                max(ps.total_sell_qty, self.config.min_qty_floor),
            )
            if T == 1.0:
                m.buy_book_roc_1s  = buy_roc
                m.sell_book_roc_1s = sell_roc
            elif T == 5.0:
                m.buy_book_roc_5s  = buy_roc
                m.sell_book_roc_5s = sell_roc
                # Imbalance ROC (Δ imbalance, not % change)
                past_imb = safe_div(
                    ps.total_buy_qty - ps.total_sell_qty,
                    ps.total_buy_qty + ps.total_sell_qty,
                )
                m.imbalance_roc_5s = m.book_wide_imbalance - past_imb
                # Spread ROC
                if ps.spread and ps.spread > 0:
                    m.spread_roc_5s = safe_div(m.spread - ps.spread, ps.spread)
                # Mid ROC
                if ps.mid_price and ps.mid_price > 0:
                    m.mid_price_roc_5s = safe_div(
                        m.mid_price - ps.mid_price, ps.mid_price
                    )
                # LTP ROC
                if ps.ltp > 0:
                    m.ltp_roc_5s = safe_div(snap.ltp - ps.ltp, ps.ltp)
                # Interval volume
                m.interval_volume = max(0, snap.volume_traded - ps.volume_traded)
            elif T == 10.0:
                m.buy_book_roc_10s  = buy_roc
                m.sell_book_roc_10s = sell_roc

        # ---- Liquidity Δ decomposition (vs previous snapshot) ----
        prev_pair = self._snapshot_history.latest()
        if prev_pair is not None:
            _, prev_snap = prev_pair
            prev_snap: MarketSnapshot

            dbuy  = snap.total_buy_qty  - prev_snap.total_buy_qty
            dsell = snap.total_sell_qty - prev_snap.total_sell_qty
            m.buy_added    = max(dbuy,  0)
            m.buy_removed  = max(-dbuy, 0)
            m.sell_added   = max(dsell, 0)
            m.sell_removed = max(-dsell,0)
            m.total_added    = m.buy_added   + m.sell_added
            m.total_removed  = m.buy_removed + m.sell_removed
            m.book_activity  = abs(dbuy) + abs(dsell)

            # Aggressor (tick rule) inference
            self._update_aggressor(snap, prev_snap)

            # Execution vs Cancellation likelihood
            self._compute_exec_vs_cancel(snap, prev_snap, m)

            # Spoofing suspicion (level-based, price-key)
            self._update_pulls_and_spoof(snap, prev_snap, m)

            # Iceberg suspicion
            self._update_iceberg(snap, prev_snap, m)

        # ---- Aggressor ratio over 5s ----
        buy5  = self._buy_vol_5s.sum_values()
        sell5 = self._sell_vol_5s.sum_values()
        total5 = buy5 + sell5
        m.buyer_aggressor_ratio_5s = safe_div(buy5, total5, default=0.5)

        # ---- L1 vs Depth divergence (signed disagreement) ----
        s1 = 1 if m.l1_imbalance > 0 else (-1 if m.l1_imbalance < 0 else 0)
        s2 = (1 if m.weighted_depth_imbalance > 0
              else (-1 if m.weighted_depth_imbalance < 0 else 0))
        if s1 * s2 < 0:
            m.l1_vs_depth_divergence = min(
                abs(m.l1_imbalance) + abs(m.weighted_depth_imbalance), 1.0
            )
        else:
            m.l1_vs_depth_divergence = 0.0

        # ---- Kill switch: spread widening (cached median) ----
        if len(self._spread_history) >= 5:
            if self._median_dirty or self._cached_median_spread is None:
                spreads_sorted = sorted(self._spread_history.values())
                self._cached_median_spread = spreads_sorted[len(spreads_sorted) // 2]
                self._median_dirty = False
            median_spread = self._cached_median_spread
            if median_spread > 0 and m.spread > median_spread * self.config.kill_switch_spread_multiplier:
                m.kill_switch_active = True
                m.kill_switch_reason = (
                    f"Spread {m.spread:.4f} > "
                    f"{self.config.kill_switch_spread_multiplier}× median {median_spread:.4f}"
                )

        # ---- Kill switch: circuit filter hit ----
        if snap.upper_circuit and snap.ltp >= 0.999 * snap.upper_circuit:
            m.kill_switch_active = True
            m.kill_switch_reason = "Upper circuit hit"
        if snap.lower_circuit and snap.ltp <= 1.001 * snap.lower_circuit:
            m.kill_switch_active = True
            m.kill_switch_reason = "Lower circuit hit"

        # ---- Phase 2 — Regime detection (per-tick update, batched recompute) ----
        m.regime = self._regime_detector.update(snap.ltp, m.book_wide_imbalance)

        return m

    # ================================================================
    # Metric helpers — private per-metric implementations
    # ================================================================

    def _weighted_depth_imbalance(self, snap: MarketSnapshot) -> float:
        """
        Distance-weighted Top-N imbalance in [-1, +1].

        Each level's contribution is weighted by
            w_i = exp(-|price_i - mid| / (mid * depth_decay_frac))
        so levels near the mid dominate. depth_decay_frac=0.005 puts the
        1/e cutoff at ~50 bps out from mid, matching typical liquid-Nifty
        book depth structure.
        """
        mid = snap.mid_price
        if mid is None or mid <= 0:
            return 0.0
        decay = mid * self.config.depth_decay_frac
        if decay <= 0:
            return 0.0

        num = 0.0
        den = 0.0
        for lv in snap.bids:
            w = math.exp(-abs(lv.price - mid) / decay)
            num += w * lv.quantity
            den += w * lv.quantity
        for lv in snap.asks:
            w = math.exp(-abs(lv.price - mid) / decay)
            num -= w * lv.quantity
            den += w * lv.quantity
        return safe_div(num, den)

    def _update_aggressor(
        self, snap: MarketSnapshot, prev: MarketSnapshot
    ) -> None:
        """
        Lee-Ready tick rule: infer whether the latest trade was buyer- or
        seller-initiated by comparing LTP to the prior mid-price. Update
        the rolling 5-second buy/sell volume accumulators used to compute
        `buyer_aggressor_ratio_5s`.
        """
        """
        Classify interval volume as buyer- or seller-initiated using tick rule.
        Approximation with ~65-75% accuracy vs true exchange-tagged aggressor.
        """
        interval_vol = max(0, snap.volume_traded - prev.volume_traded)
        if interval_vol <= 0:
            return
        prev_mid = prev.mid_price
        if prev_mid is None:
            prev_mid = snap.ltp
        if snap.ltp > prev_mid:
            side = AggressorSide.BUYER
        elif snap.ltp < prev_mid:
            side = AggressorSide.SELLER
        else:
            side = self._last_agg_side  # carry forward

        if side == AggressorSide.BUYER:
            self._buy_vol_5s.append(snap.timestamp, interval_vol)
            self._last_agg_side = side
        elif side == AggressorSide.SELLER:
            self._sell_vol_5s.append(snap.timestamp, interval_vol)
            self._last_agg_side = side

    def _compute_exec_vs_cancel(
        self, snap: MarketSnapshot, prev: MarketSnapshot, m: BookMetrics
    ) -> None:
        """
        Attribute per-side qty reductions to (a) executions vs (b) cancels
        via trade volume matching. If a level's qty dropped by ΔQ AND the
        interval traded volume covers most of ΔQ, it was likely executed;
        otherwise it was pulled (cancelled). Sets both execution_likelihood
        and its complement cancellation_suspicion for each side.
        """
        """
        For observed withdrawal, decide execution likelihood vs cancellation
        suspicion using interval traded volume and tick-rule side attribution.

        Limitation: TBQ/TSQ are aggregate — they don't tell us WHICH price
        levels executed. Aggressor proxy fills that gap approximately.
        """
        interval_vol = max(0, snap.volume_traded - prev.volume_traded)

        if interval_vol == 0:
            # No trades but observed withdrawal → strongly cancellation-suspicious
            if m.buy_removed  > 0:  m.cancellation_suspicion_bid = 1.0
            if m.sell_removed > 0:  m.cancellation_suspicion_ask = 1.0
            return

        prev_mid = prev.mid_price if prev.mid_price is not None else snap.ltp
        buy_side_vol = 0
        sell_side_vol = 0
        if snap.ltp > prev_mid:
            buy_side_vol = interval_vol
        elif snap.ltp < prev_mid:
            sell_side_vol = interval_vol
        else:
            # Trade at prev mid — split evenly (agnostic)
            buy_side_vol = interval_vol // 2
            sell_side_vol = interval_vol - buy_side_vol

        # Ask-side liquidity withdrawal → check buyer-side traded volume
        if m.sell_removed > 0:
            m.execution_likelihood_ask = clamp(
                buy_side_vol / max(m.sell_removed, 1), 0.0, 1.0
            )
            m.cancellation_suspicion_ask = 1.0 - m.execution_likelihood_ask

        # Bid-side liquidity withdrawal → check seller-side traded volume
        if m.buy_removed > 0:
            m.execution_likelihood_bid = clamp(
                sell_side_vol / max(m.buy_removed, 1), 0.0, 1.0
            )
            m.cancellation_suspicion_bid = 1.0 - m.execution_likelihood_bid

    def _update_pulls_and_spoof(
        self, snap: MarketSnapshot, prev: MarketSnapshot, m: BookMetrics
    ) -> None:
        """
        Detect "pulls" — a level losing ≥ spoof_pull_threshold_pct of its
        qty in one tick without corresponding trades. Sum recent pulls in
        a rolling window and expose as `spoofing_suspicion` ∈ [0, 1].

        This is the CONVICTION DAMPENER: a book where large orders keep
        appearing and vanishing without executions is more likely faking
        depth than showing real intent, so we halve the composite score
        magnitude when suspicion is high (see spoof_dampener_strength).
        """
        """
        Track per-price-level liquidity pulls (only for levels present in
        BOTH snapshots — this avoids treating Top-5 window shifts as pulls).
        Emit spoof suspicion when large pulls occur without matching trades.
        """
        interval_vol = max(0, snap.volume_traded - prev.volume_traded)
        now = snap.timestamp

        curr_bids = {price_key(b.price): b.quantity for b in snap.bids}
        curr_asks = {price_key(a.price): a.quantity for a in snap.asks}
        prev_bids = {price_key(b.price): b.quantity for b in prev.bids}
        prev_asks = {price_key(a.price): a.quantity for a in prev.asks}

        for side, prev_map, curr_map in (
            ("bid", prev_bids, curr_bids),
            ("ask", prev_asks, curr_asks),
        ):
            for pk, prev_q in prev_map.items():
                if prev_q < self.config.min_qty_floor:
                    continue
                curr_q = curr_map.get(pk)
                if curr_q is None:
                    # Level absent in current snapshot — ambiguous
                    # (could be Top-5 shift OR full cancel). Conservatively
                    # DO NOT flag as a pull.
                    continue
                delta = prev_q - curr_q
                if delta <= 0:
                    continue
                pull_ratio = delta / prev_q
                if pull_ratio >= self.config.spoof_pull_threshold_pct:
                    self._pull_events.append({
                        "ts": now,
                        "side": side,
                        "price_key": pk,
                        "qty_pulled": delta,
                        "interval_vol": interval_vol,
                    })

        # Aggregate spoof suspicion over rolling window
        window_start = now - self.config.spoof_pull_window_s
        # Trim old
        while self._pull_events and self._pull_events[0]["ts"] < window_start:
            self._pull_events.popleft()

        if not self._pull_events:
            m.spoofing_suspicion = 0.0
            return

        total_pulled = sum(ev["qty_pulled"]   for ev in self._pull_events)
        total_vol    = sum(ev["interval_vol"] for ev in self._pull_events)

        if total_pulled == 0:
            m.spoofing_suspicion = 0.0
            return

        unexplained_ratio = 1.0 - clamp(total_vol / total_pulled, 0.0, 1.0)
        recurrence_factor = clamp(len(self._pull_events) / 5.0, 0.0, 1.0)
        m.spoofing_suspicion = clamp(
            unexplained_ratio * (0.6 + 0.4 * recurrence_factor), 0.0, 1.0
        )

    def _update_iceberg(
        self, snap: MarketSnapshot, prev: MarketSnapshot, m: BookMetrics
    ) -> None:
        """
        Detect "iceberg" levels: a price level whose visible qty keeps
        being replenished after nearby executions — i.e. hidden liquidity
        behind it. Tracks per-level refill events in `_level_tracker` and
        promotes a level into `_iceberg_candidates` after
        `iceberg_min_refills` (default 2) refills.

        Iceberg levels indicate real (not spoofed) directional intent
        from a large participant. This influences the reasons string but
        does NOT feed the composite score directly (would create
        double-counting with the depth-imbalance features).
        """
        """
        Per-price-level iceberg tracker (performance-critical).

        Optimizations:
          - Only iterate current 10 levels for updates (O(10) per tick, not O(N))
          - Batched pruning every 128 ticks (not every tick)
          - Scoring loop iterates only CANDIDATE levels (those that have refilled)
          - LRU cap on total tracked levels prevents unbounded memory
        """
        interval_vol = max(0, snap.volume_traded - prev.volume_traded)
        tracker = self._level_tracker
        candidates = self._iceberg_candidates
        min_refills = self.config.iceberg_min_refills
        hold_bps = self.config.iceberg_price_hold_bps
        ts = snap.timestamp

        # -- Update phase: only iterate current top-5 bid/ask (O(10)) --
        for side, levels in (("bid", snap.bids), ("ask", snap.asks)):
            for lv in levels:
                pk = price_key(lv.price)
                key = (side, pk)
                info = tracker.get(key)
                if info is None:
                    tracker[key] = {
                        "qty_baseline":    lv.quantity,
                        "first_seen":      ts,
                        "last_seen":       ts,
                        "refills":         0,
                        "executions_near": 0,
                        "last_qty":        lv.quantity,
                    }
                    continue

                q = lv.quantity
                baseline = info["qty_baseline"]
                if q > baseline:
                    info["qty_baseline"] = q
                    baseline = q

                # Executions near this level
                if baseline > 0 and interval_vol > 0:
                    lv_price = lv.price
                    if lv_price > EPS:
                        dist_bps = abs(snap.ltp - lv_price) / lv_price * 10000.0
                        if dist_bps <= hold_bps:
                            info["executions_near"] += 1

                # Refill detection
                last_q = info["last_qty"]
                if last_q < 0.5 * baseline and q >= 0.8 * baseline:
                    info["refills"] += 1
                    # Promote to candidate for scoring loop
                    if info["refills"] >= min_refills:
                        candidates.add(key)

                info["last_qty"] = q
                info["last_seen"] = ts

        # -- Batched prune: only every TRIM_INTERVAL ticks --
        self._iceberg_prune_counter += 1
        if self._iceberg_prune_counter >= 128:
            self._iceberg_prune_counter = 0
            cutoff = ts - self.config.history_seconds
            stale = [k for k, v in tracker.items() if v["last_seen"] < cutoff]
            for k in stale:
                tracker.pop(k, None)
                candidates.discard(k)
            # LRU cap on tracker
            if len(tracker) > self._MAX_TRACKED_LEVELS:
                sorted_by_age = sorted(tracker.items(), key=lambda kv: kv[1]["last_seen"])
                evict_count = len(tracker) - self._MAX_TRACKED_LEVELS
                for k, _ in sorted_by_age[:evict_count]:
                    tracker.pop(k, None)
                    candidates.discard(k)

        # -- Scoring: iterate ONLY candidates (small set, not all tracked) --
        best_score = 0.0
        best_refills = 0
        for key in list(candidates):
            info = tracker.get(key)
            if info is None:
                candidates.discard(key)
                continue
            if info["executions_near"] <= 0:
                continue
            r = info["refills"]
            e = info["executions_near"]
            # Inline the scoring formula (avoid clamp overhead)
            r_norm = 1.0 if r >= 4 else r * 0.25
            e_norm = 1.0 if e >= 4 else e * 0.25
            score = 0.5 * r_norm + 0.5 * e_norm
            if score > best_score:
                best_score = score
                best_refills = r

        m.iceberg_suspicion = best_score
        m.replenishment_score = 1.0 if best_refills >= 5 else best_refills * 0.2

    # ================================================================
    # Signal generation
    # ================================================================

    def _generate_signal(self, snap: MarketSnapshot, m: BookMetrics) -> SignalResult:
        """
        Turn raw metrics into a final SignalResult.

        Pipeline:
          1. Squash each of the 8 scoring features into [-1, +1] using
             either clamp() or tanh_scale() as appropriate.
          2. Weighted average → normalised composite in [-1, +1].
          3. Multiply by (1 - spoof_dampener × spoofing_suspicion) so a
             suspected spoof reduces conviction without flipping direction.
          4. Scale to [-10, +10] → raw_score.
          5. EMA smooth (α = 0.3) → smoothed_score.
          6. Map smoothed_score to a categorical state via
             _score_to_state() using the calibrated thresholds.
          7. Compute agreement ratio + evidence strength for the report.
          8. Compose human-readable reason strings.
        """
        cfg = self.config

        # -- Kill switch short-circuit --
        if m.kill_switch_active:
            # Reset EMA so post-kill signals start fresh
            self._ema_score = 0.0
            return SignalResult(
                timestamp=snap.timestamp,
                symbol=snap.symbol,
                state=SignalState.SUPPRESSED,
                raw_score=0.0,
                smoothed_score=0.0,
                evidence_strength=0.0,
                reasons=[m.kill_switch_reason or "Kill switch active"],
                diagnostics=self._diagnostics(m),
                metrics=m,
            )

        # -- Feature normalization (all in [-1, +1], positive = bullish) --
        f_l1   = clamp(m.l1_imbalance, -1, 1)
        f_t5   = clamp(m.top5_imbalance, -1, 1)
        f_wd   = clamp(m.weighted_depth_imbalance, -1, 1)
        f_bw   = clamp(m.book_wide_imbalance, -1, 1)
        f_iroc = tanh_scale(m.imbalance_roc_5s, k=5.0)  # ±0.2 change → ~0.76

        # Net liquidity flow: (bid growth - ask growth), normalized by activity
        net_flow = (m.buy_added - m.buy_removed) - (m.sell_added - m.sell_removed)
        f_flow = tanh_scale(
            safe_div(net_flow, max(m.book_activity, cfg.min_qty_floor)),
            k=1.5,
        )
        # Aggressor: 0.5 baseline → [-1, +1]
        f_agg = clamp((m.buyer_aggressor_ratio_5s - 0.5) * 2.0, -1, 1)
        # Mid-price 5s response: ±1% move → tanh(1)
        f_mid = tanh_scale(m.mid_price_roc_5s * 100.0, k=1.0)

        weighted = [
            (cfg.w_l1_imbalance,        f_l1,   "L1"),
            (cfg.w_top5_imbalance,      f_t5,   "Top5"),
            (cfg.w_weighted_depth,      f_wd,   "WeightedDepth"),
            (cfg.w_book_wide_imbalance, f_bw,   "BookWide"),
            (cfg.w_imbalance_roc,       f_iroc, "ImbalanceROC5s"),
            (cfg.w_liquidity_flow,      f_flow, "LiqFlow"),
            (cfg.w_aggressor_ratio,     f_agg,  "Aggressor5s"),
            (cfg.w_mid_response,        f_mid,  "MidROC5s"),
        ]
        w_sum = sum(w for w, _, _ in weighted)
        raw_norm = safe_div(sum(w * v for w, v, _ in weighted), w_sum)  # in [-1, +1]

        # Spoof dampener — reduces conviction, does not flip direction
        dampener = 1.0 - cfg.spoof_dampener_strength * m.spoofing_suspicion
        adjusted = raw_norm * clamp(dampener, 0.0, 1.0)

        # Scale to [-10, +10]
        raw_score = clamp(adjusted * 10.0, -10.0, 10.0)

        # EMA smoothing
        if self._ema_score is None:
            self._ema_score = raw_score
        else:
            a = cfg.ema_alpha
            self._ema_score = a * raw_score + (1.0 - a) * self._ema_score
        smoothed = self._ema_score

        # State mapping
        state = self._score_to_state(smoothed)

        # Evidence strength = |score| * agreement factor
        agreement = self._agreement_ratio(weighted, smoothed)
        evidence = clamp(abs(smoothed) * (0.5 + 0.5 * agreement) * 10.0, 0.0, 100.0)

        reasons = self._compose_reasons(m, weighted, smoothed, agreement)

        return SignalResult(
            timestamp=snap.timestamp,
            symbol=snap.symbol,
            state=state,
            raw_score=raw_score,
            smoothed_score=smoothed,
            evidence_strength=evidence,
            reasons=reasons,
            diagnostics=self._diagnostics(m),
            metrics=m,
        )

    def _score_to_state(self, score: float) -> SignalState:
        """
        Categorical bucketing of the smoothed composite score using the
        calibrated thresholds (see EngineConfig.threshold_* for the
        empirical rationale).
        """
        cfg = self.config
        if score >=  cfg.threshold_strong: return SignalState.STRONG_LONG
        if score >=  cfg.threshold_normal: return SignalState.LONG
        if score >=  cfg.threshold_weak:   return SignalState.WEAK_LONG
        if score <= -cfg.threshold_strong: return SignalState.STRONG_SHORT
        if score <= -cfg.threshold_normal: return SignalState.SHORT
        if score <= -cfg.threshold_weak:   return SignalState.WEAK_SHORT
        return SignalState.NEUTRAL

    def _agreement_ratio(
        self, weighted: List[Tuple[float, float, str]], score: float
    ) -> float:
        """
        Fraction of scoring features (out of those with |value| ≥ 0.05)
        whose sign matches the direction of the final composite score.
        Feeds into evidence_strength: a signal from 8 features that all
        agree is much more trustworthy than one from 3 features fighting
        each other.
        """
        dominant = 1 if score > 0 else (-1 if score < 0 else 0)
        if dominant == 0:
            return 0.0
        counted = 0
        agreeing = 0
        for _, v, _ in weighted:
            if abs(v) < 0.05:
                continue
            counted += 1
            sign_v = 1 if v > 0 else -1
            if sign_v == dominant:
                agreeing += 1
        return safe_div(agreeing, counted, default=0.0)

    def _compose_reasons(
        self,
        m: BookMetrics,
        weighted: List[Tuple[float, float, str]],
        score: float,
        agreement: float,
    ) -> List[str]:
        """
        Human-readable one-line reason strings attached to every signal,
        shown in the live UI's reason column and persisted in JSONL logs.
        Explains WHY a signal fired — which features contributed and any
        book-integrity warnings (spoof/iceberg/cancel-suspicion).
        """
        direction = "bullish" if score > 0.05 else "bearish" if score < -0.05 else "neutral"
        reasons = [
            f"Composite score {score:+.2f}/10 ({direction}), "
            f"feature agreement {agreement*100:.0f}%"
        ]
        for w, v, name in weighted:
            if abs(v) < 0.05:
                continue
            reasons.append(f"  {name}={v:+.2f} (w={w})")
        if m.l1_vs_depth_divergence > 0.3:
            reasons.append(
                f"⚠ L1-vs-Depth divergence {m.l1_vs_depth_divergence:.2f} "
                f"— possible fake top-of-book"
            )
        if m.spoofing_suspicion > 0.3:
            reasons.append(
                f"⚠ Spoofing suspicion {m.spoofing_suspicion:.2f} "
                f"(conviction dampened)"
            )
        if m.iceberg_suspicion > 0.3:
            reasons.append(
                f"ℹ Iceberg / replenishment suspicion {m.iceberg_suspicion:.2f}"
            )
        if m.cancellation_suspicion_ask > 0.5:
            reasons.append(
                f"ℹ Ask-side observed withdrawal without matching trades "
                f"(cancel suspicion {m.cancellation_suspicion_ask:.2f})"
            )
        if m.cancellation_suspicion_bid > 0.5:
            reasons.append(
                f"ℹ Bid-side observed withdrawal without matching trades "
                f"(cancel suspicion {m.cancellation_suspicion_bid:.2f})"
            )
        return reasons

    def _diagnostics(self, m: BookMetrics) -> Dict[str, Any]:
        """
        Flat dict of every raw metric attached to each SignalResult and
        written verbatim into the JSONL audit log. Kept as plain floats
        (rounded to 4 dp) so downstream jq / pandas queries work.
        """
        return {
            "L1_imbalance":           round(m.l1_imbalance, 4),
            "Top5_imbalance":         round(m.top5_imbalance, 4),
            "WeightedDepth_imbalance": round(m.weighted_depth_imbalance, 4),
            "BookWide_imbalance":     round(m.book_wide_imbalance, 4),
            "ImbalanceROC_5s":        round(m.imbalance_roc_5s, 4),
            "BuyBookROC_1s":          round(m.buy_book_roc_1s, 4),
            "BuyBookROC_5s":          round(m.buy_book_roc_5s, 4),
            "SellBookROC_5s":         round(m.sell_book_roc_5s, 4),
            "Spread":                 round(m.spread, 4),
            "Spread_bps":             round(m.normalized_spread_bps, 2),
            "SpreadROC_5s":           round(m.spread_roc_5s, 4),
            "Mid":                    round(m.mid_price, 4),
            "MidROC_5s":              round(m.mid_price_roc_5s, 6),
            "LTP":                    m.ltp,
            "LTPROC_5s":              round(m.ltp_roc_5s, 6),
            "IntervalVolume":         m.interval_volume,
            "BuyerAggressorRatio_5s": round(m.buyer_aggressor_ratio_5s, 3),
            "Liquidity_Added":        m.total_added,
            "Liquidity_Withdrawn":    m.total_removed,
            "BookActivity":           m.book_activity,
            "L1vsDepth_Divergence":   round(m.l1_vs_depth_divergence, 3),
            "ExecLikelihood_Ask":     round(m.execution_likelihood_ask, 3),
            "ExecLikelihood_Bid":     round(m.execution_likelihood_bid, 3),
            "Cancel_Susp_Bid":        round(m.cancellation_suspicion_bid, 3),
            "Cancel_Susp_Ask":        round(m.cancellation_suspicion_ask, 3),
            "Spoof_Susp":             round(m.spoofing_suspicion, 3),
            "Iceberg_Susp":           round(m.iceberg_suspicion, 3),
            "Replenishment":          round(m.replenishment_score, 3),
            "KillSwitch":             m.kill_switch_active,
            "KillReason":             m.kill_switch_reason,
            # Phase 2 regime
            "Regime":                 m.regime.label,
            "Regime_Volatility":      m.regime.volatility,
            "Regime_Trend":           m.regime.trend,
            "Regime_DepthBias":       m.regime.depth_bias,
            "Regime_VolRatio":        round(m.regime.volatility_ratio, 3),
            "Regime_Autocorr":        round(m.regime.autocorr_lag1, 3),
            "Regime_Tradeable":       m.regime.is_tradeable(),
            "Regime_Confident":       m.regime.is_confident,
        }


# ---------------------------------------------------------------------------
# 7. Synthetic engine demo (invoked via `--engine-demo`)
# ---------------------------------------------------------------------------
# Eight hand-crafted scenarios that exercise the engine's most important
# code paths without needing a broker connection. Also serves as living
# documentation: reading this section shows exactly what each metric
# reacts to. Run via:  python3 live_hit_rate_analyzer.py --engine-demo


def _demo_snap(ts, ltp, ltq, vol, tbq, tsq, bids, asks, symbol="DEMO"):
    """Compact factory for a MarketSnapshot used in the demo scenarios."""
    return MarketSnapshot(
        timestamp=ts, symbol=symbol,
        ltp=ltp, ltq=ltq, volume_traded=vol,
        total_buy_qty=tbq, total_sell_qty=tsq,
        bids=[DepthLevel(p, q) for p, q in bids],
        asks=[DepthLevel(p, q) for p, q in asks],
    )


def _demo_print_result(res: Optional[SignalResult], header: str):
    """Pretty-print one SignalResult with its top diagnostics for the demo."""
    print(f"\n===== {header} =====")
    if res is None:
        print("  (no signal — snapshot dropped)")
        return
    print(f"  ts={res.timestamp:.2f}  State: {res.state.value}   "
          f"Score: {res.smoothed_score:+.2f}/10   "
          f"Evidence: {res.evidence_strength:.1f}/100")
    for r in res.reasons:
        print(f"    · {r}")
    d = res.diagnostics
    show = [
        "L1_imbalance", "Top5_imbalance", "WeightedDepth_imbalance",
        "BookWide_imbalance", "ImbalanceROC_5s", "BuyerAggressorRatio_5s",
        "Spread_bps", "L1vsDepth_Divergence", "Spoof_Susp",
        "Iceberg_Susp", "Cancel_Susp_Ask", "Cancel_Susp_Bid",
        "ExecLikelihood_Ask", "KillSwitch",
    ]
    for k in show:
        if k in d and d[k] not in (None, 0, 0.0, False):
            print(f"      {k}: {d[k]}")


def _engine_demo() -> None:
    """
    8-scenario BookDynamicsEngine self-test. Runs entirely on synthetic
    data — no broker, no network, no config. Useful for:

      1. Verifying a fresh install (`bash SETUP.sh --engine-demo`)
      2. Regression-testing the engine after any change
      3. Documenting each metric's expected reaction
    """
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    print("\n### BookDynamicsEngine — Synthetic Scenarios ###")

    T0 = 1_000_000.0  # arbitrary epoch base

    # ---- Scenario 1: L1 + Depth both bullish (aligned) ----
    # Note: EMA smoothing needs multiple confirming ticks; a single strong
    # tick after balanced warm-up will produce a moderate (not extreme) score.
    eng = BookDynamicsEngine()
    for i in range(8):
        eng.update(_demo_snap(
            T0 + i, ltp=100.00, ltq=10, vol=10_000 + i * 100,
            tbq=50_000, tsq=50_000,
            bids=[(99.95, 300), (99.90, 400), (99.85, 500), (99.80, 400), (99.75, 300)],
            asks=[(100.05, 300), (100.10, 400), (100.15, 500), (100.20, 400), (100.25, 300)],
        ))
    # Three consecutive bullish ticks (allow EMA to build)
    for k, ts_off in enumerate((9, 10, 11), start=1):
        r = eng.update(_demo_snap(
            T0 + ts_off, ltp=100.05 + 0.05 * (k - 1), ltq=50, vol=12_000 + k * 500,
            tbq=90_000 + k * 5_000, tsq=45_000 - k * 2_000,
            bids=[(99.95 + 0.05 * (k - 1), 900), (99.90, 800), (99.85, 700), (99.80, 500), (99.75, 400)],
            asks=[(100.05 + 0.05 * (k - 1), 200), (100.10, 250), (100.15, 300), (100.20, 250), (100.25, 200)],
        ))
    _demo_print_result(r, "SCENARIO 1: L1 + Depth aligned BULLISH (after 3 confirming ticks)")

    # ---- Scenario 2: L1 bullish, deeper book bearish (divergence) ----
    eng = BookDynamicsEngine()
    for i in range(8):
        eng.update(_demo_snap(
            T0 + i, ltp=200.00, ltq=10, vol=20_000 + i * 100,
            tbq=50_000, tsq=50_000,
            bids=[(199.90, 400), (199.80, 500), (199.70, 500), (199.60, 500), (199.50, 500)],
            asks=[(200.10, 400), (200.20, 500), (200.30, 500), (200.40, 500), (200.50, 500)],
        ))
    r = eng.update(_demo_snap(
        T0 + 10, ltp=200.00, ltq=5, vol=22_000,
        tbq=30_000, tsq=70_000,
        bids=[(199.90, 3000), (199.80, 200), (199.70, 150), (199.60, 100), (199.50, 100)],
        asks=[(200.10,  200), (200.20, 800), (200.30, 900), (200.40, 800), (200.50, 700)],
    ))
    _demo_print_result(r, "SCENARIO 2: L1 Bullish + Deeper Book Bearish (Divergence)")

    # ---- Scenario 3: Large bid appears then disappears without trades (spoof) ----
    eng = BookDynamicsEngine()
    for i in range(6):
        eng.update(_demo_snap(
            T0 + i, ltp=500.00, ltq=10, vol=30_000 + i * 50,
            tbq=40_000, tsq=40_000,
            bids=[(499.95, 200), (499.90, 200), (499.85, 200), (499.80, 200), (499.75, 200)],
            asks=[(500.05, 200), (500.10, 200), (500.15, 200), (500.20, 200), (500.25, 200)],
        ))
    # Large bid appears at 499.95
    eng.update(_demo_snap(
        T0 + 7, ltp=500.00, ltq=0, vol=30_300,
        tbq=60_000, tsq=40_000,
        bids=[(499.95, 8000), (499.90, 200), (499.85, 200), (499.80, 200), (499.75, 200)],
        asks=[(500.05, 200),  (500.10, 200), (500.15, 200), (500.20, 200), (500.25, 200)],
    ))
    # Vanishes with almost no matched trades
    r = eng.update(_demo_snap(
        T0 + 7.5, ltp=500.00, ltq=0, vol=30_310,
        tbq=42_000, tsq=40_000,
        bids=[(499.95, 200), (499.90, 200), (499.85, 200), (499.80, 200), (499.75, 200)],
        asks=[(500.05, 200), (500.10, 200), (500.15, 200), (500.20, 200), (500.25, 200)],
    ))
    _demo_print_result(r, "SCENARIO 3: Large Bid Appears + Vanishes Without Trades (Spoof)")

    # ---- Scenario 4: Repeated ask executions with replenishment (iceberg) ----
    eng = BookDynamicsEngine()
    for i in range(4):
        eng.update(_demo_snap(
            T0 + i, ltp=300.00, ltq=10, vol=50_000 + i * 300,
            tbq=40_000, tsq=40_000,
            bids=[(299.95, 300), (299.90, 300), (299.85, 300), (299.80, 300), (299.75, 300)],
            asks=[(300.05, 500), (300.10, 300), (300.15, 300), (300.20, 300), (300.25, 300)],
        ))
    # 3 execute + refill cycles at 300.05
    for step in range(3):
        eng.update(_demo_snap(
            T0 + 5 + step * 2, ltp=300.05, ltq=200, vol=52_000 + step * 600,
            tbq=40_000, tsq=39_500,
            bids=[(299.95, 300), (299.90, 300), (299.85, 300), (299.80, 300), (299.75, 300)],
            asks=[(300.05, 100), (300.10, 300), (300.15, 300), (300.20, 300), (300.25, 300)],
        ))
        eng.update(_demo_snap(
            T0 + 5 + step * 2 + 1, ltp=300.05, ltq=0, vol=52_000 + step * 600,
            tbq=40_000, tsq=40_100,
            bids=[(299.95, 300), (299.90, 300), (299.85, 300), (299.80, 300), (299.75, 300)],
            asks=[(300.05, 500), (300.10, 300), (300.15, 300), (300.20, 300), (300.25, 300)],
        ))
    r = eng.update(_demo_snap(
        T0 + 15, ltp=300.05, ltq=100, vol=54_000,
        tbq=40_000, tsq=40_200,
        bids=[(299.95, 300), (299.90, 300), (299.85, 300), (299.80, 300), (299.75, 300)],
        asks=[(300.05, 500), (300.10, 300), (300.15, 300), (300.20, 300), (300.25, 300)],
    ))
    _demo_print_result(r, "SCENARIO 4: Repeated Ask Executions + Replenishment (Iceberg)")

    # ---- Scenario 5: Sudden spread widening → kill switch ----
    eng = BookDynamicsEngine()
    for i in range(10):
        eng.update(_demo_snap(
            T0 + i, ltp=1000.00, ltq=5, vol=1_000 + i * 10,
            tbq=30_000, tsq=30_000,
            bids=[(999.95, 100), (999.90, 100), (999.85, 100), (999.80, 100), (999.75, 100)],
            asks=[(1000.05, 100), (1000.10, 100), (1000.15, 100), (1000.20, 100), (1000.25, 100)],
        ))
    r = eng.update(_demo_snap(
        T0 + 11, ltp=1000.00, ltq=0, vol=1_100,
        tbq=30_000, tsq=30_000,
        bids=[(999.50, 100), (999.30, 100), (999.10, 100), (998.90, 100), (998.70, 100)],
        asks=[(1000.80, 100), (1001.00, 100), (1001.20, 100), (1001.40, 100), (1001.60, 100)],
    ))
    _demo_print_result(r, "SCENARIO 5: Sudden Spread Widening → Kill Switch")

    # ---- Scenario 6: Book bullish but Mid/LTP don't confirm ----
    eng = BookDynamicsEngine()
    for i in range(8):
        eng.update(_demo_snap(
            T0 + i, ltp=250.00, ltq=5, vol=5_000 + i * 10,
            tbq=50_000, tsq=50_000,
            bids=[(249.95, 300), (249.90, 300), (249.85, 300), (249.80, 300), (249.75, 300)],
            asks=[(250.05, 300), (250.10, 300), (250.15, 300), (250.20, 300), (250.25, 300)],
        ))
    r = eng.update(_demo_snap(
        T0 + 10, ltp=250.00, ltq=5, vol=5_100,
        tbq=90_000, tsq=40_000,        # book bullish
        bids=[(249.95, 900), (249.90, 900), (249.85, 900), (249.80, 800), (249.75, 700)],
        asks=[(250.05, 300), (250.10, 300), (250.15, 300), (250.20, 300), (250.25, 300)],
        # BUT LTP and mid unchanged — price refuses to confirm
    ))
    _demo_print_result(r, "SCENARIO 6: Book Bullish but Mid/LTP DO NOT Confirm")

    # ---- Scenario 7: Top-5 window shifts due to price move (no false spoof) ----
    eng = BookDynamicsEngine()
    for i in range(6):
        eng.update(_demo_snap(
            T0 + i, ltp=400.00, ltq=10, vol=10_000 + i * 100,
            tbq=40_000, tsq=40_000,
            bids=[(399.95, 200), (399.90, 200), (399.85, 200), (399.80, 200), (399.75, 200)],
            asks=[(400.05, 200), (400.10, 200), (400.15, 200), (400.20, 200), (400.25, 200)],
        ))
    # Price moves up — new bid level appears at 400.00; old 399.75 falls out of Top-5.
    # Engine should NOT flag as spoof (level intersection logic).
    # Volume must be monotonic: last warm-up = 10_500, so use higher value.
    r = eng.update(_demo_snap(
        T0 + 7, ltp=400.05, ltq=100, vol=11_000,
        tbq=41_000, tsq=39_000,
        bids=[(400.00, 200), (399.95, 200), (399.90, 200), (399.85, 200), (399.80, 200)],
        asks=[(400.10, 200), (400.15, 200), (400.20, 200), (400.25, 200), (400.30, 200)],
    ))
    _demo_print_result(r, "SCENARIO 7: Top-5 Window Shifts (no false cancel/spoof expected)")

    # ---- Scenario 8: Duplicate / Out-of-order / Same-second update ----
    # Contract (post-P0):
    #   * Exact duplicate (identical content, no sequence)     → dropped
    #   * Earlier event time (no sequence)                     → dropped
    #   * Same event time BUT new content (no sequence)        → ACCEPTED
    #     (fixes the bug where genuine same-second book updates were
    #      being dropped by the old timestamp-only guard)
    #   * Newer sequence within the same exchange second       → ACCEPTED
    #   * Duplicate or lower sequence                          → dropped
    eng = BookDynamicsEngine()
    for i in range(5):
        eng.update(_demo_snap(
            T0 + i, ltp=150.00, ltq=5, vol=2_000 + i * 20,
            tbq=30_000, tsq=30_000,
            bids=[(149.95, 200), (149.90, 200), (149.85, 200), (149.80, 200), (149.75, 200)],
            asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
        ))
    # (a) EXACT duplicate (same ts, same content) → drop
    dup = eng.update(_demo_snap(
        T0 + 4, ltp=150.00, ltq=5, vol=2_080,
        tbq=30_000, tsq=30_000,
        bids=[(149.95, 200), (149.90, 200), (149.85, 200), (149.80, 200), (149.75, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    ))
    # (b) Same-second, NEW content → now accepted (was silently dropped before)
    same_sec_new = eng.update(_demo_snap(
        T0 + 4, ltp=150.02, ltq=5, vol=2_150,
        tbq=32_000, tsq=30_000,
        bids=[(149.97, 300), (149.92, 200), (149.87, 200), (149.82, 200), (149.77, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    ))
    # (c) Out-of-order event time (no sequence) → drop
    ooo = eng.update(_demo_snap(
        T0 + 3, ltp=150.00, ltq=5, vol=2_300,
        tbq=30_000, tsq=30_000,
        bids=[(149.95, 200), (149.90, 200), (149.85, 200), (149.80, 200), (149.75, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    ))

    # (d) Sequence-based dedup: same exchange second, newer sequence → accept;
    #     replay of an older sequence → drop.
    eng2 = BookDynamicsEngine()
    base = _demo_snap(
        T0 + 100, ltp=150.00, ltq=5, vol=5_000,
        tbq=30_000, tsq=30_000,
        bids=[(149.95, 200), (149.90, 200), (149.85, 200), (149.80, 200), (149.75, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    )
    base.sequence_number = 100
    base.exchange_timestamp = T0 + 100
    seq_first = eng2.update(base)
    later = _demo_snap(
        T0 + 100, ltp=150.02, ltq=5, vol=5_050,
        tbq=32_000, tsq=30_000,
        bids=[(149.97, 300), (149.92, 200), (149.87, 200), (149.82, 200), (149.77, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    )
    later.sequence_number = 101
    later.exchange_timestamp = T0 + 100
    seq_next = eng2.update(later)
    replay = _demo_snap(
        T0 + 100, ltp=150.02, ltq=5, vol=5_050,
        tbq=32_000, tsq=30_000,
        bids=[(149.97, 300), (149.92, 200), (149.87, 200), (149.82, 200), (149.77, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    )
    replay.sequence_number = 100
    replay.exchange_timestamp = T0 + 100
    seq_replay = eng2.update(replay)

    print("\n===== SCENARIO 8: Sequence / Timestamp / Dedup Contract =====")
    print(f"  (a) exact-duplicate no-seq:       {'None' if dup is None else 'SIGNAL'}  "
          f"(expected: None)")
    print(f"  (b) same-second new content:      "
          f"{'SIGNAL' if same_sec_new is not None else 'None'}  (expected: SIGNAL)")
    print(f"  (c) out-of-order event time:      {'None' if ooo is None else 'SIGNAL'}  "
          f"(expected: None)")
    print(f"  (d1) first seq accepted:          "
          f"{'SIGNAL' if seq_first is not None else 'None'}  (expected: SIGNAL)")
    print(f"  (d2) newer seq same second:       "
          f"{'SIGNAL' if seq_next is not None else 'None'}  (expected: SIGNAL)")
    print(f"  (d3) replay of older seq dropped: {'None' if seq_replay is None else 'SIGNAL'}"
          f"  (expected: None)")

    print("\n### Demo complete. ###\n")


# ---------------------------------------------------------------------------
# 8. Runtime / session-layer additional imports
# ---------------------------------------------------------------------------
# The rest of the file (config loading, Angel One session, HitRateAnalyzer,
# UI, main()) uses a broader set of stdlib modules than the engine section.
# These imports are placed here (rather than at the top of the file) so the
# engine part above can stand alone if this file were ever split.

import argparse            # CLI parsing
import json                # config.json + JSONL log I/O
import logging.handlers    # RotatingFileHandler for scanner.log
import os                  # env vars + path helpers
import random              # unused legacy import kept for compat
import signal as py_signal # SIGINT / SIGTERM graceful shutdown
import sys                 # exit codes + argv
from pathlib import Path   # log file paths

# Note: BookDynamicsEngine and all engine-related classes are defined
# ABOVE in this same file — no import needed.

# ---------------------------------------------------------------------------
# Optional runtime dependencies — graceful fallback when missing
# ---------------------------------------------------------------------------
# The tool degrades cleanly rather than crashing when a nice-to-have package
# is missing. This is important on fresh VPS installs where pip may have
# failed on a specific package but the rest of the tool should still work.

# `rich` is used ONLY for the interactive console UI. In its absence we
# fall back to the periodic-print `--no-ui` mode, which is what systemd
# uses anyway.
try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.text import Text
    from rich.align import Align
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Angel One SDK is required for actual live WebSocket connectivity but not
# for --engine-demo or unit tests. Import lazily and expose the availability
# flag so callers can produce a clear error rather than a stack trace.
SMARTAPI_AVAILABLE = False
try:
    import pyotp
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    SMARTAPI_AVAILABLE = True
except ImportError:
    pass

# `requests` is used for the one-off scrip-master JSON download. Almost
# always installed, but we mark it optional so a demo-only run works even
# without it.
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Runtime constants — broker-specific values
# ---------------------------------------------------------------------------

# Public URL of Angel One's scrip master JSON. Contains the symbol → token
# mapping for every listed instrument. Cached locally per config TTL.
SCRIP_MASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)

# Angel One WebSocket protocol constants.
#   NSE_CM_EXCHANGE_TYPE = 1 → NSE Cash Market segment
#   SUBSCRIPTION_MODE_SNAP_QUOTE = 3 → full Level-2 top-5 depth + LTP
#     (mode 1 = LTP-only, mode 2 = quote, mode 3 = full snapquote)
NSE_CM_EXCHANGE_TYPE = 1
SUBSCRIPTION_MODE_SNAP_QUOTE = 3

# Angel One reports prices as INTEGER paise. Multiplying by this converts
# to INR floats for our engine math.
PAISE_TO_INR = 0.01

# Session-layer logger (separate from the "BookDynamicsEngine" logger above)
# so operators can raise/lower verbosity independently.
logger = logging.getLogger("nse_scanner")


# ---------------------------------------------------------------------------
# 9. Configuration loading (config.json → ScannerConfig dataclass)
# ---------------------------------------------------------------------------

@dataclass
class ScannerConfig:
    """
    Complete configuration for a live session. Populated from `config.json`
    by `load_config()`. Angel One credentials are required; almost every
    other field has a sensible default.

    NOTE: Some fields (min_evidence_strength_to_log, log_signal_states,
    prediction_* etc.) are LEGACY from earlier scanner/prediction-tracker
    designs. They are still read by load_config() for backwards config-file
    compatibility but no longer drive downstream behaviour in the current
    hit-rate analyzer.
    """
    # ---- Angel One SmartAPI credentials (mandatory) ----
    api_key: str = ""       # smartapi.angelbroking.com → My Apps
    client_code: str = ""   # Angel One login id (e.g. "A1234567")
    pin: str = ""           # 4-digit trading MPIN
    totp_secret: str = ""   # base32 TOTP secret from Google Authenticator setup

    # ---- Universe of symbols to subscribe (SYMBOL-EQ format) ----
    symbols: List[str] = field(default_factory=list)

    # ---- Legacy scanner-behaviour fields (kept for config-file compat) ----
    min_evidence_strength_to_log: float = 30.0
    log_signal_states: List[str] = field(default_factory=lambda: [
        "WEAK_LONG", "LONG", "STRONG_LONG",
        "WEAK_SHORT", "SHORT", "STRONG_SHORT",
    ])
    signal_dedup_seconds: float = 5.0    # dedup window for record_signal
    ui_refresh_ms: int = 500             # rich UI refresh cadence
    top_n_display: int = 10              # not currently rendered

    # Producer-consumer WS-thread → worker queue size. Only used by the
    # removed multi-scanner class; kept here so old config.json files
    # don't fail validation.
    tick_queue_size: int = 20000

    # ---- Prediction-tracker fields (legacy, superseded by HitRateAnalyzer) ---
    # These fields were used by the deprecated inline PredictionTracker.
    # Retained here so existing config.json files load without error.
    # कि scanner के signals sirf compute-correct हैं या truly predict-correct।
    prediction_horizons_s: List[float] = field(
        default_factory=lambda: [30.0, 60.0, 120.0]
    )
    # NSE Intraday round-trip cost (brokerage + STT + exch + GST + stamp)
    # Zerodha/Angel typical: ~0.06% for liquid Nifty stocks
    transaction_cost_pct: float = 0.0006
    prediction_log_path: str = "logs/predictions.jsonl"
    # UI में कौन-सा horizon दिखाना है (JSONL में सारे log होते हैं)
    prediction_display_horizon_s: float = 60.0
    # Verdict देने से पहले कम-से-कम इतने samples चाहिए
    prediction_min_samples_for_verdict: int = 20
    # Pending prediction अगर इस समय तक भी evaluate न हो, तो force-close
    # (जैसे symbol trade करना बंद कर दे या feed disconnect हो)
    prediction_max_pending_age_s: float = 300.0

    # Paths
    signal_log_path: str = "logs/signals.jsonl"
    system_log_path: str = "logs/scanner.log"
    scrip_master_cache_path: str = "logs/scrip_master.json"
    scrip_master_ttl_hours: int = 24

    # Engine tuning (subset — see EngineConfig for all)
    engine_config: EngineConfig = field(default_factory=EngineConfig)


def load_config(path: str) -> ScannerConfig:
    """
    Load a JSON config file into a `ScannerConfig` with strict but friendly
    error messages. Called once at startup by main().

    Raises:
      FileNotFoundError  — config path does not exist (with a fix-suggestion)
      ValueError         — JSON syntax error OR empty symbols list

    Silently ignores unknown top-level keys and unknown fields inside
    the `scanner` / `engine` blocks (so newer config files can be shared
    across older code versions without breakage).
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Config file '{path}' not found.\n"
            f"Please: cp config.example.json {path} और अपने credentials भरें।"
        )
    try:
        data = json.loads(p.read_text())
    except json.JSONDecodeError as e:
        raise ValueError(f"'{path}' में JSON syntax error: {e}") from e

    cfg = ScannerConfig()

    ao = data.get("angel_one", {})
    cfg.api_key     = ao.get("api_key", "")
    cfg.client_code = ao.get("client_code", "")
    cfg.pin         = ao.get("pin", "")
    cfg.totp_secret = ao.get("totp_secret", "")

    cfg.symbols = [s for s in data.get("symbols", []) if s and not s.startswith("_")]
    if not cfg.symbols:
        raise ValueError("Config में कोई symbol नहीं मिला।")

    sc = data.get("scanner", {})
    cfg.min_evidence_strength_to_log = float(sc.get("min_evidence_strength_to_log", 30))
    cfg.log_signal_states = sc.get("log_signal_states", cfg.log_signal_states)
    cfg.signal_dedup_seconds = float(sc.get("signal_dedup_seconds", 5.0))
    cfg.ui_refresh_ms = int(sc.get("ui_refresh_ms", 500))
    cfg.top_n_display = int(sc.get("top_n_display", 10))
    cfg.tick_queue_size = int(sc.get("tick_queue_size", 20000))
    if "prediction_horizons_s" in sc:
        cfg.prediction_horizons_s = [float(x) for x in sc["prediction_horizons_s"]]
    if "transaction_cost_pct" in sc:
        cfg.transaction_cost_pct = float(sc["transaction_cost_pct"])
    if "prediction_log_path" in sc:
        cfg.prediction_log_path = sc["prediction_log_path"]
    if "prediction_display_horizon_s" in sc:
        cfg.prediction_display_horizon_s = float(sc["prediction_display_horizon_s"])
    if "prediction_min_samples_for_verdict" in sc:
        cfg.prediction_min_samples_for_verdict = int(sc["prediction_min_samples_for_verdict"])
    if "prediction_max_pending_age_s" in sc:
        cfg.prediction_max_pending_age_s = float(sc["prediction_max_pending_age_s"])
    cfg.signal_log_path = sc.get("signal_log_path", cfg.signal_log_path)
    cfg.system_log_path = sc.get("system_log_path", cfg.system_log_path)
    cfg.scrip_master_cache_path = sc.get("scrip_master_cache_path", cfg.scrip_master_cache_path)
    cfg.scrip_master_ttl_hours = int(sc.get("scrip_master_ttl_hours", 24))

    ec = data.get("engine", {})
    if ec:
        # Only override defined fields
        ecfg = cfg.engine_config
        for fld in [
            "history_seconds", "depth_decay_frac", "ema_alpha",
            "threshold_strong", "threshold_normal", "threshold_weak",
            "spoof_dampener_strength", "kill_switch_spread_multiplier",
        ]:
            if fld in ec:
                setattr(ecfg, fld, type(getattr(ecfg, fld))(ec[fld]))

    return cfg


# ---------------------------------------------------------------------------
# 2. Logging setup
# ---------------------------------------------------------------------------

def setup_logging(config: ScannerConfig) -> None:
    """
    Configure root logging for the process:
      * INFO+ to `config.system_log_path` (rotating, 10 MB × 5 backups)
      * WARNING+ to stderr (so the terminal / systemd journal stays clean)
      * Quiets down the noisy SmartApi + websocket internal loggers.

    Idempotent — safe to call multiple times.
    """
    log_path = Path(config.system_log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Clear pre-existing handlers (idempotent restart)
    root.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Stderr (only warnings+, so it doesn't clash with rich UI)
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.WARNING)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    # Silence chatty library loggers that would otherwise dominate the file.
    # We still see their WARNING+ messages via the root stderr handler.
    logging.getLogger("SmartApi").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# 10. Signal-state grouping constants
# ---------------------------------------------------------------------------
# These sets are referenced by the engine (score→state mapping), the
# HitRateAnalyzer (state filter), and the executable-fill layer (side
# determination from state). Centralising them here prevents string
# typos from silently breaking downstream filters.

# All LONG-side states (bullish direction, any strength).
_LONG_STATES  = {"STRONG_LONG", "LONG", "WEAK_LONG"}

# All SHORT-side states (bearish direction, any strength).
_SHORT_STATES = {"STRONG_SHORT", "SHORT", "WEAK_SHORT"}

# Any state we consider "worth recording" (union of long + short).
# NEUTRAL and SUPPRESSED are deliberately excluded.
_ACTIONABLE_STATES = _LONG_STATES | _SHORT_STATES

# Filter set used by the CLI `--strong-only` flag: record ONLY the two
# STRONG states. Anything WEAK/LONG/SHORT is dropped at the state filter.
_STRONG_STATES = {"STRONG_LONG", "STRONG_SHORT"}

# Filter set used by CLI `--skip-weak`: keep STRONG + regular LONG/SHORT,
# drop only the WEAK variants (which are typically the noisiest).
_NORMAL_AND_STRONG_STATES = {"STRONG_LONG", "LONG", "STRONG_SHORT", "SHORT"}


# ---------------------------------------------------------------------------
# 11. Signal quality gates — optional per-signal filters
# ---------------------------------------------------------------------------
# Two lightweight, standalone classes that HitRateAnalyzer can OPTIONALLY
# consult before recording a signal. Neither changes engine behaviour —
# they are pure filters. Trader can turn each on/off via CLI flags:
#
#   SessionStateManager  →  NSE market-phase tracker (fixed IST timezone).
#                           Blocks signals during LUNCH / PRE_CLOSE /
#                           CLOSING and after the 15:15 no-new-entry cutoff.
#                           Enabled by --session-filter.
#
#   RVOLCalculator       →  Relative volume tracker (current-minute vs a
#                           rolling 20-minute average). Blocks signals when
#                           the symbol is trading below `--min-rvol`.
#                           Enabled by --min-rvol N.
#
# Passing None to HitRateAnalyzer for either gate disables it entirely,
# so old configurations continue to work with no filter behaviour.
# ---------------------------------------------------------------------------


class SessionPhase(Enum):
    """
    NSE Equity segment के trading day की अलग-अलग phases।

    तीन 'सामान्य trading' phases (OPENING, MORNING, AFTERNOON) में signals
    सबसे reliable होते हैं। LUNCH में liquidity कम, PRE_CLOSE/CLOSING में
    लोग squaring off कर रहे होते हैं (fundamentals से हट कर) — इसलिए
    इन्हें default में allowed set से बाहर रखते हैं।
    """
    PRE_OPEN_ENTRY = "PRE_OPEN_ENTRY"     # 09:00 – 09:07:59 (order collection)
    PRE_OPEN_MATCH = "PRE_OPEN_MATCH"     # 09:08 – 09:14:59 (no book changes)
    OPENING        = "OPENING"            # 09:15 – 09:30    (opening volatility)
    MORNING        = "MORNING"            # 09:30 – 11:30    (best trending phase)
    LUNCH          = "LUNCH"              # 11:30 – 13:30    (low activity)
    AFTERNOON      = "AFTERNOON"          # 13:30 – 15:00    (positioning)
    PRE_CLOSE      = "PRE_CLOSE"          # 15:00 – 15:20    (squaring begins)
    CLOSING        = "CLOSING"            # 15:20 – 15:30    (heavy squaring)
    POST_CLOSE     = "POST_CLOSE"         # 15:30 – 16:00    (closing session)
    CLOSED         = "CLOSED"             # outside all above
    WEEKEND        = "WEEKEND"            # Saturday / Sunday
    HOLIDAY        = "HOLIDAY"            # user-configured trading holiday


# The canonical NSE IST timezone constant used across the whole file.
# It is defined once here and referenced as `IST` everywhere (previously we
# had a duplicate `_IST` alias further up that could silently diverge from
# `IST` if only one was changed — consolidated to a single source of truth).
IST = timezone(timedelta(hours=5, minutes=30))

# Default set of phases where signals are considered "tradeable"
# (User can override via CLI / config)
DEFAULT_TRADEABLE_PHASES: FrozenSet[SessionPhase] = frozenset({
    SessionPhase.OPENING,
    SessionPhase.MORNING,
    SessionPhase.AFTERNOON,
})


class SessionStateManager:
    """
    Thread-safe NSE market phase tracker.

    Uses fixed IST timezone (independent of VPS system timezone). Handles
    weekends automatically. Holidays must be provided by caller as a list
    of 'YYYY-MM-DD' date strings (there is no built-in NSE holiday
    calendar — user can update this list annually).

    Basic use:
        mgr = SessionStateManager()
        phase = mgr.get_phase(time.time())
        if mgr.is_tradeable(time.time()):
            # accept signal
            ...

    Phase boundaries can be overridden if NSE changes hours in future:
        mgr = SessionStateManager(phase_overrides={
            SessionPhase.CLOSING: (dt_time(15, 25), dt_time(15, 30)),
        })
    """

    # (start_inclusive, end_exclusive) for each phase (IST local time)
    _DEFAULT_BOUNDARIES: Dict[SessionPhase, Tuple[dt_time, dt_time]] = {
        SessionPhase.PRE_OPEN_ENTRY: (dt_time(9,  0), dt_time(9,  8)),
        SessionPhase.PRE_OPEN_MATCH: (dt_time(9,  8), dt_time(9, 15)),
        SessionPhase.OPENING:        (dt_time(9, 15), dt_time(9, 30)),
        SessionPhase.MORNING:        (dt_time(9, 30), dt_time(11, 30)),
        SessionPhase.LUNCH:          (dt_time(11, 30), dt_time(13, 30)),
        SessionPhase.AFTERNOON:      (dt_time(13, 30), dt_time(15, 0)),
        SessionPhase.PRE_CLOSE:      (dt_time(15,  0), dt_time(15, 20)),
        SessionPhase.CLOSING:        (dt_time(15, 20), dt_time(15, 30)),
        SessionPhase.POST_CLOSE:     (dt_time(15, 30), dt_time(16, 0)),
    }

    # Time at which we want to forcibly stop opening new positions
    # regardless of phase (safety cutoff — no last-15-min entries)
    DEFAULT_NO_NEW_ENTRY_AFTER: dt_time = dt_time(15, 15)

    def __init__(
        self,
        holidays: Optional[Iterable[str]] = None,
        phase_overrides: Optional[Dict[SessionPhase, Tuple[dt_time, dt_time]]] = None,
        no_new_entry_after: Optional[dt_time] = None,
    ):
        self._holidays: Set[str] = set(holidays or [])
        # Validate holiday format 'YYYY-MM-DD'
        for h in list(self._holidays):
            try:
                datetime.strptime(h, "%Y-%m-%d")
            except ValueError:
                logger.warning("Invalid holiday date '%s' — skipped (expected YYYY-MM-DD)", h)
                self._holidays.discard(h)

        self._boundaries: Dict[SessionPhase, Tuple[dt_time, dt_time]] = dict(
            self._DEFAULT_BOUNDARIES
        )
        if phase_overrides:
            self._boundaries.update(phase_overrides)

        self.no_new_entry_after: dt_time = (
            no_new_entry_after or self.DEFAULT_NO_NEW_ENTRY_AFTER
        )

        # Per-day tracking (for stats)
        self._lock = threading.RLock()
        self._phase_hits: Dict[SessionPhase, int] = defaultdict(int)
        self._last_phase: Optional[SessionPhase] = None
        self._phase_transitions: int = 0

    # ------- Core API -------

    def get_phase(self, ts: float) -> SessionPhase:
        """
        Return NSE phase for given Unix timestamp (UTC seconds).
        Thread-safe. Also updates internal transition stats.
        """
        dt = datetime.fromtimestamp(ts, tz=IST)
        wd = dt.weekday()   # Monday=0 ... Sunday=6

        # -- Weekend --
        if wd >= 5:
            return self._record(SessionPhase.WEEKEND)

        # -- Holiday --
        if dt.strftime("%Y-%m-%d") in self._holidays:
            return self._record(SessionPhase.HOLIDAY)

        # -- Match trading-day phase by local time --
        local_t = dt.time()
        for phase, (start, end) in self._boundaries.items():
            if start <= local_t < end:
                return self._record(phase)

        # -- Outside all phases (before 9:00, after 16:00, etc.) --
        return self._record(SessionPhase.CLOSED)

    def is_tradeable(
        self,
        ts: float,
        allowed_phases: Optional[FrozenSet[SessionPhase]] = None,
        enforce_no_new_entry_cutoff: bool = True,
    ) -> Tuple[bool, str]:
        """
        Returns (allowed, reason).
        reason string is empty when allowed, otherwise explains why blocked.
        """
        if allowed_phases is None:
            allowed_phases = DEFAULT_TRADEABLE_PHASES

        phase = self.get_phase(ts)
        if phase not in allowed_phases:
            return False, f"phase={phase.value}"

        if enforce_no_new_entry_cutoff:
            dt = datetime.fromtimestamp(ts, tz=IST)
            if dt.weekday() < 5 and dt.time() >= self.no_new_entry_after:
                return False, f"after_no_entry_cutoff({self.no_new_entry_after})"

        return True, ""

    def seconds_to_close(self, ts: float, close_time: dt_time = dt_time(15, 30)) -> float:
        """
        Seconds remaining until market close (15:30 IST by default) on the
        current trading day. Returns 0.0 if market already closed.
        """
        dt = datetime.fromtimestamp(ts, tz=IST)
        if dt.weekday() >= 5 or dt.strftime("%Y-%m-%d") in self._holidays:
            return 0.0
        target = dt.replace(
            hour=close_time.hour, minute=close_time.minute,
            second=0, microsecond=0,
        )
        return max(0.0, (target - dt).total_seconds())

    def add_holiday(self, date_str: str) -> None:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid holiday '{date_str}', expected YYYY-MM-DD")
        with self._lock:
            self._holidays.add(date_str)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "current_phase": self._last_phase.value if self._last_phase else None,
                "phase_transitions": self._phase_transitions,
                "phase_hits": {p.value: c for p, c in self._phase_hits.items() if c > 0},
                "holidays_configured": sorted(self._holidays),
                "no_new_entry_after": self.no_new_entry_after.strftime("%H:%M"),
            }

    # ------- Internal -------

    def _record(self, phase: SessionPhase) -> SessionPhase:
        with self._lock:
            self._phase_hits[phase] += 1
            if self._last_phase != phase:
                self._phase_transitions += 1
                self._last_phase = phase
        return phase


class RVOLCalculator:
    """
    Rolling Relative Volume (RVOL) tracker per symbol.

    Compares current-minute traded volume against the average of the
    previous N minutes. RVOL > 1.5 = elevated activity, > 3.0 = unusual.
    Signals during high RVOL are typically more reliable because there's
    genuine two-sided interest driving prices — not thin-book noise.

    Usage:
        rvol = RVOLCalculator(window_minutes=20, bucket_seconds=60)

        # On every incoming tick (from any thread):
        rvol.on_tick(symbol, snap.volume_traded, snap.timestamp)

        # On every signal fire:
        r = rvol.get_rvol(symbol, ts)
        if r is None or r < 1.5:
            # skip / lower confidence
            ...

    Session reset:
        A NEGATIVE delta (current_cum < prev_cum) is treated as start of
        a new trading session (day rollover). All buckets cleared, baseline
        re-established. This prevents yesterday's cumulative volume from
        polluting today's RVOL.

    Warmup:
        Returns None until at least `warmup_buckets` prior-minute buckets
        have been accumulated. Also returns None if the current bucket
        is less than `min_bucket_age_sec` old (too noisy to project).

    Rate adjustment:
        Current bucket volume is scaled by (bucket_seconds / bucket_age)
        to compare apples-to-apples against completed buckets.
    """

    def __init__(
        self,
        window_minutes: int = 20,
        bucket_seconds: int = 60,
        warmup_buckets: int = 5,
        min_bucket_age_sec: float = 5.0,
        max_delta_sanity: int = 100_000_000,  # single-tick vol > 100M = anomaly
    ):
        if window_minutes < 2:
            raise ValueError("window_minutes must be >= 2")
        if bucket_seconds < 1:
            raise ValueError("bucket_seconds must be >= 1")
        if warmup_buckets < 1:
            raise ValueError("warmup_buckets must be >= 1")
        if warmup_buckets > window_minutes:
            raise ValueError("warmup_buckets cannot exceed window_minutes")

        self.window_minutes = window_minutes
        self.bucket_seconds = bucket_seconds
        self.warmup_buckets = warmup_buckets
        self.min_bucket_age_sec = min_bucket_age_sec
        self.max_delta_sanity = max_delta_sanity

        self._lock = threading.RLock()
        self._prev_cum: Dict[str, int] = {}
        self._buckets: Dict[str, Deque[int]] = defaultdict(
            lambda: deque(maxlen=window_minutes)
        )
        # symbol → (bucket_start_ts, current_bucket_volume)
        self._current: Dict[str, Tuple[int, int]] = {}

        # Stats
        self.total_ticks_processed: int = 0
        self.session_resets: int = 0
        self.anomalies_capped: int = 0
        self.rvol_queries: int = 0
        self.rvol_returns_none: int = 0

    # ------- Core API -------

    def on_tick(self, symbol: str, cumulative_volume: int, ts: float) -> None:
        """
        Update RVOL state with cumulative day volume from a tick.

        Feed this from your tick handler for every tick where
        `cumulative_volume` is available (Angel One SnapQuote provides
        `volume_trade_for_the_day`).
        """
        if cumulative_volume < 0:
            return  # defensive: reject nonsensical negative cumulative
        with self._lock:
            self.total_ticks_processed += 1

            prev = self._prev_cum.get(symbol)
            if prev is None:
                # First observation for this symbol — baseline only,
                # don't count as delta (we don't know when day started)
                self._prev_cum[symbol] = cumulative_volume
                return

            delta = cumulative_volume - prev
            self._prev_cum[symbol] = cumulative_volume

            # Session reset detection: cumulative went DOWN → new day
            if delta < 0:
                self.session_resets += 1
                self._buckets[symbol].clear()
                self._current.pop(symbol, None)
                return

            if delta == 0:
                # No new trades this tick — still may need to roll bucket
                # forward if time advanced past current bucket end
                self._maybe_roll_bucket(symbol, ts, add_volume=0)
                return

            # Sanity cap on outlier delta (data glitch protection)
            if delta > self.max_delta_sanity:
                self.anomalies_capped += 1
                delta = self.max_delta_sanity

            self._maybe_roll_bucket(symbol, ts, add_volume=delta)

    def get_rvol(self, symbol: str, ts: float) -> Optional[float]:
        """
        Return current RVOL, or None if not enough data yet.

        RVOL = projected_current_bucket_volume / mean(prior N buckets)
        where projected_current = actual × (bucket_seconds / bucket_age).
        """
        with self._lock:
            self.rvol_queries += 1

            cur = self._current.get(symbol)
            if cur is None:
                self.rvol_returns_none += 1
                return None

            prior = self._buckets[symbol]
            if len(prior) < self.warmup_buckets:
                self.rvol_returns_none += 1
                return None

            bucket_start, current_vol = cur
            bucket_age = ts - bucket_start

            # If bucket_age negative (ts went backwards) or too small, skip
            if bucket_age < self.min_bucket_age_sec:
                # But if we have no volume yet, definitionally rvol = 0
                if current_vol == 0:
                    return 0.0
                self.rvol_returns_none += 1
                return None

            avg_prior = statistics.mean(prior)
            if avg_prior <= 0:
                # All prior buckets were zero — either brand-new session
                # or thinly-traded symbol. If current has ANY volume,
                # rvol is "infinite" — cap at 10x. If zero, return 0.
                if current_vol > 0:
                    return 10.0
                return 0.0

            # Cap bucket_age at bucket_seconds so we don't over-project
            # if a bucket was somehow not rolled (should be rare)
            effective_age = min(bucket_age, float(self.bucket_seconds))
            projected = current_vol * (self.bucket_seconds / max(effective_age, 1.0))
            return projected / avg_prior

    def is_ready(self, symbol: str) -> bool:
        """True if we have enough buckets to return a meaningful RVOL."""
        with self._lock:
            return len(self._buckets.get(symbol, ())) >= self.warmup_buckets

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            ready_syms = sum(
                1 for s in self._buckets
                if len(self._buckets[s]) >= self.warmup_buckets
            )
            return {
                "symbols_tracked": len(self._prev_cum),
                "symbols_warmed_up": ready_syms,
                "ticks_processed": self.total_ticks_processed,
                "session_resets": self.session_resets,
                "anomalies_capped": self.anomalies_capped,
                "rvol_queries": self.rvol_queries,
                "rvol_returns_none": self.rvol_returns_none,
                "window_minutes": self.window_minutes,
                "bucket_seconds": self.bucket_seconds,
                "warmup_buckets": self.warmup_buckets,
            }

    # ------- Internal -------

    def _maybe_roll_bucket(self, symbol: str, ts: float, add_volume: int) -> None:
        """Called under lock. Adds volume to current bucket, rolling if needed."""
        bucket_start = int(ts // self.bucket_seconds) * self.bucket_seconds
        cur = self._current.get(symbol)

        if cur is None:
            # First bucket for this symbol
            self._current[symbol] = (bucket_start, add_volume)
            return

        current_start, current_vol = cur

        if bucket_start == current_start:
            # Same bucket — accumulate
            self._current[symbol] = (current_start, current_vol + add_volume)
            return

        if bucket_start < current_start:
            # Clock skew (ts went backwards). Ignore — keep existing bucket.
            return

        # New bucket — close old one, push to history
        self._buckets[symbol].append(current_vol)

        # Fill any gap buckets with zero (dead time between activity)
        gap_buckets = (bucket_start - current_start) // self.bucket_seconds - 1
        if gap_buckets > 0:
            # Cap gap fill at window_minutes to avoid unbounded fills
            for _ in range(min(gap_buckets, self.window_minutes)):
                self._buckets[symbol].append(0)

        self._current[symbol] = (bucket_start, add_volume)


# ---------------------------------------------------------------------------
# 5. Angel One WebSocket production adapter
#    Parses SmartWebSocketV2's on_data message → MarketSnapshot
# ---------------------------------------------------------------------------

class AngelOneWSAdapter:
    """
    Angel One SmartWebSocketV2 का on_data callback एक dict देता है जिसमें
    prices पैसे (paise) में integer के रूप में होते हैं। यह adapter उन्हें
    INR में convert करता है और broker-agnostic MarketSnapshot बनाता है।
    """

    @staticmethod
    def _paise(v: Any) -> float:
        try:
            return float(v) * PAISE_TO_INR
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def parse(msg: Dict[str, Any], symbol: str) -> Optional[MarketSnapshot]:
        """Parse WS parsed-message dict → MarketSnapshot. Returns None if malformed."""
        received_ts = time.time_ns() / 1_000_000_000.0
        try:
            # Keep both clocks. Angel's exchange timestamp is often only
            # second-resolution; local receive time provides sub-second event
            # timing while sequence_number provides message identity/order.
            ts_ms = (
                msg.get("exchange_timestamp")
                or msg.get("exchange_feed_time_epoch_ms")
                or msg.get("last_traded_timestamp")
            )
            exchange_ts = float(ts_ms) / 1000.0 if ts_ms else None

            raw_sequence = msg.get("sequence_number")
            sequence = int(raw_sequence) if raw_sequence is not None else None

            ltp = AngelOneWSAdapter._paise(msg.get("last_traded_price"))
            if ltp <= 0:
                return None

            ltq = int(msg.get("last_traded_quantity") or 0)
            vol = int(msg.get("volume_trade_for_the_day") or 0)
            tbq = int(msg.get("total_buy_quantity") or 0)
            tsq = int(msg.get("total_sell_quantity") or 0)

            bids_raw = msg.get("best_5_buy_data") or []
            asks_raw = msg.get("best_5_sell_data") or []

            bids: List[DepthLevel] = []
            for lv in bids_raw[:5]:
                if not isinstance(lv, dict):
                    continue
                p = AngelOneWSAdapter._paise(lv.get("price"))
                q = int(lv.get("quantity") or 0)
                if p > 0 and q > 0:
                    bids.append(DepthLevel(price=p, quantity=q))

            asks: List[DepthLevel] = []
            for lv in asks_raw[:5]:
                if not isinstance(lv, dict):
                    continue
                p = AngelOneWSAdapter._paise(lv.get("price"))
                q = int(lv.get("quantity") or 0)
                if p > 0 and q > 0:
                    asks.append(DepthLevel(price=p, quantity=q))

            if not bids or not asks:
                return None

            return MarketSnapshot(
                timestamp=received_ts,
                symbol=symbol,
                ltp=ltp,
                ltq=ltq,
                volume_traded=vol,
                total_buy_qty=tbq,
                total_sell_qty=tsq,
                bids=bids,
                asks=asks,
                sequence_number=sequence,
                exchange_timestamp=exchange_ts,
                received_timestamp=received_ts,
                upper_circuit=AngelOneWSAdapter._paise(msg.get("upper_circuit_limit")) or None,
                lower_circuit=AngelOneWSAdapter._paise(msg.get("lower_circuit_limit")) or None,
            )
        except Exception as e:
            logger.exception("AngelOneWSAdapter.parse failed for %s: %s", symbol, e)
            return None


# ---------------------------------------------------------------------------
# 5. Angel One connector — login, scrip master, WebSocket
# ---------------------------------------------------------------------------

class AngelOneConnector:
    def __init__(self, config: ScannerConfig):
        if not SMARTAPI_AVAILABLE:
            raise ImportError(
                "smartapi-python और pyotp install नहीं हैं। चलाएँ:\n"
                "    pip install -r requirements.txt"
            )
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests library install नहीं है।")

        self.config = config
        self._smart_api: Optional[SmartConnect] = None
        self._ws: Optional[SmartWebSocketV2] = None
        self._auth_token: Optional[str] = None
        self._feed_token: Optional[str] = None
        self._symbol_to_token: Dict[str, int] = {}   # "RELIANCE-EQ" → 2885
        self._token_to_symbol: Dict[int, str] = {}
        self._on_tick_cb: Optional[Callable[[Dict[str, Any]], None]] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._shutdown = threading.Event()

    # -- Step 1: Login --
    def login(self) -> None:
        creds = self.config
        if not all([creds.api_key, creds.client_code, creds.pin, creds.totp_secret]):
            raise ValueError(
                "config.json में Angel One credentials अधूरे हैं। "
                "api_key, client_code, pin, totp_secret चारों भरें।"
            )
        logger.info("Angel One login initiated for client %s", creds.client_code)
        self._smart_api = SmartConnect(api_key=creds.api_key)
        totp = pyotp.TOTP(creds.totp_secret).now()
        session = self._smart_api.generateSession(creds.client_code, creds.pin, totp)
        if not session or not session.get("status"):
            raise RuntimeError(f"Login failed: {session}")
        data = session["data"]
        self._auth_token = data["jwtToken"]
        self._feed_token = self._smart_api.getfeedToken()
        logger.info("Angel One login successful.")

    # -- Step 2: Scrip master (symbol → token) --
    def load_scrip_master(self) -> None:
        cache = Path(self.config.scrip_master_cache_path)
        cache.parent.mkdir(parents=True, exist_ok=True)
        ttl_seconds = self.config.scrip_master_ttl_hours * 3600
        need_download = True
        if cache.exists():
            age = time.time() - cache.stat().st_mtime
            if age < ttl_seconds:
                logger.info("Using cached scrip master (age=%.1fh)", age / 3600)
                need_download = False

        if need_download:
            logger.info("Downloading scrip master from Angel One...")
            r = requests.get(SCRIP_MASTER_URL, timeout=60)
            r.raise_for_status()
            cache.write_text(r.text, encoding="utf-8")
            logger.info("Scrip master downloaded (%d bytes)", len(r.text))

        data = json.loads(cache.read_text(encoding="utf-8"))
        count = 0
        for item in data:
            # Only NSE Cash equity instruments
            if item.get("exch_seg") != "NSE":
                continue
            sym = item.get("symbol", "")
            if not sym.endswith("-EQ"):
                continue
            try:
                token = int(item["token"])
            except (KeyError, ValueError):
                continue
            self._symbol_to_token[sym] = token
            self._token_to_symbol[token] = sym
            count += 1
        logger.info("Loaded %d NSE-EQ symbols in scrip master.", count)

    # -- Step 3: Resolve requested symbols --
    def resolve_tokens(self) -> Tuple[Dict[str, int], List[str]]:
        resolved: Dict[str, int] = {}
        missing: List[str] = []
        for s in self.config.symbols:
            token = self._symbol_to_token.get(s)
            if token is None:
                missing.append(s)
            else:
                resolved[s] = token
        if missing:
            logger.warning(
                "%d symbols scrip master में नहीं मिले (skipped): %s",
                len(missing), ", ".join(missing[:10]) + ("..." if len(missing) > 10 else ""),
            )
        logger.info("Resolved %d/%d symbols.", len(resolved), len(self.config.symbols))
        return resolved, missing

    # -- Step 4: Connect & subscribe WebSocket --
    def start_websocket(
        self,
        tokens: List[int],
        on_tick: Callable[[Dict[str, Any]], None],
    ) -> None:
        if not self._auth_token or not self._feed_token:
            raise RuntimeError("login() first, फिर WebSocket start करें।")

        self._on_tick_cb = on_tick

        # Chunk-safe: subscribing up to ~1000 tokens in one call is OK on Angel One
        token_strs = [str(t) for t in tokens]
        token_list = [{"exchangeType": NSE_CM_EXCHANGE_TYPE, "tokens": token_strs}]

        self._ws = SmartWebSocketV2(
            auth_token=self._auth_token,
            api_key=self.config.api_key,
            client_code=self.config.client_code,
            feed_token=self._feed_token,
        )

        correlation_id = "nse_scanner_v1"

        def on_open(wsapp):
            logger.info("WebSocket OPEN. Subscribing %d tokens (SnapQuote mode)...", len(tokens))
            try:
                self._ws.subscribe(correlation_id, SUBSCRIPTION_MODE_SNAP_QUOTE, token_list)
                logger.info("Subscription confirmed.")
            except Exception as e:
                logger.exception("Subscribe failed: %s", e)

        def on_data(wsapp, message):
            # message is already parsed by SmartWebSocketV2 into a dict
            try:
                self._on_tick_cb(message)
            except Exception as e:
                logger.exception("on_tick callback error: %s", e)

        def on_error(wsapp, error):
            logger.error("WebSocket error: %s", error)

        def on_close(wsapp):
            logger.warning("WebSocket CLOSED.")

        self._ws.on_open = on_open
        self._ws.on_data = on_data
        self._ws.on_error = on_error
        self._ws.on_close = on_close

        def _run():
            try:
                self._ws.connect()  # blocking
            except Exception as e:
                logger.exception("WebSocket connect crashed: %s", e)

        self._ws_thread = threading.Thread(target=_run, name="ws-thread", daemon=True)
        self._ws_thread.start()

    def stop(self):
        self._shutdown.set()
        try:
            if self._ws is not None:
                self._ws.close_connection()
        except Exception:
            pass


# ==========================================================================
# HIT-RATE ANALYZER
# =============================================================================
# NO REAL ORDERS PLACED. Pure measurement tool. Zero financial risk.
# Every actionable signal captured at fire time, evaluated at multiple horizons
# (default 5/15/30/60/120/300s), aggregated across state × horizon × evidence
# × regime × hour × symbol buckets, and reported with a HONEST net-edge verdict.
# =============================================================================


import argparse
import json
import logging
import logging.handlers
import signal as _signal_mod
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

# CooldownManager was previously imported from paper_trader. To keep this
# module runnable as a single-file tool (only nse_book_scanner as dependency),
# the class is inlined below.


class CooldownManager:
    """
    Whipsaw protection — enforces minimum wait after exit before re-entering
    the same symbol.

    Cooldown types:
      - Regular: after any exit, wait `cooldown_seconds` before any re-entry
        on the same symbol.
      - Direction flip: entering the OPPOSITE side within
        `cooldown_seconds × flip_multiplier` is blocked (flipping right after
        an exit is usually noise).
      - Post stop-loss: extends cooldown by `stop_loss_multiplier` when the
        last exit reason was "stop_loss".

    Thread-safe (RLock) so the WS worker and any UI thread can both read/write.
    """

    def __init__(
        self,
        cooldown_seconds: float = 120.0,
        flip_multiplier: float = 2.0,
        stop_loss_multiplier: float = 1.5,
    ):
        self.cooldown_seconds = cooldown_seconds
        self.flip_multiplier = flip_multiplier
        self.stop_loss_multiplier = stop_loss_multiplier

        self._last_exit_ts: Dict[str, float] = {}
        self._last_exit_side: Dict[str, str] = {}
        self._last_exit_reason: Dict[str, str] = {}

        self.total_exits_recorded = 0
        self.total_entries_blocked = 0
        self.blocks_by_reason: Dict[str, int] = defaultdict(int)

        self._lock = threading.RLock()

    def record_exit(self, symbol: str, side: str, reason: str, ts: float) -> None:
        with self._lock:
            self._last_exit_ts[symbol] = ts
            self._last_exit_side[symbol] = side
            self._last_exit_reason[symbol] = reason
            self.total_exits_recorded += 1

    def can_enter(self, symbol: str, side: str, ts: float) -> Tuple[bool, str]:
        with self._lock:
            last_ts = self._last_exit_ts.get(symbol)
            if last_ts is None:
                return True, "no_prior_exit"

            elapsed = ts - last_ts
            last_side = self._last_exit_side.get(symbol, "")
            last_reason = self._last_exit_reason.get(symbol, "")
            base = self.cooldown_seconds

            if last_side and last_side != side:
                required = base * self.flip_multiplier
                if elapsed < required:
                    self.total_entries_blocked += 1
                    self.blocks_by_reason["direction_flip"] += 1
                    return False, (f"flip_cooldown ({elapsed:.0f}s < "
                                   f"{required:.0f}s, last exit was {last_side})")

            if last_reason == "stop_loss":
                required = base * self.stop_loss_multiplier
                if elapsed < required:
                    self.total_entries_blocked += 1
                    self.blocks_by_reason["post_stop_loss"] += 1
                    return False, (f"post_sl_cooldown ({elapsed:.0f}s < "
                                   f"{required:.0f}s)")

            if elapsed < base:
                self.total_entries_blocked += 1
                self.blocks_by_reason["regular"] += 1
                return False, f"cooldown ({elapsed:.0f}s < {base:.0f}s)"

        return True, "cooldown_expired"

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "cooldown_seconds": self.cooldown_seconds,
                "flip_multiplier": self.flip_multiplier,
                "stop_loss_multiplier": self.stop_loss_multiplier,
                "symbols_tracked": len(self._last_exit_ts),
                "total_exits_recorded": self.total_exits_recorded,
                "total_entries_blocked": self.total_entries_blocked,
                "blocks_by_reason": dict(self.blocks_by_reason),
            }

# Optional Rich UI
try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.text import Text
    from rich.align import Align
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


logger = logging.getLogger("hit_rate_analyzer")
# NSE market hours
MARKET_OPEN_MINUTES = 9 * 60 + 15    # 09:15 IST
MARKET_CLOSE_MINUTES = 15 * 60 + 30  # 15:30 IST

# ============================================================
# 0. REAL-MARKET DEFENSIVE DIAGNOSTICS
# ============================================================
#
# Angel One SmartAPI WebSocket सटीक field names verify किए बिना बनाया गया है।
# अगर field names अलग हुए, तो messages silently None return करेंगे — user
# को पता नहीं चलेगा कि data flow नहीं हो रहा। ये classes उसी silent failure
# से बचाते हैं।

def _diagnose_parse_failure(msg: Dict[str, Any]) -> str:
    """
    Analyze WHY AngelOneWSAdapter.parse() returned None.
    Returns human-readable reason string.
    """
    if not isinstance(msg, dict):
        return "not_a_dict"

    # Common LTP field variants
    ltp_keys = ["last_traded_price", "ltp", "lastTradedPrice", "LastTradedPrice"]
    if not any(msg.get(k) for k in ltp_keys):
        return f"missing_ltp (checked: {ltp_keys})"

    ltp = msg.get("last_traded_price") or msg.get("ltp") or 0
    try:
        if float(ltp) <= 0:
            return f"ltp_non_positive ({ltp!r})"
    except (TypeError, ValueError):
        return f"ltp_not_numeric ({type(ltp).__name__}: {ltp!r})"

    # Bid/ask depth
    bid_keys = ["best_5_buy_data", "bestBids", "buy", "bids"]
    ask_keys = ["best_5_sell_data", "bestAsks", "sell", "asks"]
    has_bids = any(msg.get(k) for k in bid_keys)
    has_asks = any(msg.get(k) for k in ask_keys)
    if not has_bids and not has_asks:
        return f"missing_depth (checked bid keys: {bid_keys}, ask keys: {ask_keys})"
    if not has_bids:
        return "missing_bids"
    if not has_asks:
        return "missing_asks"

    # Check depth structure
    b = None
    for k in bid_keys:
        if msg.get(k):
            b = msg[k]
            break
    if not isinstance(b, list):
        return f"bids_not_list (type: {type(b).__name__})"
    if not b:
        return "bids_empty_list"
    if not isinstance(b[0], dict):
        return f"bid_level_not_dict (type: {type(b[0]).__name__})"
    if "price" not in b[0] and "p" not in b[0] and "Price" not in b[0]:
        return f"bid_level_missing_price_key (available keys: {list(b[0].keys())})"

    return "unknown_parse_failure"


class RawMessageDumper:
    """
    Saves first N raw WebSocket messages to a JSONL file for user inspection.
    Critical for VERIFYING that AngelOneWSAdapter's assumed field names match
    actual Angel One SmartAPI output on YOUR account/subscription.

    Use --diagnose flag to enable. First 5 messages also echoed to console.
    """

    def __init__(self, dump_path: Path, max_dumps: int = 100):
        self.dump_path = Path(dump_path)
        self.dump_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_dumps = max_dumps
        self.dump_count = 0
        self._file = None
        self._lock = threading.Lock()
        self.console_echo_count = 5

    def open(self) -> None:
        self._file = open(self.dump_path, "w", encoding="utf-8", buffering=1)
        logger.info("RawMessageDumper opened: %s (first %d messages)",
                    self.dump_path, self.max_dumps)

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None

    def dump(self, msg: Dict[str, Any], parse_success: bool,
             parse_reason: str = "") -> None:
        """Dump a raw message (only until max_dumps reached)."""
        with self._lock:
            if self.dump_count >= self.max_dumps or self._file is None:
                return
            self.dump_count += 1

            payload = {
                "seq": self.dump_count,
                "wall_ts": time.time(),
                "parse_success": parse_success,
                "parse_failure_reason": parse_reason if not parse_success else None,
                "top_level_keys": list(msg.keys()) if isinstance(msg, dict) else None,
                "raw_message": msg,
            }
            try:
                line = json.dumps(payload, separators=(",", ":"), default=str)
                self._file.write(line + "\n")
            except Exception as e:
                logger.debug("Dump write failed: %s", e)

            # Console echo for first N (helps user immediately)
            if self.dump_count <= self.console_echo_count:
                status = "✓ PARSED" if parse_success else f"✗ FAILED: {parse_reason}"
                keys = list(msg.keys()) if isinstance(msg, dict) else "N/A"
                print(f"\n[RAW MSG #{self.dump_count}] {status}")
                print(f"  Top-level keys: {keys}")
                if isinstance(msg, dict):
                    for key in ("last_traded_price", "ltp", "best_5_buy_data",
                                "total_buy_quantity", "exchange_timestamp"):
                        if key in msg:
                            val = msg[key]
                            val_str = str(val)[:80]  # truncate long
                            print(f"    {key}: {val_str}")


class DataFlowHealthMonitor:
    """
    Tracks the health of the data pipeline in real-time.

    Detects the two most common real-market failure modes:
      A) NO messages arriving at all (WebSocket subscription failed)
      B) Messages arriving but ALL failing to parse (adapter field mismatch)

    Prints loud warnings when problems are detected — silent failure prevented.
    """

    def __init__(self, expected_symbols_count: int,
                 first_tick_timeout_s: float = 60.0,
                 parse_failure_alert_pct: float = 90.0):
        self.expected_symbols = expected_symbols_count
        self.first_tick_timeout_s = first_tick_timeout_s
        self.parse_failure_alert_pct = parse_failure_alert_pct

        self.started_at = time.time()
        self.msgs_received = 0
        self.msgs_parsed_ok = 0
        self.msgs_parse_failed = 0
        self.parse_failure_reasons: Dict[str, int] = defaultdict(int)

        # Track which symbols received at least one valid tick
        self.symbols_with_data: set = set()
        self.first_tick_time: Optional[float] = None
        # `last_tick_time` is updated on EVERY successfully-parsed tick and
        # is used by the session's health thread to detect a stale WebSocket
        # feed (process alive, but no data flowing — silent failure mode).
        self.last_tick_time: Optional[float] = None

        self._first_tick_alerted = False
        self._parse_failure_alerted = False
        self._lock = threading.Lock()

    def record_message(self, symbol: Optional[str],
                       parse_success: bool, parse_reason: str = "") -> None:
        with self._lock:
            self.msgs_received += 1
            if parse_success and symbol:
                self.msgs_parsed_ok += 1
                now = time.time()
                if self.first_tick_time is None:
                    self.first_tick_time = now
                    elapsed = now - self.started_at
                    logger.info("✅ FIRST VALID TICK received after %.1fs from %s",
                                elapsed, symbol)
                self.last_tick_time = now
                self.symbols_with_data.add(symbol)
            else:
                self.msgs_parse_failed += 1
                if parse_reason:
                    self.parse_failure_reasons[parse_reason] += 1

    def seconds_since_activity(self) -> float:
        """
        Seconds since either startup or the most recent valid tick, whichever
        is later. Used by the stale-feed auto-exit guard.
        """
        with self._lock:
            baseline = self.last_tick_time if self.last_tick_time else self.started_at
        return time.time() - baseline

    def check_health_and_warn(self) -> None:
        """Called periodically from health thread. Prints loud warnings."""
        elapsed = time.time() - self.started_at

        with self._lock:
            msgs_rx = self.msgs_received
            msgs_ok = self.msgs_parsed_ok
            msgs_fail = self.msgs_parse_failed
            first_tick = self.first_tick_time
            n_symbols_data = len(self.symbols_with_data)

        # -- Warning A: NO messages arriving --
        if elapsed >= 30.0 and msgs_rx == 0 and not self._first_tick_alerted:
            self._first_tick_alerted = True
            print()
            print("=" * 72)
            print("⚠️  DATA FLOW WARNING — Zero WebSocket messages after 30 seconds")
            print("=" * 72)
            print("Possible causes:")
            print("  1. Not in NSE market hours (Mon-Fri 9:15-15:30 IST)")
            print("  2. Angel One subscription mode incorrect (need SnapQuote=3)")
            print("  3. Token resolution failed (symbols not in scrip master)")
            print("  4. Network/firewall blocking WebSocket")
            print("  5. Angel One session token expired")
            print()
            print("Check: journalctl -u nse-hitrate-analyzer -f")
            print("=" * 72)
            print()

        # -- Warning B: Messages arriving but NONE parsing --
        if elapsed >= 30.0 and msgs_rx >= 5 and msgs_ok == 0 and not self._parse_failure_alerted:
            self._parse_failure_alerted = True
            print()
            print("=" * 72)
            print("🚨 CRITICAL — Messages arriving but 100% parse failure!")
            print(f"    Messages received: {msgs_rx}, parsed: 0")
            print("=" * 72)
            print("This means AngelOneWSAdapter field names DON'T MATCH your")
            print("Angel One SmartAPI's actual message format.")
            print()
            print("Top parse failure reasons:")
            with self._lock:
                top = sorted(self.parse_failure_reasons.items(),
                             key=lambda x: -x[1])[:5]
            for reason, count in top:
                print(f"  {count:>4}x  {reason}")
            print()
            print("ACTION: Check the raw payload dump (use --diagnose flag):")
            print("  cat logs/raw_ws_dump.jsonl | head -1 | python3 -m json.tool")
            print("  → Share output with adapter maintainer to fix field names")
            print("=" * 72)
            print()

        # -- Warning C: High parse failure rate (some parse, some don't) --
        if msgs_rx >= 50 and not self._parse_failure_alerted:
            fail_pct = msgs_fail / msgs_rx * 100
            if fail_pct >= self.parse_failure_alert_pct:
                self._parse_failure_alerted = True
                print()
                print("=" * 72)
                print(f"⚠️  HIGH PARSE FAILURE RATE: {fail_pct:.1f}% "
                      f"({msgs_fail} of {msgs_rx})")
                print("=" * 72)
                print("Some messages parsing OK, most failing. Possible causes:")
                print("  - Some symbols delivering different message format")
                print("  - Occasional malformed messages from broker")
                print("  - Partial payloads during reconnect")
                print("  Check logs/raw_ws_dump.jsonl for failure examples.")
                print("=" * 72)
                print()

    def summary_report_lines(self) -> List[str]:
        """Lines for the EOD report's data-quality section."""
        with self._lock:
            msgs_rx = self.msgs_received
            msgs_ok = self.msgs_parsed_ok
            msgs_fail = self.msgs_parse_failed
            n_symbols = len(self.symbols_with_data)
            top_reasons = sorted(self.parse_failure_reasons.items(),
                                 key=lambda x: -x[1])[:5]
            first_tick = self.first_tick_time

        lines = []
        lines.append(f"  Raw messages received : {msgs_rx:>10,}")
        lines.append(f"  Parsed successfully   : {msgs_ok:>10,}  "
                     f"({msgs_ok/max(msgs_rx,1)*100:.1f}%)")
        lines.append(f"  Parse failures        : {msgs_fail:>10,}  "
                     f"({msgs_fail/max(msgs_rx,1)*100:.1f}%)")
        lines.append(f"  Symbols with data     : {n_symbols:>10,} "
                     f"of {self.expected_symbols} expected")

        if first_tick:
            elapsed_to_first = first_tick - self.started_at
            lines.append(f"  Time to first tick    : {elapsed_to_first:>10.1f} s")
        else:
            lines.append("  Time to first tick    :  NEVER RECEIVED  ⚠")

        if top_reasons:
            lines.append("")
            lines.append("  Top parse failure reasons:")
            for reason, count in top_reasons:
                lines.append(f"    {count:>6,}x  {reason}")

        return lines


# ============================================================
# 1. DATA CLASSES
# ============================================================

@dataclass
class PendingPrediction:
    """One independent signal event awaiting executable horizon evaluation."""
    symbol: str
    state: str
    smoothed_score: float
    evidence: float
    regime_label: str
    price_at_signal: float          # executable entry fill, not LTP
    ltp_at_signal: float
    best_bid_at_signal: float
    best_ask_at_signal: float
    bid_qty_at_signal: int
    ask_qty_at_signal: int
    spread_bps_at_signal: float
    ts_fired: float
    horizon_seconds: float
    hour_of_day: int


@dataclass
class HitRateBucket:
    """Aggregate stats for one bucket dimension (state, horizon, etc.)."""
    count: int = 0
    hits: int = 0                 # directional_return > 0
    net_profitable: int = 0       # directional_return > cost (actually profitable)
    sum_return: float = 0.0
    sum_return_sq: float = 0.0
    sum_net_return: float = 0.0
    max_win: float = 0.0
    max_loss: float = 0.0

    def add(self, directional_return: float, cost: float) -> None:
        """Add one evaluated prediction. directional_return: signed such that
        positive = signal was correct direction."""
        self.count += 1
        if directional_return > 0:
            self.hits += 1
        net = directional_return - cost
        if net > 0:
            self.net_profitable += 1
        self.sum_return += directional_return
        self.sum_return_sq += directional_return * directional_return
        self.sum_net_return += net
        if directional_return > self.max_win:
            self.max_win = directional_return
        if directional_return < self.max_loss:
            self.max_loss = directional_return

    @property
    def hit_rate(self) -> float:
        return self.hits / self.count if self.count else 0.0

    @property
    def net_profit_rate(self) -> float:
        return self.net_profitable / self.count if self.count else 0.0

    @property
    def avg_return(self) -> float:
        return self.sum_return / self.count if self.count else 0.0

    @property
    def avg_net_edge(self) -> float:
        return self.sum_net_return / self.count if self.count else 0.0

    @property
    def std_return(self) -> float:
        if self.count < 2:
            return 0.0
        mean = self.avg_return
        var = self.sum_return_sq / self.count - mean * mean
        return var ** 0.5 if var > 0 else 0.0

    @property
    def sharpe_proxy(self) -> float:
        std = self.std_return
        return self.avg_return / std if std > 1e-9 else 0.0


# ============================================================
# 1b. LIVE SIGNAL — Real-time P&L tracking (per signal, not per horizon)
# ============================================================

@dataclass
class LiveSignal:
    """
    One in-flight signal being tracked in real-time.

    Updated on every incoming tick for its symbol until max horizon reached.
    Tracks:
      - Current directional return (positive = signal is proving RIGHT)
      - MFE (max favorable excursion) — best moment signal was proved right
      - MAE (max adverse excursion) — worst moment signal was proved wrong
      - Which configured horizons have already been crossed + their returns

    यह tool user को हर tick पर बताता है: "यह signal अभी सही जा रहा है या गलत?"
    """
    signal_id: int
    symbol: str
    state: str
    smoothed_score: float
    evidence: float
    regime_label: str
    price_at_signal: float
    ts_fired: float
    max_horizon_s: float                    # जब यह close होगा

    # Live state (updated per tick)
    current_price: float = 0.0
    current_directional_return: float = 0.0 # +ve = correct, -ve = wrong
    seconds_elapsed: float = 0.0
    tick_count: int = 0                      # ticks seen since fire

    # Excursions
    max_favorable_excursion: float = 0.0    # best signed return during life
    max_adverse_excursion: float = 0.0      # worst signed return during life
    # Sentinel -1.0 means "never happened yet" — distinguishes:
    #   MFE=0.0, t_mfe=-1.0  → signal never went positive (still losing)
    #   MFE=0.001, t_mfe=5.0 → best moment was 5s in, at +0.1%
    time_to_mfe_s: float = -1.0
    time_to_mae_s: float = -1.0

    # Horizon snapshots (filled as each horizon is crossed)
    horizon_snapshots: Dict[float, float] = field(default_factory=dict)
    # e.g., {5.0: +0.0012, 30.0: +0.0025, 60.0: -0.0003}

    # -- 15-second SURVIVAL EXIT (one-shot check) --
    # After `survival_check_seconds` elapsed, if MFE so far is below
    # `survival_min_favor_pct`, the signal is force-closed as if squared off.
    # Values are set exactly once, on the tick that crosses the survival mark.
    survival_checked: bool = False
    survival_passed: bool = False                # True if MFE >= min_favor
    survival_directional_return: float = 0.0     # dir return at survival mark
    survival_exit_ts: float = -1.0               # ts at survival mark

    # Terminal state
    is_closed: bool = False
    close_reason: str = ""                   # "max_horizon" / "timeout" / "survival_exit"

    @property
    def is_currently_winning(self) -> bool:
        return self.current_directional_return > 0

    def is_currently_net_profitable(self, cost_pct: float) -> bool:
        """True if current directional return exceeds transaction cost."""
        return self.current_directional_return > cost_pct

    def net_return(self, cost_pct: float) -> float:
        return self.current_directional_return - cost_pct


class LiveSignalMonitor:
    """
    Real-time tracker for open signals.

    On every tick for a symbol:
      - Update all open signals for that symbol
      - Compute directional return, elapsed, excursions
      - Snapshot at each horizon crossing
      - Close (evict) when max horizon reached

    Separate from HitRateAnalyzer's per-horizon evaluation:
      - HitRateAnalyzer stats: WHERE horizon expired (statistical aggregates)
      - LiveSignalMonitor: WHAT's happening NOW (real-time visibility)
    """

    def __init__(
        self,
        horizons_s: List[float],
        cost_model: ExecutionCostModel,
        max_age_s: float = 600.0,
        survival_check_seconds: float = 0.0,
        survival_min_favor_pct: float = 0.0001,
    ):
        self.horizons = sorted(float(h) for h in horizons_s)
        self.max_horizon = max(self.horizons) if self.horizons else 60.0
        self.cost_model = cost_model
        self.cost = cost_model.transaction_cost_pct
        self.max_age_s = max(max_age_s, self.max_horizon + 30.0)
        # One-shot 15-second survival check (0 disables it).
        self.survival_check_seconds = float(survival_check_seconds)
        self.survival_min_favor_pct = float(survival_min_favor_pct)
        self.total_survival_passed = 0
        self.total_survival_failed = 0
        # Callback fired once when a signal is closed by the survival exit.
        # HitRateAnalyzer wires this up to feed its policy bucket.
        self.survival_exit_callback = None  # type: Optional[Any]

        # Open signals keyed by signal_id
        self._open: Dict[int, LiveSignal] = {}
        # Per-symbol index for fast on_tick lookup
        self._by_symbol: Dict[str, List[int]] = defaultdict(list)
        self._counter = 0

        # Recently closed signals (ring buffer for UI + report)
        self.recently_closed: Deque[LiveSignal] = deque(maxlen=200)

        # Global running stats
        self.total_added = 0
        self.total_closed = 0
        self.total_max_horizon_closed = 0

        # Concurrent access from worker + UI thread
        self._lock = threading.RLock()

    def add_signal(
        self, symbol: str, state: str, smoothed_score: float,
        evidence: float, regime_label: str,
        price: float, ts: float,
    ) -> int:
        """Called when scanner fires an actionable signal. Returns new signal_id."""
        with self._lock:
            self._counter += 1
            sig = LiveSignal(
                signal_id=self._counter,
                symbol=symbol,
                state=state,
                smoothed_score=smoothed_score,
                evidence=evidence,
                regime_label=regime_label,
                price_at_signal=price,
                ts_fired=ts,
                max_horizon_s=self.max_horizon,
                current_price=price,
                current_directional_return=0.0,
            )
            self._open[sig.signal_id] = sig
            self._by_symbol[symbol].append(sig.signal_id)
            self.total_added += 1
        return sig.signal_id

    def on_tick(
        self,
        symbol: str,
        current_price: float,
        current_ts: float,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
    ) -> List[LiveSignal]:
        """
        Update all open signals using the currently executable exit quote.
        Returns signals closed on this tick (max horizon reached).
        """
        if current_price <= 0:
            return []

        newly_closed: List[LiveSignal] = []
        with self._lock:
            ids_for_symbol = self._by_symbol.get(symbol)
            if not ids_for_symbol:
                return []

            # Iterate a copy so we can mutate _open safely
            still_open_ids = []
            for sig_id in ids_for_symbol:
                sig = self._open.get(sig_id)
                if sig is None:
                    continue   # already closed elsewhere

                # Mark each signal at its executable exit side (LONG→bid,
                # SHORT→ask), including configured latency slippage.
                side = "SHORT" if sig.state in _SHORT_STATES else "LONG"
                exit_price = self.cost_model.fill_price(
                    side=side, is_entry=False, ltp=current_price,
                    best_bid=best_bid, best_ask=best_ask,
                )
                directional = self.cost_model.gross_directional_return(
                    side, sig.price_at_signal, exit_price,
                )
                elapsed = current_ts - sig.ts_fired

                sig.current_price = exit_price
                sig.current_directional_return = directional
                sig.seconds_elapsed = elapsed
                sig.tick_count += 1

                # Update excursions.
                # For MFE: only track values strictly > 0 as "the signal was proved right at some point".
                # For MAE: only track values strictly < 0.
                # This way MFE=0.0 with time_to_mfe=-1.0 means "signal never went positive".
                if directional > 0 and directional > sig.max_favorable_excursion:
                    sig.max_favorable_excursion = directional
                    sig.time_to_mfe_s = elapsed
                if directional < 0 and directional < sig.max_adverse_excursion:
                    sig.max_adverse_excursion = directional
                    sig.time_to_mae_s = elapsed

                # Snapshot at each horizon crossing (only once per horizon)
                for h in self.horizons:
                    if h not in sig.horizon_snapshots and elapsed >= h:
                        sig.horizon_snapshots[h] = directional

                # -- 15-second SURVIVAL EXIT (one-shot) --
                # After survival_check_seconds elapsed, check MFE once:
                # if it never reached the min-favor threshold, force close
                # the signal here (as if squared off flat/breakeven).
                survival_active = (
                    self.survival_check_seconds > 0
                    and not sig.survival_checked
                    and elapsed >= self.survival_check_seconds
                )
                if survival_active:
                    sig.survival_checked = True
                    sig.survival_directional_return = directional
                    sig.survival_exit_ts = current_ts
                    passed = (
                        sig.max_favorable_excursion >= self.survival_min_favor_pct
                    )
                    sig.survival_passed = passed
                    if passed:
                        self.total_survival_passed += 1
                    else:
                        self.total_survival_failed += 1
                        # Emit policy outcome BEFORE closing the signal.
                        if self.survival_exit_callback is not None:
                            try:
                                self.survival_exit_callback(
                                    sig, directional, current_ts,
                                )
                            except Exception as e:  # never break the tick loop
                                logger.debug("survival callback error: %s", e)
                        sig.is_closed = True
                        sig.close_reason = "survival_exit"
                        del self._open[sig_id]
                        self.recently_closed.append(sig)
                        newly_closed.append(sig)
                        self.total_closed += 1
                        continue

                # Close condition — max horizon reached.
                # Note: an older `elif elapsed > self.max_age_s: close as
                # "timeout"` branch was removed. Because max_age_s is
                # forced to `max(user_val, max_horizon + 30)` and each
                # signal's max_horizon_s == self.max_horizon, the timeout
                # branch was unreachable in practice.
                if elapsed >= sig.max_horizon_s:
                    sig.is_closed = True
                    sig.close_reason = "max_horizon"
                    del self._open[sig_id]
                    self.recently_closed.append(sig)
                    newly_closed.append(sig)
                    self.total_closed += 1
                    self.total_max_horizon_closed += 1
                else:
                    still_open_ids.append(sig_id)

            # Compact per-symbol index
            self._by_symbol[symbol] = still_open_ids

        return newly_closed

    # -- Read APIs (UI + report) --

    def open_count(self) -> int:
        with self._lock:
            return len(self._open)

    def snapshot_open(self, top_n: int = 40) -> List[LiveSignal]:
        """
        Return DEEP-COPIED snapshot of currently-open signals, sorted newest-first.

        Deep copy is critical: UI thread renders these while worker thread
        may be updating fields (current_price, MFE, MAE) simultaneously.
        Without copy, UI could see torn state (e.g., new price but old
        directional return computed from previous price).
        """
        with self._lock:
            # Create isolated copies while holding the lock — safe read of all fields.
            # dataclasses.replace creates a proper copy of the dataclass fields.
            # We also copy the horizon_snapshots dict explicitly (dict is mutable).
            frozen = []
            for sig in self._open.values():
                copy = LiveSignal(
                    signal_id=sig.signal_id,
                    symbol=sig.symbol,
                    state=sig.state,
                    smoothed_score=sig.smoothed_score,
                    evidence=sig.evidence,
                    regime_label=sig.regime_label,
                    price_at_signal=sig.price_at_signal,
                    ts_fired=sig.ts_fired,
                    max_horizon_s=sig.max_horizon_s,
                    current_price=sig.current_price,
                    current_directional_return=sig.current_directional_return,
                    seconds_elapsed=sig.seconds_elapsed,
                    tick_count=sig.tick_count,
                    max_favorable_excursion=sig.max_favorable_excursion,
                    max_adverse_excursion=sig.max_adverse_excursion,
                    time_to_mfe_s=sig.time_to_mfe_s,
                    time_to_mae_s=sig.time_to_mae_s,
                    horizon_snapshots=dict(sig.horizon_snapshots),  # dict copy
                    survival_checked=sig.survival_checked,
                    survival_passed=sig.survival_passed,
                    survival_directional_return=sig.survival_directional_return,
                    survival_exit_ts=sig.survival_exit_ts,
                    is_closed=sig.is_closed,
                    close_reason=sig.close_reason,
                )
                frozen.append(copy)
        # Sort newest-first (outside lock — safe on isolated list)
        frozen.sort(key=lambda s: -s.ts_fired)
        return frozen[:top_n]

    def live_verdict(self) -> Dict[str, Any]:
        """
        Aggregate 'how are we doing right now?' stats across open signals.

        Computes all counters INSIDE the lock so worker thread cannot mutate
        current_directional_return between counts (would give inconsistent
        winning + losing + flat counts otherwise).

        Notes on definitions:
          - winning: strictly current_directional_return > 0
          - losing:  strictly current_directional_return < 0
          - flat:    exactly 0 (not counted in either — total_open = winning + losing + flat)
          - hit_rate_pct: winning / total_open × 100
            (denominator includes flat — same convention as trade-book stats)
        """
        with self._lock:
            n = len(self._open)
            if n == 0:
                return {
                    "total_open": 0, "winning": 0, "losing": 0, "flat": 0,
                    "net_profitable": 0,
                    "hit_rate_pct": 0.0, "net_profit_rate_pct": 0.0,
                    "avg_current_return_pct": 0.0,
                }
            winning = 0; losing = 0; flat = 0; net_prof = 0; total_ret = 0.0
            for s in self._open.values():
                r = s.current_directional_return
                if r > 0: winning += 1
                elif r < 0: losing += 1
                else: flat += 1
                charges = self.cost_model.charge_return(s.price_at_signal, s.current_price)
                if r > charges:
                    net_prof += 1
                total_ret += r
        return {
            "total_open": n,
            "winning": winning, "losing": losing, "flat": flat,
            "net_profitable": net_prof,
            "hit_rate_pct": winning / n * 100,
            "net_profit_rate_pct": net_prof / n * 100,
            "avg_current_return_pct": (total_ret / n) * 100,
        }

    @staticmethod
    def _true_median(values: List[float]) -> float:
        """Proper median: for even n, average the two middle values."""
        n = len(values)
        if n == 0:
            return 0.0
        s = sorted(values)
        if n % 2 == 1:
            return s[n // 2]
        return (s[n // 2 - 1] + s[n // 2]) / 2.0

    def excursion_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Aggregate MFE/MAE stats by state — useful for TP/SL calibration.

        IMPORTANT: time_to_mfe_s / time_to_mae_s sentinel -1.0 means
        "never happened" — these MUST be excluded from the time averages,
        otherwise the average is polluted downward and TP calibration
        hints become misleading.
        """
        with self._lock:
            closed_snap = list(self.recently_closed)
        if not closed_snap:
            return {}
        by_state: Dict[str, List[LiveSignal]] = defaultdict(list)
        for sig in closed_snap:
            by_state[sig.state].append(sig)

        result: Dict[str, Dict[str, Any]] = {}
        for state, sigs in by_state.items():
            if not sigs:
                continue
            n = len(sigs)
            mfes = [s.max_favorable_excursion for s in sigs]
            maes = [s.max_adverse_excursion for s in sigs]
            final_rets = [s.current_directional_return for s in sigs]

            # Exclude sentinels (-1.0 = "never MFE'd/MAE'd") from time averages
            valid_times_mfe = [s.time_to_mfe_s for s in sigs if s.time_to_mfe_s >= 0]
            valid_times_mae = [s.time_to_mae_s for s in sigs if s.time_to_mae_s >= 0]
            n_had_mfe = len(valid_times_mfe)
            n_had_mae = len(valid_times_mae)

            result[state] = {
                "n": n,
                "n_had_mfe": n_had_mfe,   # how many signals ever went positive
                "n_had_mae": n_had_mae,   # how many ever went negative
                "avg_mfe_pct": (sum(mfes) / n) * 100,
                "avg_mae_pct": (sum(maes) / n) * 100,
                # Time averages ONLY over signals that actually MFE'd/MAE'd
                "avg_time_to_mfe_s": (sum(valid_times_mfe) / n_had_mfe) if n_had_mfe else -1.0,
                "avg_time_to_mae_s": (sum(valid_times_mae) / n_had_mae) if n_had_mae else -1.0,
                "avg_final_return_pct": (sum(final_rets) / n) * 100,
                # True median (avg of two middle values for even n)
                "median_mfe_pct": self._true_median(mfes) * 100,
                "median_mae_pct": self._true_median(maes) * 100,
            }
        return result


# ============================================================
# 2. HIT RATE ANALYZER
# ============================================================

class HitRateAnalyzer:
    """
    Multi-dimensional signal accuracy tracker.

    For every actionable signal that fires, creates pending records at each
    configured horizon (default: 5s, 15s, 30s, 60s, 120s, 300s). Each pending
    record is evaluated at its horizon by comparing signal-fire price to
    current LTP. Result stored across 5 dimensions of buckets for analysis.
    """

    def __init__(
        self,
        horizons_s: List[float],
        transaction_cost_pct: float = 0.0006,
        latency_slippage_bps: float = 0.0,
        log_path: str = "logs/hit_rate_predictions.jsonl",
        max_pending_age_s: float = 600.0,
        min_samples_for_verdict: int = 20,
        signal_dedup_seconds: float = 5.0,
        cooldown: Optional["CooldownManager"] = None,
        # ---- Optional signal-quality gates (all default to disabled) ----
        session_manager: Optional[SessionStateManager] = None,
        allowed_phases: Optional[FrozenSet[SessionPhase]] = None,
        rvol_calculator: Optional[RVOLCalculator] = None,
        min_rvol: float = 0.0,
        # -- 15-second RULES (Gemini "Sniper" policy) --
        # entry_confirmation_seconds: signal is recorded only after score
        # has continuously qualified for this many seconds. 0 disables.
        # survival_check_seconds / survival_min_favor_pct: one-shot MFE
        # check applied to each recorded signal; if MFE at that mark is
        # below the threshold, signal is closed there and its outcome is
        # recorded in the policy bucket.
        entry_confirmation_seconds: float = 0.0,
        entry_score_threshold: Optional[float] = None,
        entry_min_evidence: float = 0.0,
        survival_check_seconds: float = 0.0,
        survival_min_favor_pct: float = 0.0001,   # 0.01% MFE required
    ):
        self.horizons = sorted(float(h) for h in horizons_s)
        self.execution_model = ExecutionCostModel(
            transaction_cost_pct=transaction_cost_pct,
            latency_slippage_bps=latency_slippage_bps,
        )
        # Compatibility alias used by UI/report code; spread is modeled
        # separately through executable bid/ask fills.
        self.cost = self.execution_model.transaction_cost_pct
        self.min_samples = min_samples_for_verdict
        self.signal_dedup_seconds = signal_dedup_seconds

        # Optional cooldown manager (shared with PaperExecutor in dual mode).
        # If set, HitRateAnalyzer only records signals that pass cooldown gate
        # → apples-to-apples comparison with paper trading stats.
        self.cooldown: Optional["CooldownManager"] = cooldown
        self.signals_blocked_by_cooldown: int = 0

        # -- Session phase gate --
        # When session_manager is set, signals fired during non-allowed
        # phases (LUNCH by default, plus PRE_OPEN and CLOSING) are dropped.
        # This filters out low-quality signals from times when book
        # dynamics don't reflect fundamentals (lunch = thin liquidity;
        # closing = squaring off, not directional intent).
        self.session_manager: Optional[SessionStateManager] = session_manager
        self.allowed_phases: FrozenSet[SessionPhase] = (
            allowed_phases if allowed_phases is not None
            else DEFAULT_TRADEABLE_PHASES
        )
        self.signals_blocked_by_session: int = 0

        # -- RVOL gate --
        # When rvol_calculator is set AND min_rvol > 0, signals fired when
        # current RVOL for the symbol is below `min_rvol` are dropped
        # (or when RVOL is unavailable — warm-up phase — signals are
        # allowed by default, but this can be flipped via strict mode).
        self.rvol_calculator: Optional[RVOLCalculator] = rvol_calculator
        self.min_rvol: float = float(min_rvol)
        # If True, block signals when RVOL is None (warm-up).
        # Default False: allow warm-up signals but count them separately.
        self.strict_rvol_warmup: bool = False
        self.signals_blocked_by_low_rvol: int = 0
        self.signals_allowed_during_rvol_warmup: int = 0

        # -- State filter --
        # By default all ACTIONABLE states (WEAK/LONG/STRONG on both sides).
        # User can restrict to STRONG only, or STRONG+LONG (skip WEAK).
        # Set via attribute after __init__ (kept out of signature to avoid
        # bloat).
        self.allowed_signal_states: set = set(_ACTIONABLE_STATES)
        self.signals_blocked_by_state_filter: int = 0

        # Guard: max_pending_age MUST be greater than the largest horizon,
        # otherwise long-horizon predictions would silently timeout before
        # reaching their evaluation point, biasing all statistics.
        # We enforce max_pending_age ≥ max_horizon + 30s buffer.
        max_h = max(self.horizons) if self.horizons else 60.0
        if max_pending_age_s < max_h + 30.0:
            logger.warning(
                "max_pending_age_s (%.1f) is less than max_horizon (%.1f) + 30s buffer. "
                "Auto-adjusting to %.1f to prevent premature timeouts.",
                max_pending_age_s, max_h, max_h + 30.0,
            )
            max_pending_age_s = max_h + 30.0
        self.max_pending_age = max_pending_age_s

        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

        # Dedup state — last recorded signal per (symbol, state)
        # Same signal firing repeatedly (score above threshold for many ticks)
        # would create massive duplicate predictions. Dedup: only record if
        # state changed OR N seconds have passed since last record for this
        # symbol+state combo.
        self._dedup_last_ts: Dict[Tuple[str, str], float] = {}
        self._dedup_last_state: Dict[str, str] = {}
        self.signals_deduped: int = 0

        # Concurrent access lock (worker updates + UI reads)
        self._pending_lock = threading.RLock()

        # Pending predictions per symbol (per-horizon, for statistical eval)
        self._pending: Dict[str, Deque[PendingPrediction]] = defaultdict(deque)

        # -- Entry confirmation state (15-second persistence rule) --
        self.entry_confirmation_seconds = max(0.0, float(entry_confirmation_seconds))
        # Minimum |score| / evidence a signal must maintain to keep its
        # pending confirmation alive. None = accept anything actionable.
        self.entry_score_threshold: Optional[float] = (
            float(entry_score_threshold) if entry_score_threshold is not None else None
        )
        self.entry_min_evidence = float(entry_min_evidence)
        self._pending_confirmations: Dict[str, Dict[str, Any]] = {}
        self._entry_armed: Dict[str, bool] = defaultdict(lambda: True)
        self.confirmations_started: int = 0
        self.confirmations_cancelled: int = 0
        self.confirmations_passed: int = 0

        # -- Real-time live signal monitor (per-signal, for live UI) --
        self.live_monitor = LiveSignalMonitor(
            horizons_s=self.horizons,
            cost_model=self.execution_model,
            max_age_s=max_pending_age_s,
            survival_check_seconds=survival_check_seconds,
            survival_min_favor_pct=survival_min_favor_pct,
        )
        # When the monitor closes a signal via the survival exit, record its
        # outcome in the policy bucket (and drop pending horizons).
        self.live_monitor.survival_exit_callback = self._on_survival_exit

        # Multi-dimensional stats buckets
        self._stats_lock = threading.RLock()
        self._stats_state_horizon: Dict[Tuple[str, float], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_evidence: Dict[Tuple[str, str], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_regime: Dict[Tuple[str, str], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_hour: Dict[Tuple[str, int], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_symbol: Dict[Tuple[str, str], HitRateBucket] = defaultdict(HitRateBucket)
        # Per-signal outcome under the 15-second policy (survival exit OR
        # max horizon exit — one outcome per confirmed signal).
        self._stats_policy: Dict[str, HitRateBucket] = defaultdict(HitRateBucket)
        self.policy_survival_exits: int = 0
        self.policy_max_horizon_exits: int = 0
        # Track which signals have already been counted in the policy bucket
        # (a signal reaches policy either via survival_exit callback OR when
        # it closes normally at max_horizon).
        self._policy_counted: Set[int] = set()
        self._open_signal_states: Dict[int, str] = {}

        # Log file
        self._log_file = None
        self._log_lock = threading.Lock()

        # Counters
        self.signals_recorded = 0
        self.predictions_evaluated = 0
        self.predictions_timed_out = 0

    def open(self) -> None:
        self._log_file = open(self.log_path, "a", encoding="utf-8", buffering=1)
        logger.info("HitRateAnalyzer opened. Log: %s", self.log_path)
        logger.info("Horizons: %s, cost: %.4f%%",
                    self.horizons, self.cost * 100)

    def close(self) -> None:
        with self._log_lock:
            if self._log_file:
                self._log_file.close()
                self._log_file = None

    @staticmethod
    def _evidence_bucket(evidence: float) -> str:
        if evidence < 30: return "0-30"
        elif evidence < 50: return "30-50"
        elif evidence < 70: return "50-70"
        else: return "70+"

    # -- 15-second entry confirmation gate --

    def _qualifies_for_entry(self, state: str, result: SignalResult) -> bool:
        """True while the score/state should keep a pending confirmation alive."""
        if state not in _ACTIONABLE_STATES:
            return False
        if state not in self.allowed_signal_states:
            return False
        if self.entry_score_threshold is not None:
            if abs(result.smoothed_score) < self.entry_score_threshold:
                return False
        if result.evidence_strength < self.entry_min_evidence:
            return False
        return True

    def _cancel_pending_confirmation(self, symbol: str) -> None:
        if self._pending_confirmations.pop(symbol, None) is not None:
            self.confirmations_cancelled += 1
        self._entry_armed[symbol] = True

    def _confirmation_matured(
        self, symbol: str, state: str, result: SignalResult, ts: float,
    ) -> bool:
        """
        Track a rolling pending confirmation. Returns True only on the tick
        where the score has been continuously qualifying for at least
        `entry_confirmation_seconds`. Direction flips cancel the pending
        confirmation and re-arm the symbol.
        """
        side = "LONG" if state in _LONG_STATES else "SHORT"
        pending = self._pending_confirmations.get(symbol)
        if pending is None or pending["side"] != side:
            if pending is not None:
                self.confirmations_cancelled += 1
            elif not self._entry_armed[symbol]:
                # Same side already confirmed once; wait for score to drop
                # below threshold before starting a fresh confirmation.
                return False
            self._pending_confirmations[symbol] = {
                "side": side,
                "started_ts": ts,
                "last_seen_ts": ts,
                "state": state,
                "score": result.smoothed_score,
                "evidence": result.evidence_strength,
            }
            self._entry_armed[symbol] = False
            self.confirmations_started += 1
            return False

        pending["last_seen_ts"] = ts
        pending["state"] = state
        pending["score"] = result.smoothed_score
        pending["evidence"] = result.evidence_strength
        if ts - pending["started_ts"] < self.entry_confirmation_seconds:
            return False

        # Matured — clear so we don't fire again on the next same-side tick.
        self._pending_confirmations.pop(symbol, None)
        self.confirmations_passed += 1
        return True

    def _on_survival_exit(
        self, sig: LiveSignal, directional_return: float, current_ts: float,
    ) -> None:
        """
        Called by LiveSignalMonitor when a signal is force-closed at the
        survival mark. Feed the policy bucket with this single outcome.
        """
        if sig.signal_id in self._policy_counted:
            return
        self._policy_counted.add(sig.signal_id)
        # Estimate round-trip charges on the survival exit price.
        # (We don't have entry_price handy here except via sig.price_at_signal.)
        charge_return = self.execution_model.charge_return(
            sig.price_at_signal, sig.current_price,
        )
        with self._stats_lock:
            self._stats_policy[sig.state].add(directional_return, charge_return)
        self.policy_survival_exits += 1
        self._open_signal_states.pop(sig.signal_id, None)

    def record_signal(
        self, symbol: str, result: SignalResult, price: float, ts: float,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
        bid_qty: int = 0,
        ask_qty: int = 0,
    ) -> None:
        """
        Called when scanner fires an actionable signal.

        DEDUP RULE (critical): A scanner signal state can persist for many
        consecutive ticks (e.g., STRONG_LONG stays STRONG_LONG for 60s if
        book conditions don't reverse). Without dedup, we'd create hundreds
        of duplicate predictions per symbol per second.

        Recording rules:
          1. If state changed vs last recorded for this symbol → record
             (new signal event)
          2. If same state but > signal_dedup_seconds elapsed → record
             (treat as fresh signal after cooldown)
          3. Else → skip (deduped)
        """
        state = result.state.value
        if price <= 0:
            return

        # -- 15-SECOND ENTRY CONFIRMATION GATE (runs first so cancellation
        # sees non-qualifying states too) --
        if self.entry_confirmation_seconds > 0:
            if not self._qualifies_for_entry(state, result):
                self._cancel_pending_confirmation(symbol)
                return
            if not self._confirmation_matured(symbol, state, result, ts):
                return

        if state not in _ACTIONABLE_STATES:
            return

        # -- STATE FILTER --
        # Skip signals whose state isn't in the user-configured allowed set.
        # Default: all ACTIONABLE states. User can restrict to STRONG only.
        if state not in self.allowed_signal_states:
            self.signals_blocked_by_state_filter += 1
            return

        # -- DEDUP GATE --
        prev_state = self._dedup_last_state.get(symbol)
        state_changed = (prev_state != state)
        last_recorded_ts = self._dedup_last_ts.get((symbol, state), 0.0)
        time_since_last = ts - last_recorded_ts

        if not state_changed and time_since_last < self.signal_dedup_seconds:
            self.signals_deduped += 1
            return

        # -- COOLDOWN CHECK (after dedup, before recording) --
        # Same cooldown as PaperExecutor uses — ensures both analyzers
        # measure the same set of "tradeable" signals in dual mode.
        if self.cooldown is not None:
            side = "LONG" if state in _LONG_STATES else "SHORT"
            allowed, _ = self.cooldown.can_enter(symbol, side, ts)
            if not allowed:
                self.signals_blocked_by_cooldown += 1
                return

        # -- SESSION PHASE GATE --
        # Skip signals fired during non-tradeable phases (LUNCH etc.).
        # is_tradeable() also enforces the "no new entry after 15:15" cutoff.
        if self.session_manager is not None:
            ok, _reason = self.session_manager.is_tradeable(
                ts, allowed_phases=self.allowed_phases,
                enforce_no_new_entry_cutoff=True,
            )
            if not ok:
                self.signals_blocked_by_session += 1
                return

        # -- RVOL GATE --
        # Skip signals fired during low-volume periods.
        # Only active when min_rvol > 0 AND rvol_calculator is set.
        if self.rvol_calculator is not None and self.min_rvol > 0.0:
            current_rvol = self.rvol_calculator.get_rvol(symbol, ts)
            if current_rvol is None:
                # RVOL not yet warmed up for this symbol
                if self.strict_rvol_warmup:
                    self.signals_blocked_by_low_rvol += 1
                    return
                self.signals_allowed_during_rvol_warmup += 1
            elif current_rvol < self.min_rvol:
                self.signals_blocked_by_low_rvol += 1
                return

        # Executable entry: LONG crosses ask, SHORT hits bid. Optional latency
        # slippage is modeled separately from explicit transaction charges.
        side = "LONG" if state in _LONG_STATES else "SHORT"
        try:
            entry_price = self.execution_model.fill_price(
                side=side, is_entry=True, ltp=price,
                best_bid=best_bid, best_ask=best_ask,
            )
        except ValueError:
            return
        if best_bid and best_ask and best_bid > 0 and best_ask >= best_bid:
            mid = (best_bid + best_ask) / 2.0
            spread_bps = (best_ask - best_bid) / mid * 10000.0 if mid > 0 else 0.0
        else:
            spread_bps = 0.0

        # Update dedup memory (record this event)
        self._dedup_last_ts[(symbol, state)] = ts
        self._dedup_last_state[symbol] = state

        # Hour of day in IST — defensive against malformed timestamps
        # (OverflowError for years > 9999, OSError on Windows for negatives,
        # ValueError for other invalid values)
        try:
            hour = datetime.fromtimestamp(ts, tz=IST).hour
        except (ValueError, OSError, OverflowError):
            hour = -1

        # Regime label (Phase 2)
        regime_label = getattr(result.metrics.regime, "label", "unknown")

        # Create pending at each horizon (for statistical horizon-based eval)
        with self._pending_lock:
            for h in self.horizons:
                self._pending[symbol].append(PendingPrediction(
                    symbol=symbol, state=state,
                    smoothed_score=result.smoothed_score,
                    evidence=result.evidence_strength,
                    regime_label=regime_label,
                    price_at_signal=entry_price,
                    ltp_at_signal=price,
                    best_bid_at_signal=float(best_bid or 0.0),
                    best_ask_at_signal=float(best_ask or 0.0),
                    bid_qty_at_signal=int(bid_qty),
                    ask_qty_at_signal=int(ask_qty),
                    spread_bps_at_signal=spread_bps,
                    ts_fired=ts,
                    horizon_seconds=h,
                    hour_of_day=hour,
                ))

        # Also add to live monitor (per-signal, for real-time UI)
        signal_id = self.live_monitor.add_signal(
            symbol=symbol,
            state=state,
            smoothed_score=result.smoothed_score,
            evidence=result.evidence_strength,
            regime_label=regime_label,
            price=entry_price,
            ts=ts,
        )
        # Remember state so policy bucket can attribute max-horizon closures
        # even after the LiveSignal is popped from `_open`.
        self._open_signal_states[signal_id] = state

        self.signals_recorded += 1

    def on_tick(
        self, symbol: str, current_price: float, current_ts: float,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
        bid_qty: int = 0,
        ask_qty: int = 0,
    ) -> None:
        """
        Called on every tick. Two things happen:
          1. LiveSignalMonitor.on_tick — updates real-time state of ALL open
             signals for this symbol (current P&L, MFE, MAE, horizon snapshots)
          2. Evaluate any pending predictions whose horizons have expired
             → statistical stats update
        """
        # Real-time update (fast, always run). newly_closed contains signals
        # that either reached max_horizon, timed out, OR were closed by the
        # survival-exit rule (already accounted for via callback).
        newly_closed = self.live_monitor.on_tick(
            symbol, current_price, current_ts,
            best_bid=best_bid, best_ask=best_ask,
        )
        for sig in newly_closed:
            if sig.close_reason == "survival_exit":
                continue  # already added to policy bucket via callback
            if sig.signal_id in self._policy_counted:
                continue
            self._policy_counted.add(sig.signal_id)
            state = self._open_signal_states.pop(sig.signal_id, sig.state)
            charge_return = self.execution_model.charge_return(
                sig.price_at_signal, sig.current_price,
            )
            with self._stats_lock:
                self._stats_policy[state].add(
                    sig.current_directional_return, charge_return,
                )
            if sig.close_reason == "max_horizon":
                self.policy_max_horizon_exits += 1

        # Horizon-based statistical evaluation (with lock — UI thread also reads pending_count)
        with self._pending_lock:
            pending = self._pending.get(symbol)
            if not pending or current_price <= 0:
                return

            remaining: Deque[PendingPrediction] = deque()
            for pred in pending:
                age = current_ts - pred.ts_fired
                if age >= pred.horizon_seconds:
                    self._evaluate(
                        pred, current_price, current_ts, timed_out=False,
                        best_bid=best_bid, best_ask=best_ask,
                        bid_qty=bid_qty, ask_qty=ask_qty,
                    )
                elif age > self.max_pending_age:
                    self._evaluate(
                        pred, current_price, current_ts, timed_out=True,
                        best_bid=best_bid, best_ask=best_ask,
                        bid_qty=bid_qty, ask_qty=ask_qty,
                    )
                else:
                    remaining.append(pred)

            if remaining:
                self._pending[symbol] = remaining
            else:
                del self._pending[symbol]

    def _evaluate(
        self, pred: PendingPrediction, current_price: float,
        current_ts: float, timed_out: bool,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
        bid_qty: int = 0,
        ask_qty: int = 0,
    ) -> None:
        """Evaluate at executable exit quote with spread and charges separated."""
        side = "SHORT" if pred.state in _SHORT_STATES else "LONG"
        try:
            exit_price = self.execution_model.fill_price(
                side=side, is_entry=False, ltp=current_price,
                best_bid=best_bid, best_ask=best_ask,
            )
        except ValueError:
            return
        directional, charge_return, net_return = self.execution_model.evaluate(
            side, pred.price_at_signal, exit_price,
        )
        raw_return = (exit_price - pred.price_at_signal) / pred.price_at_signal

        evidence_bucket = self._evidence_bucket(pred.evidence)

        with self._stats_lock:
            self._stats_state_horizon[(pred.state, pred.horizon_seconds)].add(directional, charge_return)
            self._stats_evidence[(pred.state, evidence_bucket)].add(directional, charge_return)
            self._stats_regime[(pred.state, pred.regime_label)].add(directional, charge_return)
            self._stats_hour[(pred.state, pred.hour_of_day)].add(directional, charge_return)
            self._stats_symbol[(pred.state, pred.symbol)].add(directional, charge_return)

        self.predictions_evaluated += 1
        if timed_out:
            self.predictions_timed_out += 1

        # Persistent audit log
        if self._log_file is not None:
            payload = {
                "ts_fired":         round(pred.ts_fired, 3),
                "ts_evaluated":     round(current_ts, 3),
                "actual_horizon_s": round(current_ts - pred.ts_fired, 3),
                "target_horizon_s": pred.horizon_seconds,
                "symbol":           pred.symbol,
                "state":            pred.state,
                "score":            round(pred.smoothed_score, 3),
                "evidence":         round(pred.evidence, 2),
                "evidence_bucket":  evidence_bucket,
                "regime":           pred.regime_label,
                "hour":             pred.hour_of_day,
                "price_at_signal":  round(pred.price_at_signal, 4),
                "ltp_at_signal":    round(pred.ltp_at_signal, 4),
                "bid_at_signal":    round(pred.best_bid_at_signal, 4),
                "ask_at_signal":    round(pred.best_ask_at_signal, 4),
                "bid_qty_at_signal": pred.bid_qty_at_signal,
                "ask_qty_at_signal": pred.ask_qty_at_signal,
                "spread_bps_at_signal": round(pred.spread_bps_at_signal, 3),
                "price_at_horizon": round(exit_price, 4),
                "ltp_at_horizon":   round(current_price, 4),
                "bid_at_horizon":   round(float(best_bid or 0.0), 4),
                "ask_at_horizon":   round(float(best_ask or 0.0), 4),
                "bid_qty_at_horizon": int(bid_qty),
                "ask_qty_at_horizon": int(ask_qty),
                "raw_return_pct":       round(raw_return * 100, 4),
                "directional_return_pct": round(directional * 100, 4),
                "charges_pct":          round(charge_return * 100, 4),
                "net_return_pct":       round(net_return * 100, 4),
                "is_hit":               directional > 0,
                "is_net_profitable":    net_return > 0,
                "timed_out":            timed_out,
            }
            with self._log_lock:
                try:
                    self._log_file.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
                except Exception as e:
                    logger.debug("Log write failed: %s", e)

    # -- Read APIs (used by UI + report) --

    def snapshot_state_horizon(self) -> Dict[Tuple[str, float], HitRateBucket]:
        with self._stats_lock:
            return {k: HitRateBucket(**v.__dict__) for k, v in self._stats_state_horizon.items()}

    def snapshot_evidence(self) -> Dict[Tuple[str, str], HitRateBucket]:
        with self._stats_lock:
            return {k: HitRateBucket(**v.__dict__) for k, v in self._stats_evidence.items()}

    def snapshot_regime(self) -> Dict[Tuple[str, str], HitRateBucket]:
        with self._stats_lock:
            return {k: HitRateBucket(**v.__dict__) for k, v in self._stats_regime.items()}

    def snapshot_hour(self) -> Dict[Tuple[str, int], HitRateBucket]:
        with self._stats_lock:
            return {k: HitRateBucket(**v.__dict__) for k, v in self._stats_hour.items()}

    def snapshot_symbol(self) -> Dict[Tuple[str, str], HitRateBucket]:
        with self._stats_lock:
            return {k: HitRateBucket(**v.__dict__) for k, v in self._stats_symbol.items()}

    def snapshot_policy(self) -> Dict[str, HitRateBucket]:
        """One-outcome-per-signal stats under the 15-second entry+exit policy."""
        with self._stats_lock:
            return {k: HitRateBucket(**v.__dict__) for k, v in self._stats_policy.items()}

    def pending_count(self) -> int:
        with self._pending_lock:
            return sum(len(v) for v in self._pending.values())

    def verdict(self, bucket: HitRateBucket) -> Tuple[str, str]:
        """Return (verdict_text, verdict_style) for a bucket."""
        if bucket.count < self.min_samples:
            return f"need {self.min_samples - bucket.count} more", "dim"
        net_edge_pct = bucket.avg_net_edge * 100
        if net_edge_pct > 0.05:
            return "✓✓ STRONG EDGE", "bold green"
        elif net_edge_pct > 0.02:
            return "✓ edge", "green"
        elif net_edge_pct > 0.0:
            return "~ marginal", "yellow"
        elif net_edge_pct > -0.03:
            return "✗ break-even", "dim red"
        else:
            return "✗✗ noise/loss", "red"


# ============================================================
# 3. LIVE SESSION ORCHESTRATOR
# ============================================================

class LiveHitRateSession:
    """Ties Angel One WebSocket + BookDynamicsEngine + HitRateAnalyzer."""

    def __init__(
        self,
        config: ScannerConfig,
        analyzer: HitRateAnalyzer,
        diagnose: bool = False,
        dump_count: int = 100,
        dump_path: str = "logs/raw_ws_dump.jsonl",
        # Custom EngineConfig — allows CLI to override thresholds, EMA, etc.
        engine_config: Optional[EngineConfig] = None,
    ):
        if not SMARTAPI_AVAILABLE:
            raise ImportError(
                "smartapi-python not installed. Run:\n"
                "    pip install -r requirements.txt"
            )
        self.config = config
        self.analyzer = analyzer
        self.engine_config: EngineConfig = engine_config or EngineConfig()
        self.connector = AngelOneConnector(config)
        self.engines: Dict[str, BookDynamicsEngine] = {}
        self.token_to_symbol: Dict[int, str] = {}
        self.symbols_seen: set = set()
        self.last_prices: Dict[str, float] = {}

        # Global stats
        self.started_at: Optional[float] = None
        self.total_ticks_received = 0
        self.total_ticks_dropped = 0
        self.total_signals_computed = 0
        self.signal_state_counts: Dict[str, int] = defaultdict(int)
        self.regime_counts: Dict[str, int] = defaultdict(int)

        # Latency tracking
        self._latency_samples: Deque[float] = deque(maxlen=1000)
        self._latency_max = 0.0

        # -- Real-market defensive diagnostics --
        # Health monitor is ALWAYS on (silent failure prevention).
        # Dumper is on only when --diagnose is passed (dumps raw payloads).
        self.health = DataFlowHealthMonitor(
            expected_symbols_count=len(config.symbols),
            first_tick_timeout_s=60.0,
        )
        self.dumper: Optional[RawMessageDumper] = None
        if diagnose:
            self.dumper = RawMessageDumper(Path(dump_path), max_dumps=dump_count)

        # Background health check thread
        self._health_thread: Optional[threading.Thread] = None

        # Shutdown
        self._shutdown_event = threading.Event()

        # -- Stale-feed auto-exit (production reliability) --
        # If we go this long during NSE market hours without a single valid
        # tick, treat the WebSocket as silently dead. The health thread sets
        # `stale_feed_detected` and the main loop breaks out with a distinct
        # exit code so systemd (Restart=always) can restart the process.
        self.stale_feed_seconds: float = 90.0
        self.stale_feed_detected: bool = False

    def prepare(self) -> Dict[str, int]:
        """Login + scrip master + token resolution."""
        self.connector.login()
        self.connector.load_scrip_master()
        resolved, missing = self.connector.resolve_tokens()
        if not resolved:
            raise RuntimeError("No symbols resolved. Check config.symbols.")
        self.token_to_symbol = {t: s for s, t in resolved.items()}
        if missing:
            logger.warning("Missing %d symbols (skipped): %s",
                          len(missing), ", ".join(missing[:5]))
        logger.info("Resolved %d/%d symbols.", len(resolved), len(self.config.symbols))
        return resolved

    def _on_tick(self, msg: Dict[str, Any]) -> None:
        t_start = time.perf_counter()
        try:
            self.total_ticks_received += 1

            # -- Route by token to symbol --
            token_raw = msg.get("token") if isinstance(msg, dict) else None
            if token_raw is None:
                self.total_ticks_dropped += 1
                self.health.record_message(None, False, "missing_token_key")
                if self.dumper:
                    self.dumper.dump(msg, False, "missing_token_key")
                return
            try:
                token = int(token_raw)
            except (TypeError, ValueError):
                self.total_ticks_dropped += 1
                self.health.record_message(None, False,
                                            f"non_numeric_token ({token_raw!r})")
                if self.dumper:
                    self.dumper.dump(msg, False, "non_numeric_token")
                return
            symbol = self.token_to_symbol.get(token)
            if symbol is None:
                self.total_ticks_dropped += 1
                self.health.record_message(None, False,
                                            f"unknown_token ({token})")
                if self.dumper:
                    self.dumper.dump(msg, False, "unknown_token")
                return

            # -- Parse to MarketSnapshot with diagnostic capture --
            snap = AngelOneWSAdapter.parse(msg, symbol)
            if snap is None:
                self.total_ticks_dropped += 1
                # Diagnose WHY parse failed
                reason = _diagnose_parse_failure(msg)
                self.health.record_message(symbol, False, reason)
                if self.dumper:
                    self.dumper.dump(msg, False, reason)
                return

            # -- Success path --
            self.health.record_message(symbol, True)
            if self.dumper:
                self.dumper.dump(msg, True)

            self.symbols_seen.add(symbol)
            self.last_prices[symbol] = snap.ltp

            # STEP 0: Feed RVOL tracker with cumulative volume
            # (harmless if no RVOL gate active — analyzer.rvol_calculator
            # will be None and this block skipped)
            if self.analyzer.rvol_calculator is not None:
                self.analyzer.rvol_calculator.on_tick(
                    symbol, snap.volume_traded, snap.timestamp,
                )

            # STEP 1: Evaluate any pending predictions for this symbol
            self.analyzer.on_tick(
                symbol, snap.ltp, snap.timestamp,
                best_bid=snap.best_bid, best_ask=snap.best_ask,
                bid_qty=snap.best_bid_qty, ask_qty=snap.best_ask_qty,
            )

            # STEP 2: Engine update to get new signal
            engine = self.engines.get(symbol)
            if engine is None:
                engine = BookDynamicsEngine(config=self.engine_config)
                self.engines[symbol] = engine
            result = engine.update(snap)
            if result is None:
                return

            self.total_signals_computed += 1
            state = result.state.value
            self.signal_state_counts[state] += 1
            self.regime_counts[result.metrics.regime.label] += 1

            # STEP 3: Feed EVERY signal into the analyzer so the 15-second
            # entry-confirmation gate can cancel pending confirmations when
            # the state falls out of the qualifying zone. Non-actionable
            # states short-circuit inside record_signal.
            self.analyzer.record_signal(
                symbol, result, snap.ltp, snap.timestamp,
                best_bid=snap.best_bid, best_ask=snap.best_ask,
                bid_qty=snap.best_bid_qty, ask_qty=snap.best_ask_qty,
            )

        except Exception as e:
            logger.exception("_on_tick error: %s", e)
        finally:
            elapsed_us = (time.perf_counter() - t_start) * 1_000_000
            self._latency_samples.append(elapsed_us)
            if elapsed_us > self._latency_max:
                self._latency_max = elapsed_us

    def start(self) -> None:
        self.analyzer.open()
        if self.dumper is not None:
            self.dumper.open()
        self.started_at = time.time()
        # Sync health monitor start time
        self.health.started_at = self.started_at

        tokens = list(self.token_to_symbol.keys())
        logger.info("Starting WebSocket for %d tokens…", len(tokens))
        self.connector.start_websocket(tokens, self._on_tick)

        # Start background health monitor thread — checks every 5s,
        # prints loud warnings if silent failures detected.
        self._health_thread = threading.Thread(
            target=self._health_check_loop, name="health-monitor", daemon=True,
        )
        self._health_thread.start()

    def _health_check_loop(self) -> None:
        """
        Background thread — check data flow health, warn if broken, and
        request a shutdown if the feed has gone stale during market hours
        (silent-failure recovery for VPS deployments).
        """
        while not self._shutdown_event.is_set():
            time.sleep(5.0)
            try:
                self.health.check_health_and_warn()

                # Stale-feed detection: only meaningful during actual
                # NSE market hours. Outside those hours "no ticks" is
                # the expected state and must NOT trigger a restart.
                if (not self.stale_feed_detected
                        and self.stale_feed_seconds > 0
                        and is_market_hours()
                        and self.health.seconds_since_activity() > self.stale_feed_seconds):
                    silent = self.health.seconds_since_activity()
                    logger.critical(
                        "🚨 STALE FEED — no valid ticks for %.0fs during "
                        "market hours. Requesting shutdown so systemd can "
                        "restart the process (Restart=always).",
                        silent,
                    )
                    # Print once loudly to stdout too (in case journal is
                    # not being tailed).
                    print(
                        "\n" + "=" * 72 +
                        f"\n🚨 STALE FEED after {silent:.0f}s in market hours — exiting for restart"
                        f"\n" + "=" * 72 + "\n",
                        flush=True,
                    )
                    # Setting the flag is enough — main() polls it and
                    # exits cleanly with return code 75 (EX_TEMPFAIL).
                    self.stale_feed_detected = True
            except Exception as e:
                logger.debug("Health check error: %s", e)

    def stop(self) -> None:
        self._shutdown_event.set()
        if self._health_thread is not None:
            self._health_thread.join(timeout=2.0)
        try:
            self.connector.stop()
        except Exception:
            pass
        if self.dumper is not None:
            self.dumper.close()
        self.analyzer.close()

    def latency_stats(self) -> Tuple[float, float, float]:
        """Return (avg, p50, p99) latencies in microseconds."""
        if not self._latency_samples:
            return (0.0, 0.0, 0.0)
        sorted_samples = sorted(self._latency_samples)
        n = len(sorted_samples)
        avg = sum(sorted_samples) / n
        p50 = sorted_samples[n // 2]
        p99 = sorted_samples[min(n - 1, int(n * 0.99))]
        return (avg, p50, p99)

    def ticks_per_second(self) -> float:
        if self.started_at is None:
            return 0.0
        elapsed = max(time.time() - self.started_at, 1.0)
        return self.total_ticks_received / elapsed


# ============================================================
# 4. CONSOLE UI (Rich)
# ============================================================

class HitRateUI:
    """Live console dashboard using Rich."""

    STATE_STYLE = {
        "STRONG_LONG":  "bold green",
        "LONG":         "green",
        "WEAK_LONG":    "dim green",
        "STRONG_SHORT": "bold red",
        "SHORT":        "red",
        "WEAK_SHORT":   "dim red",
    }

    def __init__(self, session: LiveHitRateSession, analyzer: HitRateAnalyzer,
                 refresh_ms: int = 1000):
        if not RICH_AVAILABLE:
            raise ImportError("rich not installed: pip install rich")
        self.session = session
        self.analyzer = analyzer
        self.refresh_hz = max(1, 1000 // refresh_ms)
        self.console = Console()
        self._shutdown = threading.Event()

    def _header_panel(self) -> Panel:
        s = self.session
        elapsed = int(time.time() - (s.started_at or time.time()))
        h, rem = divmod(elapsed, 3600)
        m, sec = divmod(rem, 60)
        avg_us, p50_us, p99_us = s.latency_stats()

        # Live verdict from the real-time monitor
        verdict = self.analyzer.live_monitor.live_verdict()
        live_open = verdict["total_open"]
        winning = verdict["winning"]
        losing = verdict["losing"]
        hit_pct_now = verdict["hit_rate_pct"]
        avg_ret_now = verdict["avg_current_return_pct"]
        hit_style = ("bold green" if hit_pct_now > 55
                     else "green" if hit_pct_now > 50
                     else "red" if hit_pct_now < 50 else "white")

        line1 = Text.assemble(
            ("📊  NSE Live Hit Rate Analyzer", "bold cyan"),
            ("   |   ", "dim"),
            (f"Symbols: {len(s.symbols_seen)}", "white"),
            ("   |   ", "dim"),
            (f"Uptime: {h:02d}:{m:02d}:{sec:02d}", "white"),
            ("   |   ", "dim"),
            (f"Ticks: {s.total_ticks_received:,}", "white"),
            ("  ", "dim"),
            (f"({s.ticks_per_second():.0f}/s)", "dim"),
            ("   |   ", "dim"),
            (f"Signals: {s.total_signals_computed:,}", "white"),
        )
        # LIVE VERDICT line — "is market moving as we predicted RIGHT NOW?"
        line2 = Text.assemble(
            ("⚡ LIVE VERDICT (open signals): ", "bold magenta"),
            (f"{live_open} open ", "white"),
            (f"→ {winning} winning ", "green"),
            (f"/ {losing} losing", "red"),
            ("   |   ", "dim"),
            ("Hit rate now: ", "white"),
            (f"{hit_pct_now:>5.1f}%", hit_style),
            ("   |   ", "dim"),
            ("Avg current: ", "white"),
            (f"{avg_ret_now:+.3f}%",
             "green" if avg_ret_now > 0 else "red"),
            ("   |   ", "dim"),
            (f"Evaluated: {self.analyzer.predictions_evaluated:,}", "dim"),
        )
        line3 = Text.assemble(
            (f"Latency p50={p50_us:.0f}µs p99={p99_us:.0f}µs", "dim"),
            ("   |   ", "dim"),
            (f"Execution: {self.analyzer.execution_model.description()}", "dim"),
            ("   |   ", "dim"),
            (f"Signals closed (max horizon): "
             f"{self.analyzer.live_monitor.total_max_horizon_closed:,}", "dim"),
        )
        return Panel(Align.center(Text.assemble(line1, "\n", line2, "\n", line3)),
                     border_style="magenta")

    def _live_signals_panel(self) -> Table:
        """
        Real-time panel — every open signal with current status.
        यह वो table है जो user चाहता है: "अभी scanner सही जा रहा है या गलत?"
        """
        signals = self.analyzer.live_monitor.snapshot_open(top_n=30)
        cost_pct = self.analyzer.cost * 100

        table = Table(
            title=(f"⚡  LIVE OPEN SIGNALS  —  Real-time score verdict "
                   f"(sign-adjusted; +ve = signal proving RIGHT)"),
            title_style="bold cyan",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("Symbol", style="cyan", width=13)
        table.add_column("State", width=13)
        table.add_column("Score", justify="right", width=7)
        table.add_column("Evid", justify="right", width=6)
        table.add_column("Age", justify="right", width=6)
        table.add_column("Entry", justify="right", width=9)
        table.add_column("Now", justify="right", width=9)
        table.add_column("Dir Ret", justify="right", width=9)
        table.add_column("MFE", justify="right", width=8)
        table.add_column("MAE", justify="right", width=8)
        table.add_column("Status", width=15)

        if not signals:
            # More informative empty-state message so the user knows WHY
            # the panel is empty instead of just staring at a blank table.
            a = self.analyzer
            hints: List[str] = []
            if a.entry_confirmation_seconds > 0:
                # Confirmation gate active — signals must qualify continuously.
                pending = len(a._pending_confirmations)
                hints.append(
                    f"confirmation gate ON ({a.entry_confirmation_seconds:.0f}s, "
                    f"score≥{a.entry_score_threshold or 0:.1f}, "
                    f"pending={pending}, cancelled={a.confirmations_cancelled}, "
                    f"passed={a.confirmations_passed})"
                )
            if a.allowed_signal_states != set(_ACTIONABLE_STATES):
                filt = ("STRONG-only" if a.allowed_signal_states == set(_STRONG_STATES)
                        else "STRONG+LONG/SHORT" if a.allowed_signal_states == set(_NORMAL_AND_STRONG_STATES)
                        else "custom")
                hints.append(
                    f"state filter: {filt} (blocked so far: "
                    f"{a.signals_blocked_by_state_filter})"
                )
            if a.session_manager is not None:
                hints.append(
                    f"session gate ON (blocked: {a.signals_blocked_by_session})"
                )
            if a.min_rvol > 0.0:
                hints.append(
                    f"RVOL gate ON (min={a.min_rvol}, blocked: {a.signals_blocked_by_low_rvol})"
                )
            base_msg = "No open signals yet."
            if hints:
                table.caption = base_msg + "  " + " · ".join(hints)
            else:
                # No filters — waiting for scores to hit thresholds.
                table.caption = (
                    base_msg + "  All filters OFF — waiting for "
                    "score ≥ ±2.0 (WEAK) or higher."
                )
            table.caption_style = "dim"
            return table

        cost_frac = self.analyzer.cost  # already in fractional form, e.g. 0.0006

        for sig in signals:
            state_style = self.STATE_STYLE.get(sig.state, "white")
            dir_pct = sig.current_directional_return * 100
            mfe_pct = sig.max_favorable_excursion * 100
            mae_pct = sig.max_adverse_excursion * 100

            # Status determination (compare fraction-to-fraction, not fraction-to-pct)
            if sig.current_directional_return > cost_frac:
                status_text, status_style = "✓✓ PROFITABLE", "bold green"
            elif sig.current_directional_return > 0:
                status_text, status_style = "✓ winning", "green"
            elif sig.current_directional_return > -0.001:
                status_text, status_style = "~ flat", "yellow"
            elif sig.current_directional_return > -0.003:
                status_text, status_style = "✗ losing", "red"
            else:
                status_text, status_style = "✗✗ HEAVY LOSS", "bold red"

            # Format age
            age_s = int(sig.seconds_elapsed)
            if age_s < 60:
                age_str = f"{age_s}s"
            else:
                m, s_ = divmod(age_s, 60)
                age_str = f"{m}m{s_}s"

            dir_style = "green" if dir_pct > 0 else "red"
            # Handle "never MFE'd / MAE'd" sentinel — display "—" instead of 0.00%
            if sig.time_to_mfe_s < 0:
                mfe_display = Text("—", style="dim")
            else:
                mfe_display = Text(f"{mfe_pct:+.2f}%", style="green")
            if sig.time_to_mae_s < 0:
                mae_display = Text("—", style="dim")
            else:
                mae_display = Text(f"{mae_pct:+.2f}%", style="red")

            table.add_row(
                sig.symbol,
                Text(sig.state, style=state_style),
                f"{sig.smoothed_score:+.2f}",
                f"{sig.evidence:.0f}",
                age_str,
                f"{sig.price_at_signal:.2f}",
                f"{sig.current_price:.2f}",
                Text(f"{dir_pct:+.3f}%", style=dir_style),
                mfe_display,
                mae_display,
                Text(status_text, style=status_style),
            )

        return table

    def _state_horizon_table(self) -> Table:
        """Main table: state × horizon breakdown."""
        analyzer = self.analyzer
        stats = analyzer.snapshot_state_horizon()

        table = Table(
            title=f"🎯  Hit Rate by State × Horizon  "
                  f"(min {analyzer.min_samples} samples for verdict)",
            title_style="bold magenta",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("State", style="cyan", width=14)
        table.add_column("Horizon", justify="right", width=8)
        table.add_column("N", justify="right", width=6)
        table.add_column("Hit %", justify="right", width=8)
        table.add_column("% Above Cost", justify="right", width=13)
        table.add_column("Avg Ret %", justify="right", width=11)
        table.add_column("Net Edge %", justify="right", width=11)
        table.add_column("Sharpe", justify="right", width=8)
        table.add_column("Verdict", width=18)

        # Order: LONG side (strong→weak), SHORT side (weak→strong)
        state_order = ["STRONG_LONG", "LONG", "WEAK_LONG",
                       "WEAK_SHORT", "SHORT", "STRONG_SHORT"]

        any_data = False
        for state in state_order:
            style = self.STATE_STYLE.get(state, "white")
            for h in analyzer.horizons:
                bucket = stats.get((state, h))
                if bucket is None or bucket.count == 0:
                    continue
                any_data = True
                hit = bucket.hit_rate * 100
                net_prof = bucket.net_profit_rate * 100
                avg_ret = bucket.avg_return * 100
                net_edge = bucket.avg_net_edge * 100
                sharpe = bucket.sharpe_proxy
                verdict_text, verdict_style = analyzer.verdict(bucket)

                avg_style = "green" if avg_ret > 0 else "red"
                edge_style = ("bold green" if net_edge > 0.02
                              else "yellow" if net_edge > 0 else "red")

                table.add_row(
                    Text(state, style=style),
                    f"{int(h)}s",
                    f"{bucket.count:,}",
                    f"{hit:.1f}%",
                    f"{net_prof:.1f}%",
                    Text(f"{avg_ret:+.3f}%", style=avg_style),
                    Text(f"{net_edge:+.3f}%", style=edge_style),
                    f"{sharpe:+.2f}",
                    Text(verdict_text, style=verdict_style),
                )

        if not any_data:
            table.caption = "Waiting for first signal + horizon expiry…"
            table.caption_style = "dim"

        return table

    def _regime_hour_table(self) -> Table:
        """Compact table: hour of day × top actionable states."""
        analyzer = self.analyzer
        stats_hour = analyzer.snapshot_hour()

        table = Table(
            title="🕐  Hit Rate by Hour of Day (IST) — top 3 states combined",
            title_style="bold magenta",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("Hour (IST)", style="cyan", width=12)
        table.add_column("Samples", justify="right", width=9)
        table.add_column("Hit %", justify="right", width=8)
        table.add_column("Avg Ret %", justify="right", width=11)
        table.add_column("Net Edge %", justify="right", width=12)
        table.add_column("Verdict", width=18)

        # Aggregate across states per hour
        by_hour: Dict[int, HitRateBucket] = defaultdict(HitRateBucket)
        for (state, hour), b in stats_hour.items():
            if hour < 0:
                continue
            agg = by_hour[hour]
            agg.count += b.count
            agg.hits += b.hits
            agg.net_profitable += b.net_profitable
            agg.sum_return += b.sum_return
            agg.sum_return_sq += b.sum_return_sq
            agg.sum_net_return += b.sum_net_return
            if b.max_win > agg.max_win:
                agg.max_win = b.max_win
            if b.max_loss < agg.max_loss:
                agg.max_loss = b.max_loss

        any_data = False
        for hour in sorted(by_hour.keys()):
            b = by_hour[hour]
            if b.count == 0:
                continue
            any_data = True
            verdict_text, verdict_style = analyzer.verdict(b)
            avg_ret = b.avg_return * 100
            net_edge = b.avg_net_edge * 100
            table.add_row(
                f"{hour:02d}:00-{hour:02d}:59",
                f"{b.count:,}",
                f"{b.hit_rate*100:.1f}%",
                Text(f"{avg_ret:+.3f}%", style="green" if avg_ret > 0 else "red"),
                Text(f"{net_edge:+.3f}%",
                     style="green" if net_edge > 0 else "red"),
                Text(verdict_text, style=verdict_style),
            )

        if not any_data:
            table.caption = "Waiting for hourly data…"
            table.caption_style = "dim"

        return table

    def _render(self) -> Layout:
        layout = Layout()
        # 4-panel layout: header + LIVE open signals + horizon stats + hour breakdown
        layout.split_column(
            Layout(name="header", size=5),
            Layout(name="live_open", ratio=3),      # ⚡ New — real-time verdict
            Layout(name="state_horizon", ratio=2),  # Statistical (horizon-based)
            Layout(name="hour", ratio=2),
        )
        layout["header"].update(self._header_panel())
        layout["live_open"].update(self._live_signals_panel())
        layout["state_horizon"].update(self._state_horizon_table())
        layout["hour"].update(self._regime_hour_table())
        return layout

    def run(self) -> None:
        with Live(self._render(), console=self.console,
                  refresh_per_second=self.refresh_hz, screen=False) as live:
            while not self._shutdown.is_set():
                time.sleep(1.0 / self.refresh_hz)
                live.update(self._render())

    def stop(self) -> None:
        self._shutdown.set()


# ============================================================
# 5. HEADLESS MODE + EOD REPORT
# ============================================================

def _print_headless_status(session: LiveHitRateSession, analyzer: HitRateAnalyzer) -> None:
    """Compact multi-line status for --no-ui mode: live verdict + horizon stats."""
    _, p50_us, p99_us = session.latency_stats()

    # LIVE snapshot — right now
    verdict = analyzer.live_monitor.live_verdict()

    # Statistical (horizon-based) aggregate
    stats = analyzer.snapshot_state_horizon()
    total_hits = sum(b.hits for b in stats.values())
    total_count = sum(b.count for b in stats.values())
    total_net_prof = sum(b.net_profitable for b in stats.values())
    hit_pct = (total_hits / total_count * 100) if total_count else 0
    net_pct = (total_net_prof / total_count * 100) if total_count else 0

    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(
        f"[{ts} IST]  "
        f"ticks={session.total_ticks_received:,} ({session.ticks_per_second():.0f}/s)  "
        f"signals={session.total_signals_computed:,}  "
        f"⚡LIVE: {verdict['total_open']} open ({verdict['winning']}✓/{verdict['losing']}✗ "
        f"= {verdict['hit_rate_pct']:.0f}% hit_now, avg={verdict['avg_current_return_pct']:+.2f}%)  "
        f"| Historic: {total_count:,} evaluated, hit={hit_pct:.1f}%, net_prof={net_pct:.1f}%  "
        f"| lat p50/p99={p50_us:.0f}/{p99_us:.0f}µs",
        flush=True,
    )

    # Show top 5 currently-open signals with real-time status
    open_signals = analyzer.live_monitor.snapshot_open(top_n=5)
    if open_signals:
        cost_pct = analyzer.cost * 100
        print("           Top open signals RIGHT NOW:")
        for sig in open_signals:
            dir_pct = sig.current_directional_return * 100
            age_s = int(sig.seconds_elapsed)
            status = "✓" if dir_pct > cost_pct else "~" if dir_pct > 0 else "✗"
            # Sentinel-aware MFE/MAE display (matches rich UI behavior)
            mfe_str = ("      —" if sig.time_to_mfe_s < 0
                       else f"{sig.max_favorable_excursion*100:+.2f}%")
            mae_str = ("      —" if sig.time_to_mae_s < 0
                       else f"{sig.max_adverse_excursion*100:+.2f}%")
            print(f"             {status} {sig.symbol:<14} {sig.state:<12} "
                  f"score={sig.smoothed_score:+.1f}  age={age_s:>3}s  "
                  f"entry={sig.price_at_signal:>8.2f} → now={sig.current_price:>8.2f}  "
                  f"dir_ret={dir_pct:+.3f}%  "
                  f"MFE={mfe_str} MAE={mae_str}")


def generate_eod_report(session: LiveHitRateSession, analyzer: HitRateAnalyzer) -> str:
    """Generate comprehensive end-of-day report."""
    W = 82
    lines: List[str] = []
    lines.append("═" * W)
    lines.append("  📊 END-OF-DAY HIT RATE REPORT")
    lines.append("═" * W)

    # Session summary
    elapsed = time.time() - (session.started_at or time.time())
    h, rem = divmod(int(elapsed), 3600)
    m, sec = divmod(rem, 60)
    lines.append("")
    lines.append(f"  Session duration    : {h:02d}:{m:02d}:{sec:02d}")
    lines.append(f"  Symbols tracked     : {len(session.symbols_seen)}")
    lines.append(f"  Total ticks         : {session.total_ticks_received:,}")
    lines.append(f"  Signals computed    : {session.total_signals_computed:,}")
    lines.append(f"  Signals recorded    : {analyzer.signals_recorded:,}")
    lines.append(f"  Signals deduped     : {analyzer.signals_deduped:,}  "
                 f"(same state within {analyzer.signal_dedup_seconds:.0f}s)")
    lines.append(f"  Blocked by cooldown : {analyzer.signals_blocked_by_cooldown:,}")
    lines.append(f"  Blocked by session  : {analyzer.signals_blocked_by_session:,}  "
                 f"({'ENABLED' if analyzer.session_manager else 'gate disabled'})")
    lines.append(f"  Blocked by low RVOL : {analyzer.signals_blocked_by_low_rvol:,}  "
                 f"({'ENABLED @ min=' + str(analyzer.min_rvol) if analyzer.min_rvol > 0 else 'gate disabled'})")
    _state_filter_desc = "all actionable" if analyzer.allowed_signal_states == set(_ACTIONABLE_STATES) \
        else ("STRONG only" if analyzer.allowed_signal_states == set(_STRONG_STATES)
              else ("STRONG+LONG/SHORT" if analyzer.allowed_signal_states == set(_NORMAL_AND_STRONG_STATES)
                    else "custom: " + ",".join(sorted(analyzer.allowed_signal_states))))
    lines.append(f"  Blocked by state    : {analyzer.signals_blocked_by_state_filter:,}  "
                 f"(filter: {_state_filter_desc})")
    if analyzer.rvol_calculator is not None:
        lines.append(f"  RVOL warmup passes  : {analyzer.signals_allowed_during_rvol_warmup:,}  "
                     f"(signals recorded while RVOL still warming up)")
    lines.append(f"  Predictions evaluated: {analyzer.predictions_evaluated:,}")
    lines.append(f"  Pending (not yet expired): {analyzer.pending_count():,}")
    lines.append(f"  Execution model     : {analyzer.execution_model.description()}")

    # -- Signal-quality-gate detail sections --
    if analyzer.session_manager is not None:
        ss = analyzer.session_manager.stats()
        lines.append("")
        lines.append("─" * W)
        lines.append("  🕐 SESSION PHASE STATS")
        lines.append("─" * W)
        lines.append(f"  Current phase        : {ss['current_phase']}")
        lines.append(f"  Total transitions    : {ss['phase_transitions']}")
        lines.append(f"  Allowed phases       : "
                     f"{', '.join(p.name for p in sorted(analyzer.allowed_phases, key=lambda x: x.name))}")
        lines.append(f"  No-entry cutoff      : {ss['no_new_entry_after']}")
        lines.append(f"  Holidays configured  : {', '.join(ss['holidays_configured']) or '(none)'}")
        if ss['phase_hits']:
            lines.append(f"  Phase hit counts:")
            for phase, count in sorted(ss['phase_hits'].items(),
                                        key=lambda kv: -kv[1]):
                lines.append(f"    {phase:<18} {count:,}")

    if analyzer.rvol_calculator is not None:
        rs = analyzer.rvol_calculator.stats()
        lines.append("")
        lines.append("─" * W)
        lines.append("  📊 RELATIVE VOLUME (RVOL) STATS")
        lines.append("─" * W)
        lines.append(f"  Min RVOL threshold   : {analyzer.min_rvol:.2f}"
                     f"{' (gate ACTIVE)' if analyzer.min_rvol > 0 else ' (gate off)'}")
        lines.append(f"  Symbols tracked      : {rs['symbols_tracked']}")
        lines.append(f"  Symbols warmed up    : {rs['symbols_warmed_up']}")
        lines.append(f"  Ticks processed      : {rs['ticks_processed']:,}")
        lines.append(f"  Session resets       : {rs['session_resets']}")
        lines.append(f"  Anomalies capped     : {rs['anomalies_capped']}")
        lines.append(f"  RVOL queries         : {rs['rvol_queries']:,}")
        lines.append(f"  RVOL returned None   : {rs['rvol_returns_none']:,}  "
                     f"(warm-up / too-early bucket)")

    # Signal distribution
    lines.append("")
    lines.append("─" * W)
    lines.append("  📡 SIGNAL STATE DISTRIBUTION")
    lines.append("─" * W)
    total_signals = sum(session.signal_state_counts.values())
    for state in ["STRONG_LONG", "LONG", "WEAK_LONG",
                  "WEAK_SHORT", "SHORT", "STRONG_SHORT",
                  "NEUTRAL", "SUPPRESSED"]:
        count = session.signal_state_counts.get(state, 0)
        if count == 0:
            continue
        pct = count / total_signals * 100 if total_signals else 0
        lines.append(f"  {state:<16} {count:>10,}  ({pct:>5.2f}%)")

    # Main table: state × horizon
    lines.append("")
    lines.append("─" * W)
    lines.append("  🎯 HIT RATE BY STATE × HORIZON")
    lines.append("─" * W)
    lines.append(f"  Column meanings (CHARGES = {analyzer.cost*100:.3f}% round-trip; spread via bid/ask):")
    lines.append(f"    Hit %       = % of signals that went in predicted direction")
    lines.append(f"    %AboveCost  = % of signals where profit > cost (COUNT, not amount)")
    lines.append(f"    AvgRet %    = average per-trade return BEFORE cost (small = noise)")
    lines.append(f"    NetEdge %   = average per-trade return AFTER cost (BOTTOM LINE)")
    lines.append(f"    → +ve NetEdge = profitable | -ve NetEdge = loss-making")
    lines.append("")
    stats_sh = analyzer.snapshot_state_horizon()
    lines.append(f"  {'State':<14} {'Horizon':>8} {'N':>7} {'Hit %':>8} "
                 f"{'%AboveCost':>13} {'AvgRet %':>10} {'NetEdge %':>11} {'Verdict':<20}")
    lines.append("  " + "-" * (W - 2))
    for state in ["STRONG_LONG", "LONG", "WEAK_LONG",
                  "WEAK_SHORT", "SHORT", "STRONG_SHORT"]:
        for h in analyzer.horizons:
            b = stats_sh.get((state, h))
            if b is None or b.count == 0:
                continue
            hit = b.hit_rate * 100
            netp = b.net_profit_rate * 100
            avg = b.avg_return * 100
            edge = b.avg_net_edge * 100
            verdict, _ = analyzer.verdict(b)
            lines.append(f"  {state:<14} {int(h):>6}s  {b.count:>6,} "
                         f"{hit:>7.1f}% {netp:>12.1f}% {avg:>+9.3f}% "
                         f"{edge:>+10.3f}% {verdict:<20}")

    # By evidence bucket
    lines.append("")
    lines.append("─" * W)
    lines.append("  📊 HIT RATE BY EVIDENCE STRENGTH (default 60s horizon aggregation)")
    lines.append("─" * W)
    stats_ev = analyzer.snapshot_evidence()
    lines.append(f"  {'State':<14} {'Evidence':>10} {'N':>7} {'Hit %':>8} "
                 f"{'AvgRet %':>10} {'NetEdge %':>11} {'Verdict':<20}")
    lines.append("  " + "-" * (W - 2))
    for state in ["STRONG_LONG", "LONG", "WEAK_LONG",
                  "WEAK_SHORT", "SHORT", "STRONG_SHORT"]:
        for ev_bucket in ["0-30", "30-50", "50-70", "70+"]:
            b = stats_ev.get((state, ev_bucket))
            if b is None or b.count == 0:
                continue
            verdict, _ = analyzer.verdict(b)
            lines.append(f"  {state:<14} {ev_bucket:>10} {b.count:>6,} "
                         f"{b.hit_rate*100:>7.1f}% {b.avg_return*100:>+9.3f}% "
                         f"{b.avg_net_edge*100:>+10.3f}% {verdict:<20}")

    # By regime (Phase 2)
    lines.append("")
    lines.append("─" * W)
    lines.append("  🌀 HIT RATE BY MARKET REGIME (Phase 2)")
    lines.append("─" * W)
    stats_reg = analyzer.snapshot_regime()
    # Aggregate by regime across states
    by_regime: Dict[str, HitRateBucket] = defaultdict(HitRateBucket)
    for (state, regime), b in stats_reg.items():
        agg = by_regime[regime]
        agg.count += b.count
        agg.hits += b.hits
        agg.net_profitable += b.net_profitable
        agg.sum_return += b.sum_return
        agg.sum_return_sq += b.sum_return_sq
        agg.sum_net_return += b.sum_net_return

    lines.append(f"  {'Regime':<20} {'N':>7} {'Hit %':>8} "
                 f"{'AvgRet %':>10} {'NetEdge %':>11} {'Verdict':<20}")
    lines.append("  " + "-" * (W - 2))
    for regime in sorted(by_regime.keys(), key=lambda k: -by_regime[k].count):
        b = by_regime[regime]
        if b.count == 0:
            continue
        verdict, _ = analyzer.verdict(b)
        lines.append(f"  {regime:<20} {b.count:>6,} {b.hit_rate*100:>7.1f}% "
                     f"{b.avg_return*100:>+9.3f}% {b.avg_net_edge*100:>+10.3f}% "
                     f"{verdict:<20}")

    # By hour of day
    lines.append("")
    lines.append("─" * W)
    lines.append("  🕐 HIT RATE BY HOUR OF DAY (IST)")
    lines.append("─" * W)
    stats_hour = analyzer.snapshot_hour()
    by_hour_agg: Dict[int, HitRateBucket] = defaultdict(HitRateBucket)
    for (state, hour), b in stats_hour.items():
        if hour < 0:
            continue
        agg = by_hour_agg[hour]
        agg.count += b.count
        agg.hits += b.hits
        agg.net_profitable += b.net_profitable
        agg.sum_return += b.sum_return
        agg.sum_return_sq += b.sum_return_sq
        agg.sum_net_return += b.sum_net_return
    lines.append(f"  {'Hour':<12} {'N':>7} {'Hit %':>8} "
                 f"{'AvgRet %':>10} {'NetEdge %':>11} {'Verdict':<20}")
    lines.append("  " + "-" * (W - 2))
    for hour in sorted(by_hour_agg.keys()):
        b = by_hour_agg[hour]
        if b.count == 0:
            continue
        verdict, _ = analyzer.verdict(b)
        lines.append(f"  {hour:02d}:00-{hour:02d}:59 {b.count:>6,} "
                     f"{b.hit_rate*100:>7.1f}% {b.avg_return*100:>+9.3f}% "
                     f"{b.avg_net_edge*100:>+10.3f}% {verdict:<20}")

    # MFE/MAE excursion statistics (for TP/SL calibration)
    lines.append("")
    lines.append("─" * W)
    lines.append("  📈 MFE / MAE EXCURSION STATS (per state, from closed signals)")
    lines.append("─" * W)
    lines.append("  MFE = Max Favorable Excursion (best point signal was proved right)")
    lines.append("  MAE = Max Adverse Excursion  (worst point signal was proved wrong)")
    lines.append("  Use these to calibrate Take-Profit and Stop-Loss levels.")
    lines.append("")
    exc_stats = analyzer.live_monitor.excursion_stats()
    if exc_stats:
        lines.append(f"  {'State':<14} {'N':>6} {'MFE hit':>8} "
                     f"{'Avg MFE':>10} {'Med MFE':>10} {'Avg MAE':>10} "
                     f"{'Med MAE':>10} {'t→MFE (s)':>10} {'Final':>10}")
        lines.append("  " + "-" * (W - 2))
        for state in ["STRONG_LONG", "LONG", "WEAK_LONG",
                      "WEAK_SHORT", "SHORT", "STRONG_SHORT"]:
            s = exc_stats.get(state)
            if s is None:
                continue
            # % of signals that ever went positive (had MFE > 0)
            mfe_hit_pct = (s["n_had_mfe"] / s["n"] * 100) if s["n"] else 0
            # Handle sentinel: -1.0 means "no signals ever MFE'd"
            t_mfe_str = "—" if s["avg_time_to_mfe_s"] < 0 else f"{s['avg_time_to_mfe_s']:>7.1f}s"
            lines.append(
                f"  {state:<14} {s['n']:>6} {mfe_hit_pct:>6.1f}% "
                f"{s['avg_mfe_pct']:>+9.3f}% {s['median_mfe_pct']:>+9.3f}% "
                f"{s['avg_mae_pct']:>+9.3f}% {s['median_mae_pct']:>+9.3f}% "
                f"{t_mfe_str:>10} "
                f"{s['avg_final_return_pct']:>+9.3f}%"
            )
        lines.append("")
        lines.append("  💡 Calibration hint (only meaningful when MFE hit rate > 60%):")
        lines.append("     - Set TAKE_PROFIT ≈ median MFE (most trades reach it before decay)")
        lines.append("     - Set STOP_LOSS  ≈ median MAE × 0.7 (avoid stopping out too early)")
        lines.append("     - 'MFE hit' = % of signals that EVER went positive during their life")
    else:
        lines.append("  (No closed signals yet — need signals to reach max horizon)")

    # Top 10 symbols by predictive accuracy
    lines.append("")
    lines.append("─" * W)
    lines.append("  🏆 TOP 10 SYMBOLS BY PREDICTIVE HIT RATE (min 20 samples)")
    lines.append("─" * W)
    stats_sym = analyzer.snapshot_symbol()
    by_symbol_agg: Dict[str, HitRateBucket] = defaultdict(HitRateBucket)
    for (state, symbol), b in stats_sym.items():
        agg = by_symbol_agg[symbol]
        agg.count += b.count
        agg.hits += b.hits
        agg.net_profitable += b.net_profitable
        agg.sum_return += b.sum_return
        agg.sum_return_sq += b.sum_return_sq
        agg.sum_net_return += b.sum_net_return

    symbol_ranked = sorted(
        [(s, b) for s, b in by_symbol_agg.items() if b.count >= 20],
        key=lambda x: -x[1].avg_net_edge,
    )
    if symbol_ranked:
        lines.append(f"  {'Symbol':<16} {'N':>7} {'Hit %':>8} "
                     f"{'AvgRet %':>10} {'NetEdge %':>11}")
        lines.append("  " + "-" * (W - 2))
        for symbol, b in symbol_ranked[:10]:
            lines.append(f"  {symbol:<16} {b.count:>6,} {b.hit_rate*100:>7.1f}% "
                         f"{b.avg_return*100:>+9.3f}% {b.avg_net_edge*100:>+10.3f}%")
    else:
        lines.append("  (No symbols with ≥ 20 predictions yet)")

    # -- POLICY OUTCOME (15-second entry confirmation + survival exit) --
    lines.append("")
    lines.append("─" * W)
    lines.append("  🎯 15-SECOND POLICY OUTCOME (one row per confirmed signal)")
    lines.append("─" * W)
    lines.append(
        f"  Entry confirmation : "
        f"{analyzer.entry_confirmation_seconds:.0f}s continuous qualification "
        f"({'ON' if analyzer.entry_confirmation_seconds > 0 else 'off'})"
    )
    lines.append(
        f"  Survival exit      : {analyzer.live_monitor.survival_check_seconds:.0f}s "
        f"MFE ≥ {analyzer.live_monitor.survival_min_favor_pct*100:.3f}% "
        f"({'ON' if analyzer.live_monitor.survival_check_seconds > 0 else 'off'})"
    )
    lines.append(
        f"  Confirmations      : started {analyzer.confirmations_started:,}  "
        f"passed {analyzer.confirmations_passed:,}  "
        f"cancelled {analyzer.confirmations_cancelled:,}"
    )
    lines.append(
        f"  Survival check     : passed {analyzer.live_monitor.total_survival_passed:,}  "
        f"failed {analyzer.live_monitor.total_survival_failed:,}"
    )
    lines.append(
        f"  Policy exits       : survival {analyzer.policy_survival_exits:,}  "
        f"max_horizon {analyzer.policy_max_horizon_exits:,}"
    )
    policy_stats = analyzer.snapshot_policy()
    if any(b.count for b in policy_stats.values()):
        lines.append("")
        lines.append(f"  {'State':<14} {'N':>7} {'Hit %':>8} "
                     f"{'AvgRet %':>10} {'NetEdge %':>11} {'Verdict':<20}")
        lines.append("  " + "-" * (W - 2))
        for state in ["STRONG_LONG", "LONG", "WEAK_LONG",
                      "WEAK_SHORT", "SHORT", "STRONG_SHORT"]:
            b = policy_stats.get(state)
            if b is None or b.count == 0:
                continue
            verdict, _ = analyzer.verdict(b)
            lines.append(f"  {state:<14} {b.count:>6,} "
                         f"{b.hit_rate*100:>7.1f}% "
                         f"{b.avg_return*100:>+9.3f}% "
                         f"{b.avg_net_edge*100:>+10.3f}% {verdict:<20}")
    else:
        lines.append("")
        lines.append("  (No confirmed signals have closed yet — run longer)")

    # -- HONEST VERDICT --
    lines.append("")
    lines.append("═" * W)
    lines.append("  📌 HONEST OVERALL VERDICT")
    lines.append("═" * W)

    total_count = sum(b.count for b in stats_sh.values())
    total_hits = sum(b.hits for b in stats_sh.values())
    total_net_prof = sum(b.net_profitable for b in stats_sh.values())
    total_net_return = sum(b.sum_net_return for b in stats_sh.values())

    if total_count < analyzer.min_samples:
        lines.append("  ⚠ INSUFFICIENT DATA — need more predictions for confident verdict.")
        lines.append(f"     Currently have {total_count} predictions across all buckets.")
        lines.append("     Recommendation: Run for full trading day, or record 5 days")
        lines.append("     via tick_recorder.py then batch-analyze.")
    else:
        overall_hit_rate = total_hits / total_count * 100
        overall_net_rate = total_net_prof / total_count * 100
        avg_net_edge = total_net_return / total_count * 100

        lines.append(f"  Total predictions evaluated: {total_count:,}")
        lines.append(f"  Overall directional hit rate: {overall_hit_rate:.1f}%")
        lines.append(f"  Overall NET profit rate     : {overall_net_rate:.1f}%  "
                     f"(after {analyzer.cost*100:.2f}% cost)")
        lines.append(f"  Average net edge per signal : {avg_net_edge:+.4f}%")
        lines.append("")

        if avg_net_edge > 0.05:
            lines.append("  ✅ STRONG POSITIVE EDGE: Signals actually predict + profitable")
            lines.append("     after costs. This is rare. Verify with 5-10 day recording +")
            lines.append("     replay before deploying real capital.")
        elif avg_net_edge > 0.02:
            lines.append("  ✓ MARGINAL POSITIVE EDGE: Small predictable alpha.")
            lines.append("     Recommendation: Refine parameters, longer testing.")
        elif avg_net_edge > 0.0:
            lines.append("  🟡 BREAK-EVEN: Signals slightly predictive but cost eats edge.")
            lines.append("     Recommendation: Lower cost broker OR wait for stronger signals.")
        elif avg_net_edge > -0.03:
            lines.append("  ⚠ SLIGHT LOSS: Signals unreliable at current parameters.")
            lines.append("     Recommendation: DO NOT trade real money. Tune first.")
        else:
            lines.append("  ❌ SIGNIFICANT LOSS: Scanner does NOT predict correctly on")
            lines.append("     real NSE data with current parameters.")
            lines.append("     Recommendation: DO NOT deploy. Fundamental review needed.")

    lines.append("")
    lines.append("  Files generated:")
    lines.append(f"    - {analyzer.log_path}     ({analyzer.predictions_evaluated} records)")
    lines.append("═" * W)
    return "\n".join(lines)


# ============================================================
# 6. TIME-OF-DAY HELPERS
# ============================================================

def is_market_hours() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return MARKET_OPEN_MINUTES <= mins <= MARKET_CLOSE_MINUTES


def seconds_until_market_close() -> float:
    now = datetime.now(IST)
    close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >= close:
        return 0.0
    return (close - now).total_seconds()


# ============================================================
# 7. MAIN
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Live Hit Rate Analyzer — measure scanner's predictive "
                    "accuracy on real Angel One data (virtual, no orders).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default 60-min session, rich UI
  python3 live_hit_rate_analyzer.py --config config.json

  # Full trading day, headless (VPS tmux mode)
  python3 live_hit_rate_analyzer.py --config config.json \\
      --duration-hours 6.5 --no-ui

  # Custom horizons in seconds
  python3 live_hit_rate_analyzer.py --config config.json \\
      --horizons 10,30,60,180,600

  # Symbol subset
  python3 live_hit_rate_analyzer.py --config config.json \\
      --symbols RELIANCE-EQ,TCS-EQ,HDFCBANK-EQ

  # Custom transaction cost (0.10% instead of default 0.06%)
  python3 live_hit_rate_analyzer.py --config config.json --cost-pct 0.001
""",
    )
    p.add_argument("--config", default="config.json",
                   help="Angel One config file (default: config.json)")
    p.add_argument("--duration-hours", type=float, default=1.0,
                   help="Max session duration in hours (default: 1.0; "
                        "auto-stops at market close 15:30 IST)")
    p.add_argument("--horizons", default="5,15,30,60,120,300",
                   help="Comma-separated horizons in seconds "
                        "(default: 5,15,30,60,120,300)")
    p.add_argument("--cost-pct", type=float, default=0.0006,
                   help="Explicit round-trip charges excluding spread "
                        "(default: 0.0006 = 0.06%%)")
    p.add_argument("--latency-slippage-bps", type=float, default=0.0,
                   help="Optional adverse slippage per fill in bps, separate "
                        "from bid/ask spread (default: 0)")

    # -- 15-SECOND RULES (Gemini "Sniper" policy — OPT-IN) --
    # Defaults are 0 (OFF) so a plain `bash SETUP.sh` records every
    # actionable signal for observability. Enable explicitly for
    # production hardening. Systemd unit (SETUP.sh --install-service)
    # passes these flags with production values.
    p.add_argument("--entry-confirmation-sec", type=float, default=0.0,
                   help="Signal recorded only after score continuously "
                        "qualifies for N seconds (default 0 = OFF; set to "
                        "15 to enable the 15-second sniper entry rule). "
                        "Cancels + rearms on direction flip or score "
                        "falling below --entry-score / --entry-evidence.")
    p.add_argument("--entry-score", type=float, default=4.0,
                   help="Min |smoothed_score| a signal must maintain during "
                        "the confirmation window (default 4.0 = calibrated "
                        "STRONG threshold). Only applied when "
                        "--entry-confirmation-sec > 0.")
    p.add_argument("--entry-evidence", type=float, default=30.0,
                   help="Min evidence_strength during confirmation (default 30). "
                        "Only applied when --entry-confirmation-sec > 0.")
    p.add_argument("--survival-check-sec", type=float, default=0.0,
                   help="One-shot MFE check N seconds after entry; if MFE "
                        "is below --survival-min-favor-pct, the signal is "
                        "closed at that moment (default 0 = OFF; set to 15 "
                        "to enable the 15-second survival exit rule).")
    p.add_argument("--engine-demo", action="store_true",
                   help="Run the 8-scenario BookDynamicsEngine self-test "
                        "(no config or network needed) and exit.")
    p.add_argument("--stale-feed-sec", type=float, default=90.0,
                   help="Exit with code 75 (for systemd auto-restart) if no "
                        "valid tick arrives for this many seconds during NSE "
                        "market hours. Default 90.0. Set 0 to disable.")
    p.add_argument("--survival-min-favor-pct", type=float, default=0.0001,
                   help="Minimum favorable MFE %% within the survival "
                        "window (default 0.0001 = 0.01%%). Only applied when "
                        "--survival-check-sec > 0.")
    p.add_argument("--symbols", default=None,
                   help="Comma-separated symbol subset (default: all from config)")
    p.add_argument("--min-samples", type=int, default=20,
                   help="Minimum samples for confident verdict (default: 20)")
    p.add_argument("--dedup-seconds", type=float, default=5.0,
                   help="Signal dedup window in seconds (default: 5.0). "
                        "Same state fires within this window are ignored to "
                        "prevent memory/disk blowup during sustained signals.")

    # -- Score threshold overrides (calibrated to real market data) --
    # Defaults: strong=4.0, normal=3.0, weak=2.0 (from empirical 67k
    # signals; smoothed_score rarely exceeds ±5 due to EMA + weighted-avg
    # dampening). Old defaults (strong=8) were unreachable.
    p.add_argument("--strong-threshold", type=float, default=None,
                   help="Score threshold for STRONG_LONG / STRONG_SHORT state "
                        "(default: 4.0). Signals with |score| ≥ this fire as "
                        "STRONG state. Old default 8.0 was unreachable — "
                        "calibrated to real live NSE data.")
    p.add_argument("--normal-threshold", type=float, default=None,
                   help="Score threshold for LONG / SHORT state "
                        "(default: 3.0)")
    p.add_argument("--weak-threshold", type=float, default=None,
                   help="Score threshold for WEAK_LONG / WEAK_SHORT state "
                        "(default: 2.0). Below this = NEUTRAL (no signal).")
    p.add_argument("--ema-alpha", type=float, default=None,
                   help="EMA smoothing factor for score (default: 0.3). "
                        "Higher = faster response to raw score changes "
                        "(0.6 = tighter tracking, but more noise). "
                        "Lower = smoother but delayed (0.15 = very stable).")

    # -- Signal state filter (which states to record) --
    p.add_argument("--strong-only", action="store_true",
                   help="Record ONLY STRONG_LONG + STRONG_SHORT signals. "
                        "Skips WEAK_LONG/WEAK_SHORT/LONG/SHORT entirely. "
                        "Best for focused testing of high-conviction signals "
                        "(user's 44-min data showed STRONG=56%% hit vs "
                        "WEAK=44%%). Recommended after enough data collection.")
    p.add_argument("--skip-weak", action="store_true",
                   help="Record STRONG + LONG/SHORT (i.e., skip only WEAK "
                        "signals). Middle ground between default (all) and "
                        "--strong-only.")

    # -- Signal quality gates (OPTIONAL — all default to disabled) --
    p.add_argument("--session-filter", action="store_true",
                   help="Enable NSE session phase filter. Signals fired "
                        "during LUNCH, PRE_OPEN, or after 15:15 (cutoff) are "
                        "dropped. Default: disabled (record all phases). "
                        "Recommended for production live use.")
    p.add_argument("--allowed-phases",
                   default="OPENING,MORNING,AFTERNOON",
                   help="Comma-separated phase names to allow when "
                        "--session-filter is set. Options: OPENING, MORNING, "
                        "LUNCH, AFTERNOON, PRE_CLOSE, CLOSING. "
                        "Default: OPENING,MORNING,AFTERNOON")
    p.add_argument("--holidays", default="",
                   help="Comma-separated trading holidays in YYYY-MM-DD "
                        "format. Signals on these dates are blocked. "
                        "Example: 2026-08-15,2026-10-02")
    p.add_argument("--no-entry-cutoff", default="15:15",
                   help="HH:MM after which no new signals recorded "
                        "(IST). Default 15:15. Set to 15:30 to disable.")

    p.add_argument("--min-rvol", type=float, default=0.0,
                   help="Minimum Relative Volume required to record signal. "
                        "0.0 = disabled (default). 1.5 = require 1.5× "
                        "20-min average. Higher = more selective. "
                        "Recommended after 10+ days of live data: 1.2 to 2.0.")
    p.add_argument("--rvol-window-minutes", type=int, default=20,
                   help="RVOL rolling window size in minutes (default: 20)")
    p.add_argument("--rvol-warmup-buckets", type=int, default=5,
                   help="RVOL warm-up buckets needed before signals gated. "
                        "Default 5 (= 5 min for 1-min buckets). During "
                        "warm-up, signals are allowed unless "
                        "--rvol-strict-warmup is set.")
    p.add_argument("--rvol-strict-warmup", action="store_true",
                   help="If set, signals during RVOL warm-up are BLOCKED "
                        "(safest for empirical testing). Default: allow "
                        "warm-up signals but count separately in report.")

    # -- Real-market defensive diagnostics --
    p.add_argument("--diagnose", action="store_true",
                   help="Save first N raw WebSocket messages to logs/raw_ws_dump.jsonl "
                        "and echo first 5 to console. CRITICAL for first-time market "
                        "deployment to verify Angel One field-name assumptions. "
                        "Recommended for very first live run.")
    p.add_argument("--dump-count", type=int, default=100,
                   help="If --diagnose, save this many raw messages (default: 100)")
    p.add_argument("--dump-path", default="logs/raw_ws_dump.jsonl",
                   help="Path for raw WS message dump (default: logs/raw_ws_dump.jsonl)")
    p.add_argument("--log-path", default="logs/hit_rate_predictions.jsonl",
                   help="Path for prediction audit log")
    p.add_argument("--report-path", default="logs/hit_rate_report.txt",
                   help="Path for EOD report")
    p.add_argument("--no-ui", action="store_true",
                   help="Headless mode (no rich UI; periodic text status)")
    p.add_argument("--skip-market-hours-check", action="store_true",
                   help="Don't auto-stop at market close (for after-hours testing)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Short-circuit: engine self-test (no config, no network needed)
    if getattr(args, "engine_demo", False):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        _engine_demo()
        return 0

    # Load config
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"\n❌ Config error: {e}", file=sys.stderr)
        print(f"   Copy config.example.json to {args.config} and fill "
              f"Angel One credentials.\n", file=sys.stderr)
        return 2

    # Filter symbols if requested
    if args.symbols:
        subset = {s.strip() for s in args.symbols.split(",") if s.strip()}
        config.symbols = [s for s in config.symbols if s in subset] + \
                         [s for s in subset if s not in config.symbols]

    setup_logging(config)
    logger.info("═" * 82)
    logger.info(" 📊 NSE LIVE HIT RATE ANALYZER")
    logger.info("═" * 82)

    # Parse horizons
    try:
        horizons = [float(h.strip()) for h in args.horizons.split(",")]
    except ValueError:
        print(f"\n❌ Invalid --horizons: {args.horizons}", file=sys.stderr)
        return 2

    logger.info(f"  Config          : {args.config}")
    logger.info(f"  Symbols         : {len(config.symbols)}")
    logger.info(f"  Horizons        : {horizons} seconds")
    logger.info(f"  Cost model      : -{args.cost_pct*100:.4f}% round-trip")
    logger.info(f"  Min samples     : {args.min_samples}")
    logger.info(f"  Max duration    : {args.duration_hours} hours")
    logger.info(f"  Log path        : {args.log_path}")

    # ---- Optional signal quality gates ----
    session_manager: Optional[SessionStateManager] = None
    rvol_calculator: Optional[RVOLCalculator] = None
    allowed_phases_set: Optional[FrozenSet[SessionPhase]] = None

    if args.session_filter:
        # Parse --no-entry-cutoff (HH:MM)
        try:
            hh, mm = args.no_entry_cutoff.split(":")
            from datetime import time as _dt_time
            cutoff = _dt_time(int(hh), int(mm))
        except (ValueError, AttributeError):
            print(f"\n❌ Invalid --no-entry-cutoff '{args.no_entry_cutoff}' "
                  f"(expected HH:MM)\n", file=sys.stderr)
            return 2

        # Parse --holidays
        holidays = [h.strip() for h in args.holidays.split(",") if h.strip()]
        session_manager = SessionStateManager(
            holidays=holidays,
            no_new_entry_after=cutoff,
        )

        # Parse allowed phases
        allowed_phases: Set[SessionPhase] = set()
        for name in args.allowed_phases.split(","):
            name = name.strip().upper()
            if not name:
                continue
            try:
                allowed_phases.add(SessionPhase[name])
            except KeyError:
                print(f"\n❌ Invalid phase name '{name}'. "
                      f"Options: {', '.join(p.name for p in SessionPhase)}\n",
                      file=sys.stderr)
                return 2
        allowed_phases_set = frozenset(allowed_phases) if allowed_phases else None
        logger.info(f"  Session filter  : ENABLED "
                    f"(allowed={sorted(p.name for p in allowed_phases)}, "
                    f"cutoff={args.no_entry_cutoff}, "
                    f"holidays={holidays})")
    else:
        logger.info(f"  Session filter  : disabled")

    if args.min_rvol > 0.0:
        rvol_calculator = RVOLCalculator(
            window_minutes=args.rvol_window_minutes,
            warmup_buckets=args.rvol_warmup_buckets,
        )
        logger.info(f"  RVOL gate       : ENABLED "
                    f"(min_rvol={args.min_rvol:.2f}, "
                    f"window={args.rvol_window_minutes}min, "
                    f"warmup={args.rvol_warmup_buckets} buckets"
                    f"{', STRICT warmup' if args.rvol_strict_warmup else ''})")
    else:
        # If session filter is on but RVOL not required, we STILL create
        # the calculator so stats show up in the report (informational only).
        # But NO gating. Skip creation to keep memory lean.
        logger.info(f"  RVOL gate       : disabled")

    # Prerequisites
    if not SMARTAPI_AVAILABLE:
        print("\n❌ smartapi-python not installed. Run:\n"
              "    pip install -r requirements.txt\n", file=sys.stderr)
        return 3

    # Market hours check
    if not args.skip_market_hours_check and not is_market_hours():
        logger.warning("Outside NSE market hours (Mon-Fri 9:15-15:30 IST).")
        logger.warning("WebSocket may deliver stale/no ticks.")
        logger.warning("Use --skip-market-hours-check to suppress this warning.")

    # Build analyzer
    analyzer = HitRateAnalyzer(
        horizons_s=horizons,
        transaction_cost_pct=args.cost_pct,
        latency_slippage_bps=args.latency_slippage_bps,
        log_path=args.log_path,
        min_samples_for_verdict=args.min_samples,
        signal_dedup_seconds=args.dedup_seconds,
        session_manager=session_manager,
        allowed_phases=allowed_phases_set,
        rvol_calculator=rvol_calculator,
        min_rvol=args.min_rvol,
        entry_confirmation_seconds=args.entry_confirmation_sec,
        entry_score_threshold=args.entry_score,
        entry_min_evidence=args.entry_evidence,
        survival_check_seconds=args.survival_check_sec,
        survival_min_favor_pct=args.survival_min_favor_pct,
    )
    # Propagate strict-warmup flag (constructor doesn't take it directly
    # to keep signature small; setting attribute directly is cheap)
    analyzer.strict_rvol_warmup = args.rvol_strict_warmup

    # -- State filter (which signal states to record) --
    if args.strong_only:
        analyzer.allowed_signal_states = set(_STRONG_STATES)
        logger.info(f"  State filter    : STRONG_LONG + STRONG_SHORT only "
                    f"(WEAK/LONG/SHORT skipped)")
    elif args.skip_weak:
        analyzer.allowed_signal_states = set(_NORMAL_AND_STRONG_STATES)
        logger.info(f"  State filter    : STRONG + LONG/SHORT (WEAK skipped)")
    else:
        logger.info(f"  State filter    : all actionable (STRONG + LONG/SHORT + WEAK)")

    # Build custom EngineConfig if user overrode thresholds
    engine_config = EngineConfig()
    threshold_overrides = []
    if args.strong_threshold is not None:
        engine_config.threshold_strong = args.strong_threshold
        threshold_overrides.append(f"strong={args.strong_threshold}")
    if args.normal_threshold is not None:
        engine_config.threshold_normal = args.normal_threshold
        threshold_overrides.append(f"normal={args.normal_threshold}")
    if args.weak_threshold is not None:
        engine_config.threshold_weak = args.weak_threshold
        threshold_overrides.append(f"weak={args.weak_threshold}")
    if args.ema_alpha is not None:
        engine_config.ema_alpha = args.ema_alpha
        threshold_overrides.append(f"ema_alpha={args.ema_alpha}")

    logger.info(f"  Score thresholds: STRONG≥{engine_config.threshold_strong}, "
                f"NORMAL≥{engine_config.threshold_normal}, "
                f"WEAK≥{engine_config.threshold_weak}, "
                f"EMA_alpha={engine_config.ema_alpha}"
                f"{' (overridden: ' + ', '.join(threshold_overrides) + ')' if threshold_overrides else ' (defaults)'}")

    # -- FILTER SUMMARY --
    # Print explicitly which gates are ACTIVE so operators can see at a
    # glance why signals might (or might not) be showing up.
    print()
    print("=" * 72)
    print("  🎛️  ACTIVE FILTERS")
    print("=" * 72)
    print(f"  State filter        : "
          f"{'STRONG only' if args.strong_only else 'STRONG + LONG/SHORT (skip WEAK)' if args.skip_weak else 'ALL actionable (incl. WEAK)'}")
    print(f"  Entry confirmation  : "
          f"{'ON — score must hold ≥ ' + str(args.entry_score) + ' for ' + str(int(args.entry_confirmation_sec)) + 's continuously' if args.entry_confirmation_sec > 0 else 'OFF (signals recorded on first fire)'}")
    print(f"  Survival exit       : "
          f"{'ON — square off at ' + str(int(args.survival_check_sec)) + 's if MFE < ' + f'{args.survival_min_favor_pct*100:.4f}%' if args.survival_check_sec > 0 else 'OFF (signals held to full horizon)'}")
    print(f"  Session phase gate  : "
          f"{'ON (' + args.allowed_phases + ')' if args.session_filter else 'OFF (LUNCH / PRE_CLOSE not skipped)'}")
    print(f"  RVOL gate           : "
          f"{'ON — min ' + str(args.min_rvol) + '× ' + str(args.rvol_window_minutes) + '-min avg' if args.min_rvol > 0 else 'OFF (no volume filter)'}")
    print(f"  Stale-feed guard    : "
          f"{'ON — exit 75 after ' + str(int(args.stale_feed_sec)) + 's silence in market hours' if args.stale_feed_sec > 0 else 'OFF'}")
    print(f"  Cost model          : "
          f"{args.cost_pct*100:.4f}% charges + {args.latency_slippage_bps:.2f} bps/fill slippage "
          f"(spread already via bid/ask)")
    # If no filters are active — remind the user what to try if they see
    # "no open signal yet" for a long time.
    no_filters = (
        not args.strong_only and not args.skip_weak
        and args.entry_confirmation_sec == 0.0
        and args.survival_check_sec == 0.0
        and not args.session_filter
        and args.min_rvol == 0.0
    )
    if no_filters:
        print()
        print("  ℹ  NO filters active — every actionable (WEAK/LONG/STRONG) signal")
        print("     will be recorded. Expect first signals within 1-2 minutes of")
        print("     market open. Zero signals after 5+ min = data-flow issue.")
    print("=" * 72)

    # Build session (with optional diagnostic dump)
    session = LiveHitRateSession(
        config=config, analyzer=analyzer,
        diagnose=args.diagnose,
        dump_count=args.dump_count,
        dump_path=args.dump_path,
        engine_config=engine_config,
    )
    session.stale_feed_seconds = max(0.0, float(args.stale_feed_sec))

    # -- STARTUP PROGRESS INDICATORS (each stage clearly logged) --
    print()
    print("=" * 72)
    print("  STAGE 1/4 — Logging in to Angel One SmartAPI…")
    print("=" * 72)
    try:
        session.connector.login()
        print("  ✅ Logged in successfully")
    except Exception as e:
        print(f"  ❌ Login failed: {e}")
        logger.exception("Angel One login failed")
        return 4

    print()
    print("=" * 72)
    print("  STAGE 2/4 — Loading scrip master + resolving tokens…")
    print("=" * 72)
    try:
        session.connector.load_scrip_master()
        resolved, missing = session.connector.resolve_tokens()
        if not resolved:
            print("  ❌ No symbols resolved! Check config.symbols.")
            return 4
        session.token_to_symbol = {t: s for s, t in resolved.items()}
        print(f"  ✅ Resolved {len(resolved)}/{len(config.symbols)} symbols")
        if missing:
            print(f"  ⚠  Missing (skipped): {', '.join(missing[:5])}"
                  f"{'…' if len(missing) > 5 else ''}")
    except Exception as e:
        print(f"  ❌ Token resolution failed: {e}")
        logger.exception("Token resolution failed")
        return 4

    if args.diagnose:
        print()
        print("=" * 72)
        print("  🔍 DIAGNOSTIC MODE ACTIVE")
        print("=" * 72)
        print(f"  First {args.dump_count} raw WS messages will be saved to:")
        print(f"    {args.dump_path}")
        print("  First 5 messages will also print to this console.")
        print("  Inspect these to verify Angel One field-name assumptions.")
        print()

    # Shutdown handling
    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        logger.info("Signal %s received; stopping…", signum)
        stop_event.set()

    _signal_mod.signal(_signal_mod.SIGINT, _handle_signal)
    _signal_mod.signal(_signal_mod.SIGTERM, _handle_signal)

    # Start session
    print()
    print("=" * 72)
    print("  STAGE 3/4 — Starting WebSocket subscription…")
    print("=" * 72)
    try:
        session.start()
        print("  ✅ WebSocket subscription initiated")
        print("     Background health monitor will alert if no data within 30s")
    except Exception as e:
        print(f"  ❌ WebSocket start failed: {e}")
        logger.exception("Session start failed")
        return 5

    print()
    print("=" * 72)
    print("  STAGE 4/4 — Live tracking active. Ctrl+C to stop gracefully.")
    print("=" * 72)
    if args.no_ui or not RICH_AVAILABLE:
        print("  Headless mode — status will print every 10 seconds")
    else:
        print("  Rich UI will render below")
    print()

    # Main loop
    max_duration_sec = args.duration_hours * 3600
    start_ts = time.time()
    last_headless_status = start_ts
    # Set by the wait loops when the health thread detects a silently-dead
    # feed. When True, main returns 75 (EX_TEMPFAIL) so systemd restarts.
    stale_feed_shutdown = False

    try:
        if args.no_ui or not RICH_AVAILABLE:
            if not RICH_AVAILABLE:
                logger.warning("rich not installed — running headless.")
            print("\nLive hit rate tracker running headless. Ctrl+C to stop.")
            print(f"Predictions log: {args.log_path}\n")
            while not stop_event.is_set():
                time.sleep(2.0)
                elapsed = time.time() - start_ts
                if elapsed >= max_duration_sec:
                    logger.info("Max duration reached.")
                    break
                if not args.skip_market_hours_check and seconds_until_market_close() <= 0:
                    logger.info("Market close reached.")
                    break
                if session.stale_feed_detected:
                    stale_feed_shutdown = True
                    break
                # Status every 10 seconds
                if time.time() - last_headless_status >= 10.0:
                    last_headless_status = time.time()
                    _print_headless_status(session, analyzer)
        else:
            ui = HitRateUI(session, analyzer, refresh_ms=1000)
            ui_thread = threading.Thread(target=ui.run, name="ui", daemon=True)
            ui_thread.start()
            while not stop_event.is_set():
                time.sleep(1.0)
                elapsed = time.time() - start_ts
                if elapsed >= max_duration_sec:
                    logger.info("Max duration reached.")
                    break
                if not args.skip_market_hours_check and seconds_until_market_close() <= 0:
                    logger.info("Market close reached.")
                    break
                if session.stale_feed_detected:
                    stale_feed_shutdown = True
                    break
            ui.stop()
            ui_thread.join(timeout=2.0)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt.")
    finally:
        logger.info("Stopping session…")
        session.stop()

    # EOD report — with data quality diagnostic section
    report = generate_eod_report(session, analyzer)
    # Prepend data-quality section (critical for verifying real-market readiness)
    quality_lines: List[str] = []
    quality_lines.append("═" * 82)
    quality_lines.append("  📶 DATA FLOW QUALITY (from real-market run)")
    quality_lines.append("═" * 82)
    quality_lines.extend(session.health.summary_report_lines())
    quality_lines.append("")
    if session.health.msgs_parsed_ok == 0 and session.health.msgs_received > 0:
        quality_lines.append(
            "  🚨 CRITICAL: 100% parse failure. Adapter field-name mismatch.")
        quality_lines.append(
            "     Re-run with --diagnose to inspect actual Angel One payload.")
    elif session.health.msgs_received == 0:
        quality_lines.append(
            "  ⚠  No WebSocket messages received. Check subscription + market hours.")
    else:
        quality_lines.append(
            f"  ✅ Data flow healthy: "
            f"{session.health.msgs_parsed_ok:,} valid ticks parsed.")
    quality_lines.append("═" * 82)
    quality_report = "\n".join(quality_lines) + "\n"

    print()
    print(quality_report)
    print(report)
    print()

    # Save report to file
    try:
        Path(args.report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report_path).write_text(report, encoding="utf-8")
        logger.info(f"EOD report saved: {args.report_path}")
    except Exception as e:
        logger.warning(f"Could not save report to {args.report_path}: {e}")

    # If we broke out because the feed went silent during market hours,
    # exit with EX_TEMPFAIL so a systemd unit with Restart=always will
    # restart us. Partial report above is still written for audit.
    if stale_feed_shutdown:
        logger.critical(
            "Exiting with code 75 (EX_TEMPFAIL) due to stale feed — "
            "systemd should restart the process.",
        )
        return 75

    return 0


if __name__ == "__main__":
    sys.exit(main())
