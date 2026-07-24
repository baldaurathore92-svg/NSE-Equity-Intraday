"""
nse_book_scanner.py — Pure signal-generator scanner (no hit-rate tracking).

Extracted from `live_hit_rate_analyzer.py`. Same engine, same market-data
adapter, same gates (session / RVOL / cooldown), but WITHOUT the horizon-
based P&L measurement layer. Use this file when you only want to WATCH
signals fire live, not measure their post-hoc accuracy.

======================================================================
यह क्या करती है
======================================================================
1. Angel One WebSocket से live depth+quote ticks पढ़ती है
2. Per-symbol BookDynamicsEngine से 20+ book-flow metrics compute करती है
3. Weighted composite score → STRONG_LONG / LONG / WEAK_LONG / NEUTRAL /
   WEAK_SHORT / SHORT / STRONG_SHORT state में classify करती है
4. Actionable state आने पर terminal में print करती है (एक line per signal)

क्या NOT करती है:
- कोई order नहीं भेजती (analysis-only, virtual tool)
- कोई horizon-based hit-rate tracking नहीं (उसके लिए live_hit_rate_analyzer.py)
- कोई EOD report नहीं generate करती
- कोई JSONL audit log नहीं लिखती

======================================================================
कब use करें
======================================================================
- नए symbols पर scanner को "screen करने" के लिए (does it fire at all?)
- Signal generator के behaviour को eyeball करने के लिए
- Engine tuning के दौरान quick feedback loop के लिए
- Low-resource VPS पर hit-rate overhead बचाने के लिए

======================================================================
Quick usage
======================================================================
1. Engine self-test (no config, no network):
       python3 nse_book_scanner.py --engine-demo

2. Live scan (Angel One credentials in config.json चाहिए):
       python3 nse_book_scanner.py --config config.json

3. Strong-only signals with session filter:
       python3 nse_book_scanner.py --config config.json \
           --strong-only --session-filter

======================================================================
Related files
======================================================================
- live_hit_rate_analyzer.py — full hit-rate measurement pipeline
- config.example.json       — Angel One credentials + symbol list
- SETUP.sh                  — one-file installer / systemd wiring
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
    iceberg_suspicion:         float = 0.0   # hidden liquidity refill (magnitude)
    iceberg_side:              str   = ""    # "bid" (bullish), "ask" (bearish), or ""
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
    w_iceberg:             float = 0.0   # default 0 preserves backwards compat

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

    # -- EMA warm-up guard (fixes "Cold Start Trap") --
    # 50 ticks ≈ 10 seconds at 5 tps. Signals suppressed during warmup.
    ema_warmup_ticks: int = 50

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

    # ==============================================================
    # -- Regime gate (added after user feedback exposing that
    #    RegimeState.is_tradeable() and .should_invert_signal() were
    #    only referenced in the diagnostics dict, never in the actual
    #    signal-generation path). --
    # ==============================================================
    # When True, signals fired while regime.is_tradeable() is False
    # (RANDOM regime OR still-warming-up regime detector) are DEMOTED
    # to NEUTRAL. Default False for backwards compat.
    regime_gate_enabled:            bool = False
    regime_invert_mean_reverting:   bool = False


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
        # Warm-up counter — signals suppressed until this reaches
        # cfg.ema_warmup_ticks so EMA has time to converge past its seed.
        self._warmup_ticks: int = 0

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
            self._warmup_ticks = 0
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

        # Track winning iceberg's SIDE so downstream composite scoring can
        # turn it into a directional feature (bid → LONG, ask → SHORT).
        best_score = 0.0
        best_refills = 0
        best_side = ""
        for key in list(candidates):
            info = tracker.get(key)
            if info is None:
                candidates.discard(key)
                continue
            if info["executions_near"] <= 0:
                continue
            r = info["refills"]
            e = info["executions_near"]
            r_norm = 1.0 if r >= 4 else r * 0.25
            e_norm = 1.0 if e >= 4 else e * 0.25
            score = 0.5 * r_norm + 0.5 * e_norm
            if score > best_score:
                best_score = score
                best_refills = r
                best_side = key[0]

        m.iceberg_suspicion = best_score
        m.iceberg_side = best_side
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

        # Iceberg — signed feature (bid → +, ask → -). Previously dead code.
        if m.iceberg_side == "bid":
            f_ice =  clamp(m.iceberg_suspicion, -1, 1)
        elif m.iceberg_side == "ask":
            f_ice = -clamp(m.iceberg_suspicion, -1, 1)
        else:
            f_ice = 0.0

        weighted = [
            (cfg.w_l1_imbalance,        f_l1,   "L1"),
            (cfg.w_top5_imbalance,      f_t5,   "Top5"),
            (cfg.w_weighted_depth,      f_wd,   "WeightedDepth"),
            (cfg.w_book_wide_imbalance, f_bw,   "BookWide"),
            (cfg.w_imbalance_roc,       f_iroc, "ImbalanceROC5s"),
            (cfg.w_liquidity_flow,      f_flow, "LiqFlow"),
            (cfg.w_aggressor_ratio,     f_agg,  "Aggressor5s"),
            (cfg.w_mid_response,        f_mid,  "MidROC5s"),
            (cfg.w_iceberg,             f_ice,  "Iceberg"),
        ]
        w_sum = sum(w for w, _, _ in weighted)
        raw_norm = safe_div(sum(w * v for w, v, _ in weighted), w_sum)  # in [-1, +1]

        # Spoof dampener — reduces conviction, does not flip direction
        dampener = 1.0 - cfg.spoof_dampener_strength * m.spoofing_suspicion
        adjusted = raw_norm * clamp(dampener, 0.0, 1.0)

        # Scale to [-10, +10]
        raw_score = clamp(adjusted * 10.0, -10.0, 10.0)

        # EMA smoothing — seed at 0.0 (neutral), NOT at first raw_score.
        # Fixes "9:15 AM Cold Start Trap" where a noise tick pinned the EMA.
        if self._ema_score is None:
            self._ema_score = 0.0
        a = cfg.ema_alpha
        self._ema_score = a * raw_score + (1.0 - a) * self._ema_score
        smoothed = self._ema_score

        # Track ticks-since-startup for the warm-up gate below.
        self._warmup_ticks += 1

        # =====================================================
        # REGIME GATE (opt-in via EngineConfig.regime_gate_enabled).
        # Two operations applied BEFORE state mapping so every field
        # (state, reasons, evidence) reflects the gate's decision:
        #   (a) regime_invert_mean_reverting: flip score sign in a
        #       confirmed MEAN_REVERTING regime (contrarian entry).
        #   (b) regime_gate_enabled: clamp score to 0 when regime is
        #       not tradeable (RANDOM or not-yet-confident).
        # =====================================================
        regime_note: Optional[str] = None
        if cfg.regime_invert_mean_reverting and m.regime.should_invert_signal():
            smoothed = -smoothed
            regime_note = f"Signal inverted (regime={m.regime.label})"
        if cfg.regime_gate_enabled and not m.regime.is_tradeable():
            smoothed = 0.0
            regime_note = (
                f"Regime gate: {m.regime.label} not tradeable "
                f"(confident={m.regime.is_confident}, trend={m.regime.trend})"
            )

        # State mapping
        state = self._score_to_state(smoothed)

        # -- EMA WARM-UP GATE (fixes 9:15 AM Cold Start Trap) --
        if self._warmup_ticks < cfg.ema_warmup_ticks:
            state = SignalState.NEUTRAL
            warmup_note = (
                f"EMA warm-up: {self._warmup_ticks}/{cfg.ema_warmup_ticks} ticks"
            )
        else:
            warmup_note = None

        # Evidence strength = |score| * agreement factor
        agreement = self._agreement_ratio(weighted, smoothed)
        evidence = clamp(abs(smoothed) * (0.5 + 0.5 * agreement) * 10.0, 0.0, 100.0)

        reasons = self._compose_reasons(m, weighted, smoothed, agreement)
        if warmup_note is not None:
            reasons.insert(0, warmup_note)
        if regime_note is not None:
            reasons.insert(0, regime_note)

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



# ============================================================
# 6. TIME-OF-DAY HELPERS
# ============================================================

def is_market_hours() -> bool:
    """NSE cash market open (9:15 - 15:30 IST, Mon-Fri) — no holiday calendar."""
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return dt_time(9, 15) <= now.time() < dt_time(15, 30)


def seconds_until_market_close() -> float:
    """Seconds until 15:30 IST close (0 if already closed for the day)."""
    now = datetime.now(IST)
    close_dt = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if now >= close_dt:
        return 0.0
    return (close_dt - now).total_seconds()


# ============================================================
# 7. SCANNER CLI + MAIN
# ============================================================

def parse_args() -> argparse.Namespace:
    """
    Minimal CLI for the scanner. Deliberately smaller than the hit-rate
    analyzer's flag set — no horizons, no cost model, no verify tools.

    यह क्यों small है: scanner का mandate सिर्फ signal print करना है।
    Post-hoc measurement is a separate tool (live_hit_rate_analyzer.py).
    """
    p = argparse.ArgumentParser(
        description="NSE order-book scanner — live signal generator "
                    "(no hit-rate tracking, no order placement).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Engine self-test (5 seconds, no config or network needed)
  python3 nse_book_scanner.py --engine-demo

  # Live scan with default config
  python3 nse_book_scanner.py --config config.json

  # Full trading day, STRONG-only signals, headless (systemd-safe)
  python3 nse_book_scanner.py --config config.json \
      --strong-only --duration-hours 6.5

  # With production gates (session + RVOL + entry-confirmation)
  python3 nse_book_scanner.py --config config.json \
      --session-filter --min-rvol 1.2 --entry-confirmation-sec 15
""",
    )
    p.add_argument("--config", default="config.json",
                   help="Angel One config file (default: config.json)")
    p.add_argument("--duration-hours", type=float, default=6.5,
                   help="Max session duration in hours (default: 6.5; "
                        "auto-stops at market close 15:30 IST)")
    p.add_argument("--symbols", default=None,
                   help="Comma-separated symbol subset (default: all from config)")
    p.add_argument("--engine-demo", action="store_true",
                   help="Run the 8-scenario BookDynamicsEngine self-test "
                        "(no config or network needed) and exit.")
    p.add_argument("--stale-feed-sec", type=float, default=90.0,
                   help="Exit with code 75 (for systemd auto-restart) if no "
                        "valid tick arrives for this many seconds during NSE "
                        "market hours. Default 90.0. Set 0 to disable.")

    # -- Score threshold overrides --
    p.add_argument("--strong-threshold", type=float, default=None,
                   help="Score threshold for STRONG_* states (default: 4.0).")
    p.add_argument("--normal-threshold", type=float, default=None,
                   help="Score threshold for LONG/SHORT states (default: 3.0)")
    p.add_argument("--weak-threshold", type=float, default=None,
                   help="Score threshold for WEAK_* states (default: 2.0).")
    p.add_argument("--ema-alpha", type=float, default=None,
                   help="EMA smoothing factor for score (default: 0.3).")

    # -- Regime gate (opt-in) --
    p.add_argument("--regime-gate", action="store_true",
                   help="Drop signals when regime is RANDOM or still warming up "
                        "(sets state=NEUTRAL). Default: OFF.")
    p.add_argument("--regime-invert", action="store_true",
                   help="Flip LONG↔SHORT in confirmed MEAN_REVERTING regime. "
                        "Contrarian mode. Default: OFF.")

    # -- Engine tunables (previously hardcoded) --
    p.add_argument("--spoof-dampener-strength", type=float, default=None,
                   help="Spoof suspicion conviction reduction (default: 0.5). "
                        "Set 0.0 to disable and test false-positive rate.")
    p.add_argument("--kill-switch-spread-mult", type=float, default=None,
                   help="Kill-switch trigger = median × N (default: 3.0). "
                        "Try 2.0 for tighter fast-market protection.")

    # -- Feature-weight overrides + warmup + iceberg window --
    p.add_argument("--w-book-wide", type=float, default=None,
                   help="Weight for book_wide_imbalance (default 1.0). "
                        "Set 0.0 if you suspect deep-book spoofing.")
    p.add_argument("--w-aggressor", type=float, default=None,
                   help="Weight for buyer_aggressor_ratio_5s (default 2.0).")
    p.add_argument("--w-iceberg", type=float, default=None,
                   help="Weight for iceberg feature (default 0.0). "
                        "Try 1.0 to enable hidden-liquidity signal.")
    p.add_argument("--ema-warmup-ticks", type=int, default=None,
                   help="Suppress signals for first N ticks (default 50). "
                        "Fixes 9:15 AM Cold Start Trap.")
    p.add_argument("--iceberg-hold-bps", type=float, default=None,
                   help="±bps window around iceberg level (default 5.0). "
                        "Widen to 10.0 for fast sweeps.")

    # -- Signal state filter --
    p.add_argument("--strong-only", action="store_true",
                   help="Print ONLY STRONG_LONG + STRONG_SHORT signals.")
    p.add_argument("--skip-weak", action="store_true",
                   help="Print STRONG + LONG/SHORT (skip WEAK).")

    # -- Signal quality gates (all default to disabled) --
    p.add_argument("--session-filter", action="store_true",
                   help="Enable NSE session phase filter (skip LUNCH etc.).")
    p.add_argument("--allowed-phases",
                   default="OPENING,MORNING,AFTERNOON",
                   help="Comma-separated phase names to allow.")
    p.add_argument("--holidays", default="",
                   help="Comma-separated trading holidays (YYYY-MM-DD).")
    p.add_argument("--no-entry-cutoff", default="15:15",
                   help="HH:MM after which no new signals printed (IST).")

    # -- RVOL gate --
    p.add_argument("--min-rvol", type=float, default=0.0,
                   help="Minimum Relative Volume required (0 = disabled).")
    p.add_argument("--rvol-window-minutes", type=int, default=20,
                   help="RVOL rolling window size in minutes (default: 20).")

    # -- Cooldown gate --
    p.add_argument("--cooldown-seconds", type=float, default=0.0,
                   help="Minimum seconds between same-symbol-same-side signals "
                        "(0 = disabled).")

    # -- Sniper confirmation (opt-in) --
    p.add_argument("--entry-confirmation-sec", type=float, default=0.0,
                   help="Print signal only after score continuously qualifies "
                        "for N seconds (0 = OFF; 15 = 15-second sniper rule).")
    p.add_argument("--entry-score", type=float, default=4.0,
                   help="Min |smoothed_score| during confirmation window.")
    p.add_argument("--entry-evidence", type=float, default=30.0,
                   help="Min evidence_strength during confirmation window.")

    # -- Diagnostics --
    p.add_argument("--diagnose", action="store_true",
                   help="Save first N raw WebSocket messages to disk.")
    p.add_argument("--dump-count", type=int, default=100,
                   help="If --diagnose, save this many raw messages.")
    p.add_argument("--dump-path", default="logs/raw_ws_dump.jsonl",
                   help="Path for raw WS message dump.")

    p.add_argument("--skip-market-hours-check", action="store_true",
                   help="Don't auto-stop at market close (for after-hours testing)")
    return p.parse_args()


def _format_signal_line(res: SignalResult, snap_ltp: float,
                        snap_bid: Optional[float],
                        snap_ask: Optional[float]) -> str:
    """
    Compact one-line human-readable signal print.

    Format:
      HH:MM:SS.mmm  SYMBOL       STATE          score=+3.42  ev=52.1
                                                 ltp=1401.80  bid=1401.75 ask=1401.85
                                                 reasons=BookWide+, Aggressor5s+, ...
    """
    ts_str = datetime.fromtimestamp(res.timestamp, tz=IST).strftime("%H:%M:%S.%f")[:-3]
    bid_str = f"{snap_bid:.2f}" if snap_bid else "-"
    ask_str = f"{snap_ask:.2f}" if snap_ask else "-"
    top_reasons = ", ".join(res.reasons[:3])
    return (
        f"{ts_str}  {res.symbol:<14} {res.state.value:<13} "
        f"score={res.smoothed_score:+.2f}  ev={res.evidence_strength:.1f}  "
        f"ltp={snap_ltp:.2f}  bid={bid_str} ask={ask_str}"
        + (f"\n                {' ' * 40}reasons: {top_reasons}" if top_reasons else "")
    )


def main() -> int:
    """
    Scanner main event loop.

    Flow:
      1. Short-circuit --engine-demo (no config / network)
      2. Load config; wire logging
      3. Determine allowed signal states (all / strong-only / skip-weak)
      4. Build EngineConfig from CLI threshold overrides
      5. Build optional gates: session, RVOL, cooldown
      6. Connect to Angel One WebSocket
      7. On each tick:
           snap = adapter.parse(msg)
           result = engine(snap).update(snap)
           if actionable AND passes gates:  print signal
      8. Auto-stop at market close (unless --skip-market-hours-check)
    """
    args = parse_args()

    # Short-circuit: engine self-test
    if getattr(args, "engine_demo", False):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        _engine_demo()
        return 0

    if not SMARTAPI_AVAILABLE:
        print("❌ Angel One SmartAPI SDK not installed. Run:", file=sys.stderr)
        print("   pip install smartapi-python pyotp", file=sys.stderr)
        return 2

    # Load config
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"❌ Config error: {e}", file=sys.stderr)
        return 2

    # Filter symbols if requested
    if args.symbols:
        subset = {s.strip() for s in args.symbols.split(",") if s.strip()}
        config.symbols = [s for s in config.symbols if s in subset] +                          [s for s in subset if s not in config.symbols]

    setup_logging(config)
    logger.info("═" * 82)
    logger.info(" 📡 NSE BOOK-DYNAMICS SCANNER (signal generator only)")
    logger.info("═" * 82)
    logger.info(f"  Config          : {args.config}")
    logger.info(f"  Symbols         : {len(config.symbols)}")

    # Build engine config from CLI overrides
    engine_config = EngineConfig()
    if args.strong_threshold is not None: engine_config.threshold_strong = args.strong_threshold
    if args.normal_threshold is not None: engine_config.threshold_normal = args.normal_threshold
    if args.weak_threshold  is not None:  engine_config.threshold_weak  = args.weak_threshold
    if args.ema_alpha       is not None:  engine_config.ema_alpha       = args.ema_alpha
    # -- Regime gate wiring (opt-in) --
    if args.regime_gate:
        engine_config.regime_gate_enabled = True
        logger.info("  Regime gate     : ENABLED (signals demoted when RANDOM/warming)")
    if args.regime_invert:
        engine_config.regime_invert_mean_reverting = True
        logger.info("  Regime invert   : ENABLED (LONG↔SHORT in MEAN_REVERTING)")
    # -- Spoof + kill-switch tunables --
    if args.spoof_dampener_strength is not None:
        engine_config.spoof_dampener_strength = args.spoof_dampener_strength
        logger.info(f"  Spoof dampener  : {args.spoof_dampener_strength:.2f}"
                    + (" (DISABLED)" if args.spoof_dampener_strength == 0.0 else ""))
    if args.kill_switch_spread_mult is not None:
        engine_config.kill_switch_spread_multiplier = args.kill_switch_spread_mult
        logger.info(f"  Kill-switch mult: {args.kill_switch_spread_mult:.1f}× median spread")
    # -- Feature-weight + engine tunables (scan-3 fixes) --
    if args.w_book_wide is not None:
        engine_config.w_book_wide_imbalance = args.w_book_wide
        logger.info(f"  w_book_wide     : {args.w_book_wide:.2f}")
    if args.w_aggressor is not None:
        engine_config.w_aggressor_ratio = args.w_aggressor
        logger.info(f"  w_aggressor     : {args.w_aggressor:.2f}")
    if args.w_iceberg is not None:
        engine_config.w_iceberg = args.w_iceberg
        logger.info(f"  w_iceberg       : {args.w_iceberg:.2f}")
    if args.ema_warmup_ticks is not None:
        engine_config.ema_warmup_ticks = args.ema_warmup_ticks
        logger.info(f"  EMA warmup      : {args.ema_warmup_ticks} ticks")
    if args.iceberg_hold_bps is not None:
        engine_config.iceberg_price_hold_bps = args.iceberg_hold_bps
        logger.info(f"  Iceberg window  : ±{args.iceberg_hold_bps:.1f} bps")

    # Determine allowed state set
    if args.strong_only:
        allowed_states = set(_STRONG_STATES)
        logger.info(f"  State filter    : STRONG only")
    elif args.skip_weak:
        allowed_states = set(_NORMAL_AND_STRONG_STATES)
        logger.info(f"  State filter    : STRONG + LONG/SHORT (WEAK skipped)")
    else:
        allowed_states = set(_ACTIONABLE_STATES)
        # User's live-data analysis showed WEAK signals dominate at ~4.5/sec
        # and drag hit-rate near zero. Warn loudly if operator is running
        # with the noisy default.
        logger.warning("═" * 72)
        logger.warning("  ⚠  STATE FILTER: ALL actionable (includes WEAK — HIGH NOISE)")
        logger.warning("     WEAK signals dominate signal volume (~90%+ of firings)")
        logger.warning("     and typically show <10 %% hit rate at 5s horizons.")
        logger.warning("     Recommendation: run with --skip-weak or --strong-only")
        logger.warning("═" * 72)

    # Build optional gates
    session_manager: Optional[SessionStateManager] = None
    allowed_phases_set: Optional[FrozenSet[SessionPhase]] = None
    if args.session_filter:
        holidays_set = set()
        if args.holidays:
            for h in args.holidays.split(","):
                h = h.strip()
                if h: holidays_set.add(h)
        try:
            hh, mm = args.no_entry_cutoff.split(":")
            cutoff_time = dt_time(int(hh), int(mm))
        except (ValueError, AttributeError):
            print(f"❌ Invalid --no-entry-cutoff: {args.no_entry_cutoff}", file=sys.stderr)
            return 2
        session_manager = SessionStateManager(
            holidays=holidays_set,
            no_new_entry_after=cutoff_time,
        )
        allowed_phases_set = frozenset(
            SessionPhase[p.strip()] for p in args.allowed_phases.split(",") if p.strip()
        )
        logger.info(f"  Session gate    : ENABLED, phases={sorted(p.name for p in allowed_phases_set)}")

    rvol_calc: Optional[RVOLCalculator] = None
    if args.min_rvol > 0:
        rvol_calc = RVOLCalculator(window_minutes=args.rvol_window_minutes)
        logger.info(f"  RVOL gate       : ENABLED, min={args.min_rvol}")

    cooldown_mgr: Optional[CooldownManager] = None
    if args.cooldown_seconds > 0:
        cooldown_mgr = CooldownManager(cooldown_seconds=args.cooldown_seconds)
        logger.info(f"  Cooldown gate   : ENABLED, {args.cooldown_seconds}s per symbol+side")

    # Confirmation state (per-symbol, in-memory)
    pending_confirmations: Dict[str, Dict[str, Any]] = {}
    entry_armed: Dict[str, bool] = defaultdict(lambda: True)

    def _qualifies(state: str, res: SignalResult) -> bool:
        if state not in _ACTIONABLE_STATES: return False
        if state not in allowed_states: return False
        if abs(res.smoothed_score) < args.entry_score: return False
        if res.evidence_strength < args.entry_evidence: return False
        return True

    def _confirmation_matured(symbol: str, state: str, res: SignalResult, ts: float) -> bool:
        if args.entry_confirmation_sec <= 0: return True
        side = "LONG" if state in _LONG_STATES else "SHORT"
        pending = pending_confirmations.get(symbol)
        if pending is None or pending["side"] != side:
            if not _qualifies(state, res): return False
            pending_confirmations[symbol] = {
                "side": side, "state": state, "start_ts": ts,
                "score": res.smoothed_score, "evidence": res.evidence_strength,
            }
            entry_armed[symbol] = False
            return False
        # Same side ongoing — check maturity
        if not _qualifies(state, res):
            pending_confirmations.pop(symbol, None)
            entry_armed[symbol] = True
            return False
        elapsed = ts - pending["start_ts"]
        if elapsed >= args.entry_confirmation_sec:
            pending_confirmations.pop(symbol, None)
            return True
        return False

    # Per-symbol engine map
    engines: Dict[str, BookDynamicsEngine] = {}
    engine_lock = threading.Lock()
    def _get_engine(symbol: str) -> BookDynamicsEngine:
        with engine_lock:
            if symbol not in engines:
                engines[symbol] = BookDynamicsEngine(config=engine_config)
            return engines[symbol]

    # Connect to Angel One
    connector = AngelOneConnector(config)
    logger.info("  Logging into Angel One...")
    if not connector.login():
        logger.error("Login failed. Check credentials in config.json.")
        return 3
    if not connector.download_scrip_master():
        logger.error("Scrip master download failed.")
        return 4
    resolved, missing = connector.resolve_tokens()
    if not resolved:
        logger.error("No symbols resolved. Check symbol list in config.json.")
        return 5
    logger.info(f"  Resolved        : {len(resolved)} symbols")

    # Signal counters
    total_ticks = 0
    total_signals_printed = 0
    signals_blocked_by_gates = 0
    last_tick_ts: float = time.time()
    shutdown_flag = threading.Event()
    stale_feed_exit_requested = threading.Event()

    def on_tick(evt: Dict[str, Any]) -> None:
        nonlocal total_ticks, total_signals_printed, signals_blocked_by_gates
        nonlocal last_tick_ts
        try:
            snap = AngelOneWSAdapter.parse(evt)
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return
        if snap is None: return
        total_ticks += 1
        last_tick_ts = time.time()

        # Feed RVOL calc
        if rvol_calc is not None:
            rvol_calc.on_tick(snap.symbol, snap.volume_traded, snap.timestamp)

        engine = _get_engine(snap.symbol)
        try:
            result = engine.update(snap)
        except Exception as e:
            logger.debug(f"Engine error for {snap.symbol}: {e}")
            return
        if result is None: return
        state = result.state.value

        # Entry-confirmation gate
        if args.entry_confirmation_sec > 0:
            if not _qualifies(state, result):
                pending_confirmations.pop(snap.symbol, None)
                entry_armed[snap.symbol] = True
                return
            if not _confirmation_matured(snap.symbol, state, result, snap.timestamp):
                return

        # State filter
        if state not in allowed_states:
            signals_blocked_by_gates += 1
            return

        # Session filter
        if session_manager is not None:
            ok, _ = session_manager.is_tradeable(
                snap.timestamp, allowed_phases=allowed_phases_set,
                enforce_no_new_entry_cutoff=True,
            )
            if not ok:
                signals_blocked_by_gates += 1
                return

        # RVOL filter
        if rvol_calc is not None and args.min_rvol > 0:
            rvol = rvol_calc.get_rvol(snap.symbol, snap.timestamp)
            if rvol is not None and rvol < args.min_rvol:
                signals_blocked_by_gates += 1
                return

        # Cooldown filter
        if cooldown_mgr is not None:
            side = "LONG" if state in _LONG_STATES else "SHORT"
            allowed, _ = cooldown_mgr.can_enter(snap.symbol, side, snap.timestamp)
            if not allowed:
                signals_blocked_by_gates += 1
                return
            cooldown_mgr.on_entry(snap.symbol, side, snap.timestamp)

        # PRINT the signal
        line = _format_signal_line(result, snap.ltp, snap.best_bid, snap.best_ask)
        print(line, flush=True)
        total_signals_printed += 1

    connector.set_on_tick(on_tick)

    # Optional raw-WS dumper for --diagnose
    raw_dumper: Optional[RawMessageDumper] = None
    if args.diagnose:
        raw_dumper = RawMessageDumper(path=args.dump_path, max_count=args.dump_count)
        # Wrap on_tick to also capture raw
        original_on_tick = connector._on_tick_cb
        def wrapped(evt):
            raw_dumper.capture(evt)
            if original_on_tick: original_on_tick(evt)
        connector._on_tick_cb = wrapped
        logger.info(f"  Diagnose mode   : ON — first {args.dump_count} raw messages → {args.dump_path}")

    # Session watchdog: SIGINT/SIGTERM → graceful shutdown
    def _sig_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        shutdown_flag.set()
    py_signal.signal(py_signal.SIGINT, _sig_handler)
    py_signal.signal(py_signal.SIGTERM, _sig_handler)

    # Start the WebSocket
    if not connector.start(resolved):
        logger.error("WebSocket start failed.")
        return 6

    # Duration + market-close watchdog
    started_at = time.time()
    max_seconds = args.duration_hours * 3600.0
    logger.info(f"  Duration limit  : {args.duration_hours}h")
    logger.info(f"  Stale-feed limit: {args.stale_feed_sec}s")
    logger.info("─" * 82)
    logger.info("  📡 Scanner armed. Printing actionable signals below...")
    logger.info("─" * 82)

    try:
        while not shutdown_flag.is_set():
            time.sleep(1.0)
            elapsed = time.time() - started_at
            if elapsed >= max_seconds:
                logger.info("Duration limit reached, stopping.")
                break
            if not args.skip_market_hours_check:
                if not is_market_hours() and elapsed > 30.0:
                    logger.info("Market closed, stopping.")
                    break
            # Stale-feed watchdog
            if args.stale_feed_sec > 0 and is_market_hours():
                since_last = time.time() - last_tick_ts
                if since_last > args.stale_feed_sec:
                    logger.error(
                        f"Stale feed: no ticks for {since_last:.1f}s during market hours. "
                        f"Exiting with code 75 for systemd restart."
                    )
                    stale_feed_exit_requested.set()
                    break
    finally:
        connector.stop()

    # Final summary
    duration_s = time.time() - started_at
    logger.info("─" * 82)
    logger.info("  📊 SCANNER SUMMARY")
    logger.info("─" * 82)
    logger.info(f"  Duration        : {duration_s:.1f}s")
    logger.info(f"  Total ticks     : {total_ticks:,}")
    logger.info(f"  Signals printed : {total_signals_printed:,}")
    logger.info(f"  Blocked by gates: {signals_blocked_by_gates:,}")

    return 75 if stale_feed_exit_requested.is_set() else 0


if __name__ == "__main__":
    sys.exit(main())
