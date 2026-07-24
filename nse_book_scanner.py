#!/usr/bin/env python3
"""
nse_book_scanner.py
===================
NSE Cash Market — Complete Real-Time Order-Flow Scanner (single file).

यह एक ही file में तीन चीज़ें हैं:
  1. BookDynamicsEngine — analytics engine (17 microstructure metrics)
  2. Scanner — 100 symbols parallel में process करने वाला orchestrator
  3. Live UI + JSONL logging + Angel One WebSocket integration

===================================================================
QUICK START
===================================================================

1. Dependencies install करें:
       pip install -r requirements.txt

2. Engine का self-test (5-सेकंड, कोई config नहीं चाहिए):
       python3 nse_book_scanner.py --demo

3. Scanner simulate mode (कोई broker credential नहीं चाहिए):
       cp config.example.json config.json
       python3 nse_book_scanner.py --mode simulate

4. Higher tick rate (opening burst simulation):
       python3 nse_book_scanner.py --mode simulate --sim-rate 30

5. Live trading (Angel One credentials चाहिए):
       python3 nse_book_scanner.py --mode live

===================================================================
ANGEL ONE CREDENTIALS
===================================================================
- smartapi.angelbroking.com → login → "My Apps" → नया app create करें
- API Key + Secret मिलेगा (config.json में डालें)
- 2FA setup: Google Authenticator में QR scan करते समय "Manual entry" पर
  tap करके base32 secret copy करें (यह totp_secret में डालें)
- MPIN = आपका 4-digit trading PIN

===================================================================
FILE OUTPUTS
===================================================================
- logs/signals.jsonl     — हर actionable signal, एक JSON per line
- logs/scanner.log       — System logs (WS status, errors, rotating 10MB × 5)
- logs/scrip_master.json — Angel One का scrip master cache (24hr TTL)

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
# 1. Constants, logger, enums
# ---------------------------------------------------------------------------

EPS = 1e-9
MIN_QTY_FLOOR = 1000                # ROC denominator floor (avoids blowup)
DEFAULT_HISTORY_SEC = 60.0
NSE_TICK_SIZE = 0.05                # NSE Cash minimum price increment

logger = logging.getLogger("BookDynamicsEngine")


class SignalState(Enum):
    STRONG_LONG  = "STRONG_LONG"
    LONG         = "LONG"
    WEAK_LONG    = "WEAK_LONG"
    NEUTRAL      = "NEUTRAL"
    WEAK_SHORT   = "WEAK_SHORT"
    SHORT        = "SHORT"
    STRONG_SHORT = "STRONG_SHORT"
    SUPPRESSED   = "SUPPRESSED"     # kill switch active (spread / circuit / halt)


class AggressorSide(Enum):
    BUYER  = 1
    SELLER = -1
    NA     = 0


# ---------------------------------------------------------------------------
# 2. Data classes — inputs and outputs
# ---------------------------------------------------------------------------

@dataclass
class DepthLevel:
    """एक price level (top-N depth में से एक)।"""
    price: float
    quantity: int

    def __post_init__(self):
        self.price = float(self.price)
        self.quantity = int(self.quantity)


@dataclass
class MarketSnapshot:
    """
    Broker-agnostic snapshot — एक moment का पूरा market state।

    Requirements:
      - `bids` sorted best-first (highest bid price पहले)
      - `asks` sorted best-first (lowest ask price पहले)
      - `timestamp` is the local receive/event clock used for analytics
      - `exchange_timestamp` preserves the broker exchange clock for audit
      - `sequence_number` is the primary ordering/dedup key when available
    """
    timestamp: float                 # event epoch seconds (receive clock preferred)
    symbol: str
    ltp: float                       # last traded price
    ltq: int                         # last traded qty (optional, 0 OK)
    volume_traded: int               # cumulative day volume (monotonically increasing)
    total_buy_qty: int               # exchange-broadcast aggregate BOOK-WIDE
    total_sell_qty: int              # exchange-broadcast aggregate BOOK-WIDE
    bids: List[DepthLevel]           # top-N (typically 5), best-first
    asks: List[DepthLevel]           # top-N (typically 5), best-first

    # Market-data identity / clocks. Existing simulator callers can omit these.
    sequence_number: Optional[int] = None
    exchange_timestamp: Optional[float] = None
    received_timestamp: Optional[float] = None

    # Optional / informational
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
    Shared executable-fill and cost model used by hit-rate and paper trading.

    Spread crossing is captured by bid/ask fills. `transaction_cost_pct` is
    reserved for explicit round-trip charges, while `latency_slippage_bps` is
    an optional, separately visible adverse adjustment per fill.
    """
    transaction_cost_pct: float = 0.0006
    latency_slippage_bps: float = 0.0

    def __post_init__(self) -> None:
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
        """Return executable quote plus optional adverse latency slippage."""
        side = side.upper()
        if side not in {"LONG", "SHORT"}:
            raise ValueError(f"Unsupported side: {side}")

        is_buy = (side == "LONG" and is_entry) or (side == "SHORT" and not is_entry)
        quote = best_ask if is_buy else best_bid
        if quote is None or quote <= 0:
            quote = ltp
        if quote is None or quote <= 0:
            raise ValueError("No positive executable quote or LTP")

        slip = self.latency_slippage_bps / 10000.0
        return quote * (1.0 + slip if is_buy else 1.0 - slip)

    @staticmethod
    def gross_directional_return(side: str, entry_price: float, exit_price: float) -> float:
        if entry_price <= 0:
            return 0.0
        if side.upper() == "SHORT":
            return (entry_price - exit_price) / entry_price
        return (exit_price - entry_price) / entry_price

    def charge_return(self, entry_price: float, exit_price: float) -> float:
        """Round-trip charges as a return on entry notional."""
        if entry_price <= 0:
            return 0.0
        avg_price = (entry_price + exit_price) / 2.0
        return self.transaction_cost_pct * avg_price / entry_price

    def evaluate(self, side: str, entry_price: float, exit_price: float) -> Tuple[float, float, float]:
        """Return (gross executable return, charge return, net return)."""
        gross = self.gross_directional_return(side, entry_price, exit_price)
        charges = self.charge_return(entry_price, exit_price)
        return gross, charges, gross - charges

    def description(self) -> str:
        return (
            f"bid/ask executable + {self.transaction_cost_pct * 100:.4f}% charges"
            f" + {self.latency_slippage_bps:.2f} bps/fill latency"
        )


@dataclass
class RegimeState:
    """
    Phase 2 — Market regime classification per symbol.

    Regime dimensions:
      - Volatility:   LOW / NORMAL / HIGH  (recent σ vs baseline σ)
      - Trend:        TRENDING_UP / TRENDING_DOWN / MEAN_REVERTING / RANDOM
                      (based on lag-1 autocorrelation + mean of recent returns)
      - Depth Bias:   BULL_STRUCTURAL / BEAR_STRUCTURAL / BALANCED
                      (based on rolling book-wide imbalance)

    Trading interpretation:
      - TRENDING regime  → scanner's directional signals are meaningful
      - MEAN_REVERTING   → INVERT signals (contrarian mode)
      - RANDOM           → no edge, DO NOT trade
      - HIGH_VOL         → widen thresholds, smaller position size
      - LOW_VOL          → tighten thresholds, take more marginal signals
    """
    volatility: str = "NORMAL"        # LOW / NORMAL / HIGH
    trend: str = "RANDOM"             # TRENDING_UP / TRENDING_DOWN / MEAN_REVERTING / RANDOM
    depth_bias: str = "BALANCED"      # BULL_STRUCTURAL / BEAR_STRUCTURAL / BALANCED
    volatility_ratio: float = 1.0     # recent_σ / baseline_σ
    autocorr_lag1: float = 0.0        # lag-1 autocorrelation of tick returns
    depth_imbalance_mean: float = 0.0 # rolling mean of book_wide_imbalance
    is_confident: bool = False        # True once warm-up (~500 baseline samples) complete

    @property
    def label(self) -> str:
        """Compact human-readable regime label."""
        v = self.volatility[0]  # L/N/H
        t = {"TRENDING_UP": "T↑", "TRENDING_DOWN": "T↓",
             "MEAN_REVERTING": "MR", "RANDOM": "R"}.get(self.trend, "?")
        d = {"BULL_STRUCTURAL": "B", "BEAR_STRUCTURAL": "S",
             "BALANCED": "N"}.get(self.depth_bias, "?")
        return f"{v}·{t}·{d}"

    def is_tradeable(self) -> bool:
        """Should scanner trade in this regime? False = no edge."""
        if not self.is_confident:
            return False   # not enough data yet
        if self.trend == "RANDOM":
            return False   # no directional edge
        return True

    def should_invert_signal(self) -> bool:
        """In mean-reverting regime, invert LONG↔SHORT (contrarian)."""
        return self.is_confident and self.trend == "MEAN_REVERTING"


class RegimeDetector:
    """
    Per-symbol regime classifier. Lightweight — designed to run alongside
    BookDynamicsEngine for 100+ symbols simultaneously.

    Performance:
      - Recompute only every N ticks (default 100), not per-tick
      - Uses same pattern as our other rolling buffers (deque-based)
      - Total memory per symbol: ~5000 floats = ~40KB
    """

    def __init__(
        self,
        update_every_n_ticks: int = 100,
        recent_window: int = 500,
        baseline_window: int = 5000,
        vol_high_threshold: float = 1.5,
        vol_low_threshold: float = 0.7,
        autocorr_trending_threshold: float = 0.10,
        autocorr_reverting_threshold: float = -0.08,
        depth_bias_threshold: float = 0.15,
    ):
        self.update_every = update_every_n_ticks
        self.vol_high = vol_high_threshold
        self.vol_low = vol_low_threshold
        self.trend_up = autocorr_trending_threshold
        self.trend_down = autocorr_reverting_threshold
        self.depth_bias_thr = depth_bias_threshold

        self.recent_returns:    Deque[float] = deque(maxlen=recent_window)
        self.baseline_returns:  Deque[float] = deque(maxlen=baseline_window)
        self.recent_imbalances: Deque[float] = deque(maxlen=recent_window)

        self.last_price:  Optional[float] = None
        self.tick_count:  int = 0
        self.ticks_since_update: int = 0
        self.current_regime = RegimeState()

    def update(self, ltp: float, book_wide_imbalance: float) -> RegimeState:
        """Called by engine on every tick. Cheap; heavy compute batched."""
        if self.last_price is not None and self.last_price > 0:
            ret = (ltp - self.last_price) / self.last_price
            self.recent_returns.append(ret)
            self.baseline_returns.append(ret)
        self.last_price = ltp
        self.recent_imbalances.append(book_wide_imbalance)

        self.tick_count += 1
        self.ticks_since_update += 1
        if self.ticks_since_update >= self.update_every:
            self.ticks_since_update = 0
            self._recompute()
        return self.current_regime

    def _recompute(self) -> None:
        if len(self.recent_returns) < 50:
            return

        # ---- Volatility regime ----
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

        # ---- Trend regime (via lag-1 autocorrelation of returns) ----
        autocorr = self._autocorr_lag1(self.recent_returns)
        mean_return = sum(self.recent_returns) / len(self.recent_returns)
        if autocorr > self.trend_up:
            trend_regime = "TRENDING_UP" if mean_return > 0 else "TRENDING_DOWN"
        elif autocorr < self.trend_down:
            trend_regime = "MEAN_REVERTING"
        else:
            trend_regime = "RANDOM"

        # ---- Depth bias ----
        depth_mean = sum(self.recent_imbalances) / len(self.recent_imbalances)
        if depth_mean > self.depth_bias_thr:
            depth_regime = "BULL_STRUCTURAL"
        elif depth_mean < -self.depth_bias_thr:
            depth_regime = "BEAR_STRUCTURAL"
        else:
            depth_regime = "BALANCED"

        self.current_regime = RegimeState(
            volatility=vol_regime, trend=trend_regime, depth_bias=depth_regime,
            volatility_ratio=vol_ratio, autocorr_lag1=autocorr,
            depth_imbalance_mean=depth_mean,
            is_confident=(len(self.baseline_returns) >= 500),
        )

    @staticmethod
    def _std(values) -> float:
        n = len(values)
        if n < 2:
            return 0.0
        mean = sum(values) / n
        var = sum((x - mean) ** 2 for x in values) / n
        return var ** 0.5

    @staticmethod
    def _autocorr_lag1(values) -> float:
        """Pearson lag-1 autocorrelation. Range [-1, +1]."""
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
    """All computed microstructure metrics for one snapshot."""
    timestamp: float

    # ---- Static imbalances (each in [-1, +1], positive = bullish) ----
    book_wide_imbalance:     float = 0.0
    l1_imbalance:            float = 0.0
    top5_imbalance:          float = 0.0
    weighted_depth_imbalance: float = 0.0

    # ---- Book dynamics (ROC) ----
    buy_book_roc_1s:  float = 0.0
    buy_book_roc_5s:  float = 0.0
    buy_book_roc_10s: float = 0.0
    sell_book_roc_1s:  float = 0.0
    sell_book_roc_5s:  float = 0.0
    sell_book_roc_10s: float = 0.0
    imbalance_roc_5s: float = 0.0

    # ---- Liquidity flow (Δ decomposition, integers) ----
    buy_added:      int = 0
    buy_removed:    int = 0
    sell_added:     int = 0
    sell_removed:   int = 0
    total_added:    int = 0
    total_removed:  int = 0
    book_activity:  int = 0

    # ---- Spread + Mid + Price dynamics ----
    spread:                float = 0.0
    normalized_spread_bps: float = 0.0
    spread_roc_5s:         float = 0.0
    mid_price:             float = 0.0
    mid_price_roc_5s:      float = 0.0

    ltp:              float = 0.0
    ltp_roc_5s:       float = 0.0
    interval_volume:  int   = 0
    buyer_aggressor_ratio_5s: float = 0.5   # [0, 1], baseline 0.5

    # ---- Cross-checks & suspicions (in [0, 1] unless noted) ----
    l1_vs_depth_divergence:    float = 0.0
    execution_likelihood_ask:  float = 0.0
    execution_likelihood_bid:  float = 0.0
    cancellation_suspicion_ask: float = 0.0
    cancellation_suspicion_bid: float = 0.0
    spoofing_suspicion:        float = 0.0
    iceberg_suspicion:         float = 0.0
    replenishment_score:       float = 0.0

    # ---- Phase 2 — Market Regime ----
    regime: RegimeState = field(default_factory=RegimeState)

    # ---- Kill switch ----
    kill_switch_active: bool = False
    kill_switch_reason: Optional[str] = None


@dataclass
class SignalResult:
    """Final engine output per snapshot."""
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
# 3. Utilities
# ---------------------------------------------------------------------------

def safe_div(num: float, den: float, default: float = 0.0) -> float:
    if abs(den) < EPS:
        return default
    return num / den


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def tanh_scale(x: float, k: float = 1.0) -> float:
    """Bounded scaling to (-1, +1)."""
    return math.tanh(k * x)


def price_key(p: float, tick: float = NSE_TICK_SIZE) -> float:
    """Round price to exchange tick precision. Used as dict key for level tracking."""
    return round(round(p / tick) * tick, 2)


class TimeSeriesBuffer:
    """
    High-performance timestamped rolling buffer.

    Design:
      - Parallel lists (_ts, _values) — always sorted ascending by ts
      - `bisect` for O(log N) time-based lookups
      - Batched front-trim every TRIM_INTERVAL appends (amortized O(1))
      - `del list[:idx]` uses C-level bulk shift (fast)

    Complexity:
      - append:            O(1) amortized
      - value_seconds_ago: O(log N)
      - sum_values / values / latest: O(N) / O(1) / O(1)

    यह old deque + linear-scan implementation से 100-500x तेज़ है
    at 1000+ tps single-symbol firehose scenarios.
    """

    _TRIM_INTERVAL = 128   # batched pruning cadence

    def __init__(self, max_seconds: float = DEFAULT_HISTORY_SEC):
        self._ts: List[float] = []
        self._values: List[Any] = []
        self.max_seconds = max_seconds
        self._appends_since_trim = 0

    def append(self, ts: float, value: Any) -> None:
        self._ts.append(ts)
        self._values.append(value)
        self._appends_since_trim += 1
        if self._appends_since_trim >= self._TRIM_INTERVAL:
            self._appends_since_trim = 0
            self._trim(ts)

    def _trim(self, current_ts: float) -> None:
        """Drop entries older than max_seconds. Uses C-level `del` bulk shift."""
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
        Return (ts, value) of newest entry with ts ≤ (current_ts - seconds).
        O(log N) binary search.
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
        if not self._ts:
            return None
        return (self._ts[-1], self._values[-1])

    def values(self) -> List[Any]:
        """Direct reference — caller must not mutate."""
        return self._values

    def sum_values(self) -> float:
        return sum(self._values)

    def __len__(self):
        return len(self._ts)

    def clear(self):
        self._ts.clear()
        self._values.clear()
        self._appends_since_trim = 0


# ---------------------------------------------------------------------------
# 4. Engine configuration
# ---------------------------------------------------------------------------

@dataclass
class EngineConfig:
    # History — केवल जितना lookback चाहिए उतना ही रखें (default 15s covers 10s ROC + margin).
    # बड़ा value = ज़्यादा memory + धीरे scan. यह performance-sensitive है।
    history_seconds: float = 15.0

    # Distance-weighted depth: w_i = exp(-|price_i - mid| / (mid * depth_decay_frac))
    # depth_decay_frac = 0.005 का मतलब ~50bps पर weight 1/e हो जाता है
    depth_decay_frac: float = 0.005

    # Book ROC minimum-magnitude floor (avoids blow-up at small qty)
    min_qty_floor: int = MIN_QTY_FLOOR

    # Kill switch (spread widening)
    kill_switch_spread_multiplier: float = 3.0
    kill_switch_spread_lookback_s: float = 30.0

    # Spoofing suspicion
    spoof_pull_threshold_pct: float = 0.4    # 40% of level qty pulled
    spoof_pull_window_s:      float = 1.5    # rolling suspicion window

    # Iceberg
    iceberg_min_refills:      int   = 2
    iceberg_price_hold_bps:   float = 5.0    # ±5 bps around level for "executions near"

    # Composite weights (unnormalized; engine normalizes by Σw)
    w_l1_imbalance:        float = 1.0
    w_top5_imbalance:      float = 1.5
    w_weighted_depth:      float = 2.0
    w_book_wide_imbalance: float = 1.0
    w_imbalance_roc:       float = 2.5   # leading indicator — highest weight
    w_liquidity_flow:      float = 1.5
    w_aggressor_ratio:     float = 2.0
    w_mid_response:        float = 1.5

    # Spoof suspicion acts as a multiplicative dampener on |score|
    # (spoof reduces conviction — it does NOT flip direction)
    spoof_dampener_strength: float = 0.5

    # EMA smoothing of composite score
    ema_alpha: float = 0.3

    # State thresholds on smoothed score in [-10, +10]
    # ⚠ Empirical calibration (based on 67k signals from live NSE data):
    #    Max abs(smoothed_score) achievable in normal market ≈ 5.0
    #    Even during calm periods, weighted-avg of 8 features + EMA-α=0.3
    #    smoothing means score rarely exceeds ±5.
    #
    # Old defaults (STRONG=8, NORMAL=5) had STRONG_LONG effectively
    # unreachable — 0/67632 signals crossed 6.0 in a 6-min live run.
    # New defaults are calibrated to observed distribution:
    #    ≥ 2.0: 100% of signals (WEAK)
    #    ≥ 3.0: 16% of signals    (NORMAL — LONG/SHORT)
    #    ≥ 4.0: 1%  of signals    (STRONG — STRONG_LONG/STRONG_SHORT)
    threshold_strong: float = 4.0
    threshold_normal: float = 3.0
    threshold_weak:   float = 2.0


# ---------------------------------------------------------------------------
# 5. BookDynamicsEngine
# ---------------------------------------------------------------------------

class BookDynamicsEngine:
    """
    Broker-agnostic order-flow / book-dynamics engine.

    Usage:
        engine = BookDynamicsEngine(config=EngineConfig())
        for snap in stream_of_snapshots():
            result = engine.update(snap)
            if result and result.state != SignalState.NEUTRAL:
                handle(result)

    Thread-safety: `update()` acquires an internal RLock.
    """

    def __init__(self, config: Optional[EngineConfig] = None):
        self.config = config or EngineConfig()
        self._lock = threading.RLock()

        # Snapshot history — केवल यही query होता है (metric_history removed as dead code).
        self._snapshot_history = TimeSeriesBuffer(self.config.history_seconds)

        # Spread rolling window for kill switch (sampled at 100ms cadence, not every tick).
        # यह sample rate 30s में max 300 entries रखता है → fast median compute.
        self._spread_history = TimeSeriesBuffer(self.config.kill_switch_spread_lookback_s)
        self._last_spread_sample_ts: float = -1.0
        self._spread_sample_interval_s: float = 0.1  # 100ms
        self._cached_median_spread: Optional[float] = None
        self._median_dirty: bool = True

        # EMA state
        self._ema_score: Optional[float] = None

        # Iceberg per-level tracker: key=(side, price_key)
        # value: dict(qty_baseline, first_seen, last_seen, refills, executions_near, last_qty)
        self._level_tracker: Dict[Tuple[str, float], Dict[str, Any]] = {}
        # Candidates set — only levels that have shown refill behavior.
        # हम scoring loop में केवल इन्हीं को iterate करते हैं (performance critical)।
        self._iceberg_candidates: set = set()
        # Batched pruning counter (like TimeSeriesBuffer)
        self._iceberg_prune_counter: int = 0
        # Max tracked levels (LRU-style cap)
        self._MAX_TRACKED_LEVELS: int = 500

        # Phase 2 — Per-symbol regime detector
        self._regime_detector = RegimeDetector()

        # Spoof: recent pull events awaiting execution confirmation
        self._pull_events: Deque[Dict[str, Any]] = deque(maxlen=500)

        # Aggressor (tick rule) rolling 5s accumulators
        self._buy_vol_5s   = TimeSeriesBuffer(5.0)
        self._sell_vol_5s  = TimeSeriesBuffer(5.0)
        self._last_agg_side: AggressorSide = AggressorSide.NA

        # Market-data ordering guard. Sequence is authoritative when supplied;
        # receive/event time is only the fallback for legacy/simulated feeds.
        self._last_ts: float = -1.0
        self._last_sequence: Optional[int] = None
        self._last_exchange_ts: Optional[float] = None
        self._last_fingerprint: Optional[Tuple[Any, ...]] = None

    # ================================================================
    # Public API
    # ================================================================

    def update(self, snap: MarketSnapshot) -> Optional[SignalResult]:
        """Ingest one snapshot; return SignalResult (or None if invalid/dup)."""
        with self._lock:
            if not self._validate(snap):
                return None

            metrics = self._compute_metrics(snap)
            signal  = self._generate_signal(snap, metrics)

            # Save state AFTER metric compute (compute needs prev)
            self._snapshot_history.append(snap.timestamp, snap)
            # Spread sampling — reduces sort cost from O(N log N) per tick
            # to a bounded ~300 samples in the 30s window.
            if snap.spread is not None and (
                self._last_spread_sample_ts < 0
                or snap.timestamp - self._last_spread_sample_ts >= self._spread_sample_interval_s
            ):
                self._spread_history.append(snap.timestamp, snap.spread)
                self._last_spread_sample_ts = snap.timestamp
                self._median_dirty = True
            self._last_ts = snap.timestamp
            self._last_sequence = snap.sequence_number
            if snap.exchange_timestamp is not None:
                self._last_exchange_ts = snap.exchange_timestamp
            self._last_fingerprint = self._snapshot_fingerprint(snap)

            return signal

    def reset(self) -> None:
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
    # Validation
    # ================================================================

    @staticmethod
    def _snapshot_fingerprint(snap: MarketSnapshot) -> Tuple[Any, ...]:
        """Fallback identity for legacy feeds that do not provide a sequence."""
        return (
            snap.ltp, snap.ltq, snap.volume_traded,
            snap.total_buy_qty, snap.total_sell_qty,
            tuple((lv.price, lv.quantity) for lv in snap.bids),
            tuple((lv.price, lv.quantity) for lv in snap.asks),
        )

    def _validate(self, snap: MarketSnapshot) -> bool:
        sequence = snap.sequence_number
        if sequence is not None and self._last_sequence is not None:
            if sequence <= self._last_sequence:
                # A reconnect/session can reset the broker sequence. Only accept
                # that reset when exchange time moved forward substantially;
                # otherwise this is duplicate/out-of-order delivery.
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
    # Metric computation
    # ================================================================

    def _compute_metrics(self, snap: MarketSnapshot) -> BookMetrics:
        m = BookMetrics(timestamp=snap.timestamp)

        # ---- Static imbalances ----
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

    # ---- Metric helpers ----

    def _weighted_depth_imbalance(self, snap: MarketSnapshot) -> float:
        """Exponential-decay distance-weighted Top-N imbalance."""
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
        """% of features with sign matching the dominant score sign."""
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
# 7. Synthetic demo — 8 scenarios
# ---------------------------------------------------------------------------

def _demo_snap(ts, ltp, ltq, vol, tbq, tsq, bids, asks, symbol="DEMO"):
    return MarketSnapshot(
        timestamp=ts, symbol=symbol,
        ltp=ltp, ltq=ltq, volume_traded=vol,
        total_buy_qty=tbq, total_sell_qty=tsq,
        bids=[DepthLevel(p, q) for p, q in bids],
        asks=[DepthLevel(p, q) for p, q in asks],
    )


def _demo_print_result(res: Optional[SignalResult], header: str):
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


import argparse
import json
import logging.handlers
import os
import random
import signal as py_signal
import sys
from pathlib import Path

# (engine classes are defined above in this same file)

# ---------------------------------------------------------------------------
# Optional dependencies — graceful fallback
# ---------------------------------------------------------------------------

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

# Angel One deps loaded lazily (only if --mode live)
SMARTAPI_AVAILABLE = False
try:
    import pyotp
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    SMARTAPI_AVAILABLE = True
except ImportError:
    pass

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIP_MASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)
NSE_CM_EXCHANGE_TYPE = 1
SUBSCRIPTION_MODE_SNAP_QUOTE = 3
PAISE_TO_INR = 0.01

logger = logging.getLogger("nse_scanner")


# ---------------------------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------------------------

@dataclass
class ScannerConfig:
    # Angel One creds
    api_key: str = ""
    client_code: str = ""
    pin: str = ""
    totp_secret: str = ""

    # Symbol universe
    symbols: List[str] = field(default_factory=list)

    # Scanner behavior
    min_evidence_strength_to_log: float = 30.0
    log_signal_states: List[str] = field(default_factory=lambda: [
        "WEAK_LONG", "LONG", "STRONG_LONG",
        "WEAK_SHORT", "SHORT", "STRONG_SHORT",
    ])
    signal_dedup_seconds: float = 5.0
    ui_refresh_ms: int = 500
    top_n_display: int = 10

    # Producer-consumer queue between WS thread and processing worker.
    # WS thread enqueues in ~5µs; worker dequeues+processes at engine speed.
    # यह setting 1-core CPU पर hang प्रevent करती है।
    tick_queue_size: int = 20000    # ~2 seconds of NSE peak burst headroom

    # ---- Prediction Tracker (Phase 1: signal accuracy self-validation) ----
    # हर actionable signal पर price capture, फिर horizon के बाद check करना कि
    # actual price signal-direction में move हुई या नहीं। यह real-time proof देता है
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
    """Load scanner config from a JSON file with clear error messages."""
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

    # Reduce noise from libraries
    logging.getLogger("SmartApi").setLevel(logging.WARNING)
    logging.getLogger("websocket").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# 3. Signal recorder — JSONL persistence (thread-safe)
# ---------------------------------------------------------------------------

class SignalRecorder:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._file = None

    def open(self):
        self._file = open(self.path, "a", encoding="utf-8", buffering=1)  # line-buffered

    def close(self):
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None

    def record(self, symbol: str, result: SignalResult) -> None:
        if self._file is None:
            return
        payload = {
            "ts": result.timestamp,
            "symbol": symbol,
            "state": result.state.value,
            "raw_score": round(result.raw_score, 3),
            "smoothed_score": round(result.smoothed_score, 3),
            "evidence": round(result.evidence_strength, 2),
            "reasons": result.reasons,
            "diagnostics": result.diagnostics,
        }
        line = json.dumps(payload, separators=(",", ":"), default=str) + "\n"
        with self._lock:
            try:
                self._file.write(line)
            except Exception as e:
                logger.exception("Signal write failed: %s", e)


# ---------------------------------------------------------------------------
# 4. Prediction Tracker — Signal Accuracy Self-Validation (Phase 1)
# ---------------------------------------------------------------------------

# जो signal states LONG-side हैं और SHORT-side हैं
_LONG_STATES  = {"STRONG_LONG", "LONG", "WEAK_LONG"}
_SHORT_STATES = {"STRONG_SHORT", "SHORT", "WEAK_SHORT"}
_ACTIONABLE_STATES = _LONG_STATES | _SHORT_STATES

# Subset filters — used when users only want high-conviction signals
_STRONG_STATES = {"STRONG_LONG", "STRONG_SHORT"}
_NORMAL_AND_STRONG_STATES = {"STRONG_LONG", "LONG", "STRONG_SHORT", "SHORT"}


@dataclass
class PendingPrediction:
    """
    एक "signal fired, now waiting to check if it was right" record.
    हर actionable signal पर horizons per एक pending बनती है।
    """
    symbol: str
    state: str                # SignalState.value (e.g. "STRONG_LONG")
    score: float              # smoothed_score at fire time
    evidence: float           # evidence_strength at fire time
    price_at_signal: float    # executable entry fill (ask LONG / bid SHORT)
    ltp_at_signal: float
    best_bid_at_signal: float
    best_ask_at_signal: float
    ts_fired: float           # timestamp when signal fired (from tick)
    horizon_seconds: float    # कब evaluate करना है (fire + horizon)


@dataclass
class PredictionStatBucket:
    """Per-(state, horizon) aggregated accuracy stats. Numeric-only, small."""
    count:            int   = 0
    hits:             int   = 0     # directional_return > 0
    net_hits:         int   = 0     # directional_return > cost (actually profitable)
    sum_return:       float = 0.0   # sum of directional returns
    sum_return_sq:    float = 0.0   # for std / Sharpe calculation
    sum_net_return:   float = 0.0   # after transaction costs
    max_win:          float = 0.0   # best single prediction (directional)
    max_loss:         float = 0.0   # worst single prediction (directional)
    last_return:      float = 0.0   # most recent for trending display

    def add(self, directional_return: float, cost: float) -> None:
        """directional_return: positive if signal was correct direction."""
        net = directional_return - cost
        self.count += 1
        if directional_return > 0:
            self.hits += 1
        if net > 0:
            self.net_hits += 1
        self.sum_return += directional_return
        self.sum_return_sq += directional_return * directional_return
        self.sum_net_return += net
        if directional_return > self.max_win:
            self.max_win = directional_return
        if directional_return < self.max_loss:
            self.max_loss = directional_return
        self.last_return = directional_return

    def hit_rate(self) -> float:
        return self.hits / self.count if self.count else 0.0

    def net_hit_rate(self) -> float:
        return self.net_hits / self.count if self.count else 0.0

    def avg_return(self) -> float:
        return self.sum_return / self.count if self.count else 0.0

    def avg_net_return(self) -> float:
        return self.sum_net_return / self.count if self.count else 0.0

    def sharpe_proxy(self) -> float:
        """Rough Sharpe-like ratio (avg_return / std_return). Not annualized."""
        if self.count < 2:
            return 0.0
        avg = self.avg_return()
        var = (self.sum_return_sq / self.count) - (avg * avg)
        if var <= 0:
            return 0.0
        return avg / (var ** 0.5)


class PredictionTracker:
    """
    Signal accuracy self-measurement.

    काम कैसे करता है:
      1. जब actionable signal fire हो (LONG/SHORT states), current price capture
         और N horizons पर pending predictions create।
      2. उसी symbol के अगले ticks पर, jab bhi pending की horizon expire हो जाए,
         current price से difference निकालो — actual return.
      3. Signal-direction में move हुई तो 'hit', नहीं तो 'miss'.
      4. Cost deduct करके 'net edge' — actual money-making capacity.
      5. Sab kuchh JSONL में log होता है (audit trail), और live stats UI में दिखते हैं।

    यह real-time honest feedback देता है — "क्या हमारे signals actually काम करते हैं"।
    """

    def __init__(
        self,
        horizons_s: List[float],
        transaction_cost_pct: float,
        log_path: str,
        max_pending_age_s: float = 300.0,
    ):
        self.horizons = sorted(float(h) for h in horizons_s)
        self.execution_model = ExecutionCostModel(
            transaction_cost_pct=float(transaction_cost_pct),
            latency_slippage_bps=0.0,
        )
        self.cost = self.execution_model.transaction_cost_pct
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_pending_age = float(max_pending_age_s)

        # Pending per symbol — worker thread only, no lock needed for writes
        self._pending: Dict[str, Deque[PendingPrediction]] = {}

        # Stats per (state, horizon). Read by UI thread → protect with lock.
        self._stats: Dict[Tuple[str, float], PredictionStatBucket] = {}
        self._stats_lock = threading.RLock()

        # Ring buffer of recent completed evaluations (for potential future analytics)
        self._recent_completed: Deque[Dict[str, Any]] = deque(maxlen=2000)

        # JSONL log file
        self._log_file = None
        self._log_lock = threading.Lock()

        # Counters (single-writer from worker)
        self.signals_recorded: int = 0
        self.predictions_evaluated: int = 0
        self.predictions_timed_out: int = 0

    # -- Lifecycle --

    def open(self) -> None:
        self._log_file = open(self.log_path, "a", encoding="utf-8", buffering=1)
        logger.info("PredictionTracker opened. Log: %s (horizons=%s, cost=%.4f%%)",
                    self.log_path, self.horizons, self.cost * 100)

    def close(self) -> None:
        with self._log_lock:
            if self._log_file is not None:
                self._log_file.close()
                self._log_file = None

    # -- Recording (called from worker thread after signal fires) --

    def record_signal(
        self, symbol: str, state: str, score: float, evidence: float,
        price: float, ts: float,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
    ) -> None:
        """Actionable signal पर executable entry से pending बनाओ।"""
        if state not in _ACTIONABLE_STATES:
            return
        if price <= 0:
            return
        side = "SHORT" if state in _SHORT_STATES else "LONG"
        entry_price = self.execution_model.fill_price(
            side, True, price, best_bid, best_ask,
        )
        bucket = self._pending.setdefault(symbol, deque())
        for h in self.horizons:
            bucket.append(PendingPrediction(
                symbol=symbol, state=state, score=score, evidence=evidence,
                price_at_signal=entry_price,
                ltp_at_signal=price,
                best_bid_at_signal=float(best_bid or 0.0),
                best_ask_at_signal=float(best_ask or 0.0),
                ts_fired=ts, horizon_seconds=h,
            ))
        self.signals_recorded += 1

    # -- Evaluation (called from worker thread on every tick) --

    def on_tick(
        self, symbol: str, current_price: float, current_ts: float,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
    ) -> None:
        """
        Symbol के pending predictions में से जो horizon पार कर चुके हैं उन्हें
        evaluate करो। बाकियों को छोड़ दो।
        """
        pending = self._pending.get(symbol)
        if not pending or current_price <= 0:
            return

        # Deque को front से scan करते हैं (FIFO order of insertion).
        # Ready-to-evaluate predictions collect करके एक बार में rebuild करते हैं।
        # यह approach O(N) है per tick per symbol, N आमतौर पर छोटा (3-30)।
        ready_indices: List[int] = []
        for i, pred in enumerate(pending):
            age = current_ts - pred.ts_fired
            if age >= pred.horizon_seconds:
                # Horizon पूरा हुआ — evaluate
                self._evaluate(
                    pred, current_price, current_ts, timed_out=False,
                    best_bid=best_bid, best_ask=best_ask,
                )
                ready_indices.append(i)
            elif age > self.max_pending_age:
                # बहुत ज्यादा purani — force close (feed lag या symbol stopped)
                self._evaluate(
                    pred, current_price, current_ts, timed_out=True,
                    best_bid=best_bid, best_ask=best_ask,
                )
                ready_indices.append(i)

        if not ready_indices:
            return

        # Rebuild deque without the removed indices
        remove_set = set(ready_indices)
        new_pending = deque(p for i, p in enumerate(pending) if i not in remove_set)
        if new_pending:
            self._pending[symbol] = new_pending
        else:
            del self._pending[symbol]

    def _evaluate(
        self, pred: PendingPrediction, current_price: float,
        current_ts: float, timed_out: bool,
        best_bid: Optional[float] = None,
        best_ask: Optional[float] = None,
    ) -> None:
        """Evaluate one prediction at the executable exit quote."""
        side = "SHORT" if pred.state in _SHORT_STATES else "LONG"
        exit_price = self.execution_model.fill_price(
            side, False, current_price, best_bid, best_ask,
        )
        directional_return, charge_return, net_return = self.execution_model.evaluate(
            side, pred.price_at_signal, exit_price,
        )
        raw_return = (exit_price - pred.price_at_signal) / pred.price_at_signal

        # Update in-memory stats
        bucket_key = (pred.state, pred.horizon_seconds)
        with self._stats_lock:
            bucket = self._stats.setdefault(bucket_key, PredictionStatBucket())
            bucket.add(directional_return, charge_return)

        self.predictions_evaluated += 1
        if timed_out:
            self.predictions_timed_out += 1

        # Log to JSONL (audit trail)
        payload = {
            "ts_fired":         round(pred.ts_fired, 3),
            "ts_evaluated":     round(current_ts, 3),
            "actual_horizon_s": round(current_ts - pred.ts_fired, 3),
            "target_horizon_s": pred.horizon_seconds,
            "symbol":           pred.symbol,
            "state":            pred.state,
            "score":            round(pred.score, 3),
            "evidence":         round(pred.evidence, 2),
            "price_at_signal":  round(pred.price_at_signal, 4),
            "ltp_at_signal":    round(pred.ltp_at_signal, 4),
            "bid_at_signal":    round(pred.best_bid_at_signal, 4),
            "ask_at_signal":    round(pred.best_ask_at_signal, 4),
            "price_at_horizon": round(exit_price, 4),
            "ltp_at_horizon":   round(current_price, 4),
            "bid_at_horizon":   round(float(best_bid or 0.0), 4),
            "ask_at_horizon":   round(float(best_ask or 0.0), 4),
            "raw_return_pct":       round(raw_return * 100, 4),
            "directional_return_pct": round(directional_return * 100, 4),
            "charges_pct":          round(charge_return * 100, 4),
            "net_return_pct":       round(net_return * 100, 4),
            "is_hit":               directional_return > 0,
            "is_net_profitable":    net_return > 0,
            "timed_out":            timed_out,
        }
        self._recent_completed.append(payload)
        if self._log_file is not None:
            with self._log_lock:
                try:
                    self._log_file.write(
                        json.dumps(payload, separators=(",", ":"), default=str) + "\n"
                    )
                except Exception as e:
                    logger.exception("Prediction log write failed: %s", e)

    # -- Read API (UI thread) --

    def get_stats_snapshot(self) -> Dict[Tuple[str, float], PredictionStatBucket]:
        """Return an immutable snapshot for UI. Read-only for callers."""
        with self._stats_lock:
            # Return shallow copies of buckets so UI can read without lock contention
            return {
                key: PredictionStatBucket(
                    count=b.count, hits=b.hits, net_hits=b.net_hits,
                    sum_return=b.sum_return, sum_return_sq=b.sum_return_sq,
                    sum_net_return=b.sum_net_return,
                    max_win=b.max_win, max_loss=b.max_loss,
                    last_return=b.last_return,
                )
                for key, b in self._stats.items()
            }

    def pending_count(self) -> int:
        """Total pending predictions across all symbols (best-effort read)."""
        return sum(len(v) for v in self._pending.values())

    def summary(self) -> str:
        """One-line aggregate summary for headless / logging."""
        with self._stats_lock:
            total = sum(b.count for b in self._stats.values())
            if total == 0:
                return "predictions=0"
            hits = sum(b.hits for b in self._stats.values())
            net_hits = sum(b.net_hits for b in self._stats.values())
            net_return = sum(b.sum_net_return for b in self._stats.values()) / total
            return (f"predictions={total} "
                    f"hit_rate={hits/total*100:.1f}% "
                    f"net_hit_rate={net_hits/total*100:.1f}% "
                    f"avg_net={net_return*100:+.3f}%")


# ---------------------------------------------------------------------------
# 4b. Signal Quality Gates (optional, non-invasive)
#
# ये दो utility classes signals को filter करने में मदद करती हैं।
# दोनों STANDALONE हैं — signal generation में कुछ नहीं बदलता। सिर्फ
# analyzers/executors इन्हें call करके decide करते हैं कि signal को
# accept करना है या block।
#
#   SessionStateManager  →  NSE market phase tracker (IST-fixed)
#                           avoid lunch/closing/pre-open signals
#
#   RVOLCalculator       →  Relative volume vs rolling 20-min avg
#                           trust signals only when volume elevated
#
# दोनों अगर None हैं तो कोई filter नहीं होगा (backward-compat)।
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


# NSE IST timezone (fixed, doesn't depend on system TZ setting)
_IST = timezone(timedelta(hours=5, minutes=30))

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
        dt = datetime.fromtimestamp(ts, tz=_IST)
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
            dt = datetime.fromtimestamp(ts, tz=_IST)
            if dt.weekday() < 5 and dt.time() >= self.no_new_entry_after:
                return False, f"after_no_entry_cutoff({self.no_new_entry_after})"

        return True, ""

    def seconds_to_close(self, ts: float, close_time: dt_time = dt_time(15, 30)) -> float:
        """
        Seconds remaining until market close (15:30 IST by default) on the
        current trading day. Returns 0.0 if market already closed.
        """
        dt = datetime.fromtimestamp(ts, tz=_IST)
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


# ---------------------------------------------------------------------------
# 6. Scanner statistics
# ---------------------------------------------------------------------------

@dataclass
class ScannerStats:
    started_at: float = field(default_factory=time.time)
    ticks_received: int = 0                # WS ने भेजे
    ticks_dropped: int = 0                 # malformed / unknown token
    ticks_dropped_backpressure: int = 0    # queue full → drop (RED FLAG)
    signals_computed: int = 0
    signals_logged: int = 0
    errors: int = 0

    # Queue observability (0 = idle, >5000 = falling behind)
    queue_depth_current: int = 0
    queue_depth_max: int = 0

    # End-to-end tick processing latency (parse → engine → signal → log)
    # Rolling window of last N samples for percentile stats.
    _latency_samples_us: Deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    latency_max_us: float = 0.0

    def ticks_per_sec(self) -> float:
        elapsed = max(time.time() - self.started_at, 1.0)
        return self.ticks_received / elapsed

    def record_latency(self, us: float) -> None:
        self._latency_samples_us.append(us)
        if us > self.latency_max_us:
            self.latency_max_us = us

    def latency_stats_us(self) -> Tuple[float, float, float]:
        """Return (avg, p50, p99) in microseconds. (0,0,0) if no samples."""
        n = len(self._latency_samples_us)
        if n == 0:
            return (0.0, 0.0, 0.0)
        sorted_samples = sorted(self._latency_samples_us)
        avg = sum(sorted_samples) / n
        p50 = sorted_samples[n // 2]
        p99 = sorted_samples[min(n - 1, int(n * 0.99))]
        return (avg, p50, p99)


# ---------------------------------------------------------------------------
# 7. Symbol wrapper — per-symbol state
# ---------------------------------------------------------------------------

@dataclass
class SymbolTracker:
    symbol: str
    token: int
    engine: BookDynamicsEngine
    last_result: Optional[SignalResult] = None
    last_state: SignalState = SignalState.NEUTRAL
    last_ltp: float = 0.0
    tick_count: int = 0
    last_update_ts: float = 0.0


# ---------------------------------------------------------------------------
# 8. Scanner core — the brain
# ---------------------------------------------------------------------------

class Scanner:
    """
    Multi-symbol scanner। हर symbol का अपना BookDynamicsEngine instance है।
    Ticks आते ही सही engine को route करता है, signals collect करता है,
    और ranked top-N दिखाने के लिए API देता है।
    """

    def __init__(self, config: ScannerConfig):
        self.config = config
        self._trackers: Dict[int, SymbolTracker] = {}   # token → tracker
        self._lock = threading.RLock()
        self._stats = ScannerStats()
        self._recorder = SignalRecorder(config.signal_log_path)
        self._dedup: Dict[Tuple[str, SignalState], float] = {}
        self._last_error_log_ts: float = 0.0
        self.actionable_states = set(config.log_signal_states)

        # Producer-consumer queue: WS thread → worker thread
        # यह decoupling 1-core CPU पर scanner को hang होने से बचाता है।
        # WS callback ~5µs में enqueue करता है, worker background में process करता है।
        self._tick_queue: _queue_mod.Queue = _queue_mod.Queue(maxsize=config.tick_queue_size)
        self._shutdown_evt = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None

        # Phase 1 — Prediction Tracker (signal accuracy self-validation).
        # Note: attribute name deliberately verbose to avoid shadowing with
        # the SymbolTracker instance local (`tracker`) inside _process_tick.
        self._prediction_tracker = PredictionTracker(
            horizons_s=config.prediction_horizons_s,
            transaction_cost_pct=config.transaction_cost_pct,
            log_path=config.prediction_log_path,
            max_pending_age_s=config.prediction_max_pending_age_s,
        )

    def start(self):
        self._recorder.open()
        self._prediction_tracker.open()
        # Start processing worker thread (single worker sufficient for GIL-bound Python)
        self._shutdown_evt.clear()
        self._worker_thread = threading.Thread(
            target=self._worker_loop, name="tick-worker", daemon=True
        )
        self._worker_thread.start()
        logger.info("Scanner started. Log: %s (worker=%s)",
                    self.config.signal_log_path, self._worker_thread.name)

    def stop(self):
        # Signal worker to stop
        self._shutdown_evt.set()
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=5.0)
            if self._worker_thread.is_alive():
                logger.warning("Worker thread did not exit within 5s")
        self._recorder.close()
        self._prediction_tracker.close()
        logger.info("Scanner stopped. Queue remaining: %d ticks",
                    self._tick_queue.qsize())

    # -- Symbol registration --
    def register_symbols(self, resolved: Dict[str, int]) -> None:
        with self._lock:
            for symbol, token in resolved.items():
                engine = BookDynamicsEngine(config=self.config.engine_config)
                self._trackers[token] = SymbolTracker(
                    symbol=symbol, token=token, engine=engine,
                )
        logger.info("%d symbol engines initialized.", len(resolved))

    # -- Tick handler (called from WS thread — lightweight enqueue only) --
    def on_tick(self, msg: Dict[str, Any]) -> None:
        """
        FAST PATH — WebSocket thread कभी नहीं रुकता।
        बस queue में डालो और तुरंत वापस readable बन जाओ।

        Time budget: ~5 µs per tick.
        Backpressure: queue full हो जाए तो drop करता है (stats में counter)।
        """
        self._stats.ticks_received += 1
        try:
            self._tick_queue.put_nowait(msg)
            # Track queue depth watermark
            depth = self._tick_queue.qsize()
            self._stats.queue_depth_current = depth
            if depth > self._stats.queue_depth_max:
                self._stats.queue_depth_max = depth
        except _queue_mod.Full:
            # Backpressure — worker fall रहा है behind
            self._stats.ticks_dropped_backpressure += 1
            # Rate-limited warning log (max once per 5s)
            now = time.time()
            if now - self._last_error_log_ts > 5.0:
                logger.warning(
                    "Tick queue full (size=%d). Dropping ticks. "
                    "Consider larger tick_queue_size or faster CPU.",
                    self.config.tick_queue_size,
                )
                self._last_error_log_ts = now

    # -- Worker loop (processing thread) --
    def _worker_loop(self) -> None:
        """
        Background thread — queue से ticks pull करके actual processing करती है।
        WS thread कभी नहीं रुकती इस वजह से।
        """
        logger.info("Tick worker started.")
        while not self._shutdown_evt.is_set():
            try:
                msg = self._tick_queue.get(timeout=0.5)
            except _queue_mod.Empty:
                continue
            self._process_tick(msg)
            # Update queue depth (post-consume)
            self._stats.queue_depth_current = self._tick_queue.qsize()
        # Drain remaining queue on shutdown (up to 1 second)
        drain_deadline = time.time() + 1.0
        while time.time() < drain_deadline and not self._tick_queue.empty():
            try:
                msg = self._tick_queue.get_nowait()
                self._process_tick(msg)
            except _queue_mod.Empty:
                break
        logger.info("Tick worker exited.")

    # -- Actual processing (called from worker thread) --
    def _process_tick(self, msg: Dict[str, Any]) -> None:
        """
        Full tick processing pipeline. यह slow path है — worker thread पर चलता है
        ताकि WS thread block न हो।

        Pipeline:
          1. Parse (broker payload → MarketSnapshot)
          2. Route to correct symbol's engine
          3. engine.update() — 17 microstructure metrics compute
          4. Signal state decide + optionally log
        End-to-end latency measured in microseconds (see stats).
        """
        t_start = time.perf_counter()
        try:
            token_raw = msg.get("token")
            if token_raw is None:
                self._stats.ticks_dropped += 1
                return
            token = int(token_raw)
            tracker = self._trackers.get(token)
            if tracker is None:
                self._stats.ticks_dropped += 1
                return

            snapshot = AngelOneWSAdapter.parse(msg, tracker.symbol)
            if snapshot is None:
                self._stats.ticks_dropped += 1
                return

            # ---- INTEGRATION POINT 1: Evaluate pending predictions ----
            # हर incoming tick पर pehle check karo ki is symbol ke koi pending
            # prediction (past signal) matured हुआ या नहीं। Current tick की LTP
            # ही "future price" है उन signals के लिए जो पहले fire हुए थे।
            self._prediction_tracker.on_tick(
                tracker.symbol, snapshot.ltp, snapshot.timestamp,
                best_bid=snapshot.best_bid, best_ask=snapshot.best_ask,
            )

            result = tracker.engine.update(snapshot)
            tracker.tick_count += 1
            tracker.last_ltp = snapshot.ltp
            tracker.last_update_ts = snapshot.timestamp

            if result is None:
                return

            self._stats.signals_computed += 1
            tracker.last_result = result
            state_changed = (result.state != tracker.last_state)
            tracker.last_state = result.state

            # -- Decide whether to record --
            state_name = result.state.value
            if state_name not in self.actionable_states:
                return
            if result.evidence_strength < self.config.min_evidence_strength_to_log:
                return

            key = (tracker.symbol, result.state)
            now = time.time()
            last_ts = self._dedup.get(key, 0.0)
            if not state_changed and (now - last_ts) < self.config.signal_dedup_seconds:
                return
            self._dedup[key] = now

            self._recorder.record(tracker.symbol, result)
            self._stats.signals_logged += 1

            # ---- INTEGRATION POINT 2: Record new prediction ----
            # Signal fire होते ही, current LTP capture karke pending prediction
            # बना दो। Aage aane wale ticks पर horizon expire होते ही evaluate होगा।
            self._prediction_tracker.record_signal(
                symbol=tracker.symbol,
                state=state_name,
                score=result.smoothed_score,
                evidence=result.evidence_strength,
                price=snapshot.ltp,
                ts=snapshot.timestamp,
                best_bid=snapshot.best_bid,
                best_ask=snapshot.best_ask,
            )

        except Exception as e:
            self._stats.errors += 1
            now = time.time()
            if now - self._last_error_log_ts > 5.0:
                logger.exception("on_tick error: %s", e)
                self._last_error_log_ts = now
        finally:
            elapsed_us = (time.perf_counter() - t_start) * 1_000_000.0
            self._stats.record_latency(elapsed_us)

    # -- Snapshot for UI --
    def snapshot_for_ui(self) -> Tuple[List[SymbolTracker], List[SymbolTracker], ScannerStats]:
        """Return (top bullish, top bearish, stats) sorted by evidence."""
        with self._lock:
            trackers = list(self._trackers.values())

        bullish: List[SymbolTracker] = []
        bearish: List[SymbolTracker] = []
        for t in trackers:
            r = t.last_result
            if r is None:
                continue
            if r.state.value in ("STRONG_LONG", "LONG", "WEAK_LONG"):
                bullish.append(t)
            elif r.state.value in ("STRONG_SHORT", "SHORT", "WEAK_SHORT"):
                bearish.append(t)

        # Sort by evidence descending
        bullish.sort(key=lambda t: -(t.last_result.evidence_strength if t.last_result else 0))
        bearish.sort(key=lambda t: -(t.last_result.evidence_strength if t.last_result else 0))

        n = self.config.top_n_display
        return bullish[:n], bearish[:n], self._stats


# ---------------------------------------------------------------------------
# 9. Console UI — rich Live table
# ---------------------------------------------------------------------------

class ConsoleUI:
    """
    Live-updating three-panel console UI:
      - Top: statistics header
      - Middle: top bullish signals table
      - Bottom: top bearish signals table
    """

    STATE_STYLE = {
        "STRONG_LONG":  "bold green",
        "LONG":         "green",
        "WEAK_LONG":    "dim green",
        "STRONG_SHORT": "bold red",
        "SHORT":        "red",
        "WEAK_SHORT":   "dim red",
        "NEUTRAL":      "dim white",
        "SUPPRESSED":   "yellow",
    }

    def __init__(self, scanner: Scanner, refresh_ms: int = 500):
        if not RICH_AVAILABLE:
            raise ImportError("rich library चाहिए। चलाएँ: pip install rich")
        self.scanner = scanner
        self.refresh_hz = max(1, min(20, 1000 // refresh_ms))
        self.console = Console()
        self._shutdown = threading.Event()

    def _build_stats_header(self, stats: ScannerStats, n_symbols: int) -> Panel:
        elapsed = int(time.time() - stats.started_at)
        h, rem = divmod(elapsed, 3600)
        m, s = divmod(rem, 60)
        tps = stats.ticks_per_sec()
        avg_us, p50_us, p99_us = stats.latency_stats_us()

        bp_style = "red" if stats.ticks_dropped_backpressure > 0 else "dim"
        line1 = Text.assemble(
            ("NSE Book Dynamics Scanner", "bold cyan"),
            ("   |   ", "dim"),
            (f"Symbols: {n_symbols}", "white"),
            ("   |   ", "dim"),
            (f"Uptime: {h:02d}:{m:02d}:{s:02d}", "white"),
            ("   |   ", "dim"),
            (f"Ticks: {stats.ticks_received:,} ", "white"),
            (f"({tps:.1f}/s)", "dim"),
            ("   |   ", "dim"),
            (f"Signals: {stats.signals_computed:,}", "white"),
            ("   |   ", "dim"),
            (f"Logged: {stats.signals_logged:,}", "green"),
            ("   |   ", "dim"),
            (f"Backpressure: {stats.ticks_dropped_backpressure:,}", bp_style),
        )
        line2 = Text.assemble(
            ("Tick→Signal Latency", "bold magenta"),
            ("  ", "dim"),
            (f"avg {avg_us:.0f}µs", "white"),
            ("  ·  ", "dim"),
            (f"p50 {p50_us:.0f}µs", "white"),
            ("  ·  ", "dim"),
            (f"p99 {p99_us:.0f}µs", "white"),
            ("  ·  ", "dim"),
            (f"max {stats.latency_max_us:.0f}µs", "yellow"),
            ("   |   ", "dim"),
            ("Queue depth", "bold magenta"),
            ("  ", "dim"),
            (f"{stats.queue_depth_current}/{stats.queue_depth_max} peak", "white"),
        )
        combined = Text.assemble(line1, "\n", line2)
        return Panel(Align.center(combined), border_style="cyan")

    def _build_side_table(self, trackers: List[SymbolTracker], title: str, is_bull: bool) -> Table:
        table = Table(
            title=title,
            title_style="bold green" if is_bull else "bold red",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("Symbol", style="cyan", width=14)
        table.add_column("State", width=13)
        table.add_column("Score", justify="right", width=8)
        table.add_column("Evidence", justify="right", width=9)
        table.add_column("LTP", justify="right", width=10)
        table.add_column("OBI L1", justify="right", width=8)
        table.add_column("OBI Wt", justify="right", width=8)
        table.add_column("Spoof", justify="right", width=6)
        table.add_column("Iceberg", justify="right", width=7)
        table.add_column("Age", justify="right", width=5)

        now = time.time()
        for t in trackers:
            r = t.last_result
            if r is None:
                continue
            m = r.metrics
            state_style = self.STATE_STYLE.get(r.state.value, "white")
            age_s = int(now - t.last_update_ts) if t.last_update_ts else 0
            table.add_row(
                t.symbol,
                Text(r.state.value, style=state_style),
                f"{r.smoothed_score:+.2f}",
                f"{r.evidence_strength:.1f}",
                f"{t.last_ltp:.2f}" if t.last_ltp else "-",
                f"{m.l1_imbalance:+.2f}",
                f"{m.weighted_depth_imbalance:+.2f}",
                f"{m.spoofing_suspicion:.2f}" if m.spoofing_suspicion > 0.05 else "-",
                f"{m.iceberg_suspicion:.2f}" if m.iceberg_suspicion > 0.05 else "-",
                f"{age_s}s",
            )
        if not trackers:
            table.add_row("(no signals yet)", "", "", "", "", "", "", "", "", "")
        return table

    def _build_prediction_panel(self) -> Table:
        """
        4वाँ panel — signal accuracy self-validation output।
        हर actionable state के लिए display horizon (default 60s) पर hit rate,
        avg return, net edge (after costs), और verdict दिखाता है।
        """
        cfg = self.scanner.config
        horizon = cfg.prediction_display_horizon_s
        min_samples = cfg.prediction_min_samples_for_verdict
        cost_pct = cfg.transaction_cost_pct * 100.0
        stats_snap = self.scanner._prediction_tracker.get_stats_snapshot()
        pending_count = self.scanner._prediction_tracker.pending_count()
        signals_recorded = self.scanner._prediction_tracker.signals_recorded
        evaluated = self.scanner._prediction_tracker.predictions_evaluated

        title = (
            f"📈  Prediction Accuracy @ {int(horizon)}s horizon   "
            f"(cost model: −{cost_pct:.2f}% round-trip  ·  "
            f"pending={pending_count}  ·  evaluated={evaluated:,})"
        )
        table = Table(
            title=title,
            title_style="bold magenta",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("Signal State",  style="cyan",  width=14)
        table.add_column("Samples",        justify="right", width=8)
        table.add_column("Hit Rate",       justify="right", width=9)
        table.add_column("Net Hit %",      justify="right", width=10)
        table.add_column("Avg Return",     justify="right", width=11)
        table.add_column("Net Edge",       justify="right", width=10)
        table.add_column("Best / Worst",   justify="right", width=17)
        table.add_column("Verdict",        width=20)

        # Fixed row order — LONG side top, SHORT side bottom
        display_states = [
            ("STRONG_LONG",  "bold green"),
            ("LONG",         "green"),
            ("WEAK_LONG",    "dim green"),
            ("WEAK_SHORT",   "dim red"),
            ("SHORT",        "red"),
            ("STRONG_SHORT", "bold red"),
        ]

        any_data = False
        for state, style in display_states:
            bucket = stats_snap.get((state, horizon))
            if bucket is None or bucket.count == 0:
                table.add_row(
                    Text(state, style=style),
                    "0", "—", "—", "—", "—", "—", Text("waiting…", style="dim"),
                )
                continue
            any_data = True
            hit_rate = bucket.hit_rate() * 100.0
            net_hit_rate = bucket.net_hit_rate() * 100.0
            avg_ret_pct = bucket.avg_return() * 100.0
            net_edge_pct = bucket.avg_net_return() * 100.0
            best = bucket.max_win * 100.0
            worst = bucket.max_loss * 100.0

            # -- Verdict logic --
            if bucket.count < min_samples:
                remaining = min_samples - bucket.count
                verdict_text = f"need {remaining} more"
                verdict_style = "dim"
            elif net_edge_pct > 0.03:
                verdict_text = "✓ EDGE (profitable)"
                verdict_style = "bold green"
            elif net_edge_pct > 0.0:
                verdict_text = "~ marginal"
                verdict_style = "yellow"
            elif net_edge_pct > -0.03:
                verdict_text = "✗ break-even"
                verdict_style = "dim red"
            else:
                verdict_text = "✗ noise (loss)"
                verdict_style = "red"

            # Color the numeric cells based on sign
            avg_ret_style = "green" if avg_ret_pct > 0 else "red"
            net_edge_style = "bold green" if net_edge_pct > 0.03 else \
                             "yellow" if net_edge_pct > 0 else \
                             "red"

            table.add_row(
                Text(state, style=style),
                f"{bucket.count:,}",
                f"{hit_rate:.1f}%",
                f"{net_hit_rate:.1f}%",
                Text(f"{avg_ret_pct:+.3f}%", style=avg_ret_style),
                Text(f"{net_edge_pct:+.3f}%", style=net_edge_style),
                f"{best:+.2f}% / {worst:+.2f}%",
                Text(verdict_text, style=verdict_style),
            )

        if not any_data:
            table.caption = (
                f"Waiting for {int(horizon)}s to pass after first actionable signal…  "
                f"({signals_recorded} signals recorded so far)"
            )
            table.caption_style = "dim"

        return table

    def _render(self) -> Layout:
        bullish, bearish, stats = self.scanner.snapshot_for_ui()
        n_symbols = len(self.scanner._trackers)  # noqa: safe read

        layout = Layout()
        # 4-section layout: header (fixed 4) + bull/bear/predictions (ratio-based)
        layout.split_column(
            Layout(name="header", size=4),
            Layout(name="bullish",     ratio=2),
            Layout(name="bearish",     ratio=2),
            Layout(name="predictions", ratio=2),
        )
        layout["header"].update(self._build_stats_header(stats, n_symbols))
        layout["bullish"].update(self._build_side_table(
            bullish, f"🟢  Top {self.scanner.config.top_n_display} Bullish", is_bull=True,
        ))
        layout["bearish"].update(self._build_side_table(
            bearish, f"🔴  Top {self.scanner.config.top_n_display} Bearish", is_bull=False,
        ))
        layout["predictions"].update(self._build_prediction_panel())
        return layout

    def run(self):
        with Live(self._render(), console=self.console, refresh_per_second=self.refresh_hz,
                  screen=False) as live:
            while not self._shutdown.is_set():
                time.sleep(1.0 / self.refresh_hz)
                live.update(self._render())

    def stop(self):
        self._shutdown.set()


# ---------------------------------------------------------------------------
# 10. Simulation mode — fake tick generator (test without Angel One)
# ---------------------------------------------------------------------------

class SimulatedFeed:
    """
    Realistic-ish fake tick generator for testing scanner end-to-end.

    Each symbol:
      - Random-walk mid price
      - Random regime shifts (bullish/bearish/neutral)
      - Occasional spoof injections (~2%)
      - Level-2 top-5 depth generated around mid

    NOT for validating strategy performance — only for testing the pipeline.
    """

    def __init__(
        self,
        symbols: List[str],
        on_tick: Callable[[Dict[str, Any]], None],
        ticks_per_symbol_per_sec: float = 1.0,
    ):
        self.symbols = symbols
        self.on_tick = on_tick
        self.rate = ticks_per_symbol_per_sec
        self._shutdown = threading.Event()
        self._threads: List[threading.Thread] = []

        # Per-symbol state
        self._state: Dict[str, Dict[str, Any]] = {}
        rng = random.Random(42)
        for i, s in enumerate(symbols):
            self._state[s] = {
                "token": 100000 + i,   # fake token
                "price": rng.uniform(100.0, 5000.0),
                "volume": 0,
                "regime": rng.choice(["bull", "bear", "neutral", "neutral"]),
                "regime_ticks_left": rng.randint(20, 200),
                "rng": random.Random(i),
                "tbq": rng.randint(30000, 80000),
                "tsq": rng.randint(30000, 80000),
            }

    def _generate_tick(self, symbol: str, state: Dict[str, Any]) -> Dict[str, Any]:
        rng: random.Random = state["rng"]

        # Regime rotation
        state["regime_ticks_left"] -= 1
        if state["regime_ticks_left"] <= 0:
            state["regime"] = rng.choice(["bull", "bear", "neutral", "neutral"])
            state["regime_ticks_left"] = rng.randint(20, 200)

        # Price random walk with regime drift
        if state["regime"] == "bull":
            drift = rng.uniform(0.0, 0.0008)
        elif state["regime"] == "bear":
            drift = rng.uniform(-0.0008, 0.0)
        else:
            drift = rng.uniform(-0.0002, 0.0002)
        vol = rng.uniform(0.0003, 0.001)
        state["price"] *= (1.0 + drift + rng.gauss(0, vol))
        price = max(state["price"], 1.0)

        # Volume tick
        trade_size = rng.randint(1, 500)
        state["volume"] += trade_size

        # TBQ/TSQ drift according to regime
        if state["regime"] == "bull":
            state["tbq"] += rng.randint(-2000, 5000)
            state["tsq"] += rng.randint(-4000, 2000)
        elif state["regime"] == "bear":
            state["tbq"] += rng.randint(-4000, 2000)
            state["tsq"] += rng.randint(-2000, 5000)
        else:
            state["tbq"] += rng.randint(-3000, 3000)
            state["tsq"] += rng.randint(-3000, 3000)
        state["tbq"] = max(10000, state["tbq"])
        state["tsq"] = max(10000, state["tsq"])

        # Top-5 depth around mid
        tick = 0.05
        mid = price
        best_bid = round(mid - tick, 2)
        best_ask = round(mid + tick, 2)

        # Base level qty influenced by regime
        base = 200
        if state["regime"] == "bull":
            bid_base = int(base * rng.uniform(1.5, 3.0))
            ask_base = int(base * rng.uniform(0.5, 1.2))
        elif state["regime"] == "bear":
            bid_base = int(base * rng.uniform(0.5, 1.2))
            ask_base = int(base * rng.uniform(1.5, 3.0))
        else:
            bid_base = int(base * rng.uniform(0.8, 1.5))
            ask_base = int(base * rng.uniform(0.8, 1.5))

        # 2% spoof injection
        spoof = rng.random() < 0.02

        bids_data = []
        for lvl in range(5):
            p = round(best_bid - lvl * tick, 2)
            q = int(bid_base * rng.uniform(0.8, 1.4))
            if spoof and lvl == 0:
                q *= 20   # huge phantom bid
            bids_data.append({"price": int(p * 100), "quantity": q,
                              "no of orders": rng.randint(1, 10), "flag": 0})

        asks_data = []
        for lvl in range(5):
            p = round(best_ask + lvl * tick, 2)
            q = int(ask_base * rng.uniform(0.8, 1.4))
            asks_data.append({"price": int(p * 100), "quantity": q,
                              "no of orders": rng.randint(1, 10), "flag": 0})

        return {
            "token": str(state["token"]),
            "exchange_type": NSE_CM_EXCHANGE_TYPE,
            "subscription_mode": SUBSCRIPTION_MODE_SNAP_QUOTE,
            "last_traded_price": int(price * 100),
            "last_traded_quantity": trade_size,
            "volume_trade_for_the_day": state["volume"],
            "total_buy_quantity": state["tbq"],
            "total_sell_quantity": state["tsq"],
            "best_5_buy_data": bids_data,
            "best_5_sell_data": asks_data,
            "exchange_timestamp": int(time.time() * 1000),
            "upper_circuit_limit": int(price * 110),
            "lower_circuit_limit": int(price * 90),
        }

    def _worker(self, symbols_slice: List[str]):
        """
        हर symbol को exactly `rate` ticks/sec देता है।
        Round timing: एक full round of len(symbols_slice) symbols में
        1/rate second लगते हैं।
        """
        round_interval = 1.0 / self.rate  # seconds between ticks per symbol
        while not self._shutdown.is_set():
            round_start = time.perf_counter()
            for s in symbols_slice:
                if self._shutdown.is_set():
                    break
                state = self._state[s]
                tick = self._generate_tick(s, state)
                try:
                    self.on_tick(tick)
                except Exception as e:
                    logger.exception("Sim on_tick error: %s", e)
            # Sleep only if we finished the round faster than target rate
            elapsed = time.perf_counter() - round_start
            slack = round_interval - elapsed
            if slack > 0:
                time.sleep(slack)

    def start(self, n_workers: int = 4):
        # Fake symbol/token map for the scanner
        chunk_size = max(1, len(self.symbols) // n_workers)
        for i in range(0, len(self.symbols), chunk_size):
            slice_ = self.symbols[i:i + chunk_size]
            t = threading.Thread(target=self._worker, args=(slice_,),
                                 name=f"sim-{i}", daemon=True)
            self._threads.append(t)
            t.start()
        logger.info("Simulated feed started with %d workers.", len(self._threads))

    def stop(self):
        self._shutdown.set()

    def token_map(self) -> Dict[str, int]:
        return {s: st["token"] for s, st in self._state.items()}


# ---------------------------------------------------------------------------
# 11. Main entrypoint
# ---------------------------------------------------------------------------

def _scanner_parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="NSE Real-Time Intraday Scanner (Book Dynamics)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 nse_scanner.py --mode simulate\n"
            "  python3 nse_scanner.py --mode simulate --sim-rate 20   # 20 tps per symbol\n"
            "  python3 nse_scanner.py --mode live --config config.json\n"
        ),
    )
    p.add_argument("--mode", choices=["live", "simulate"], default="simulate",
                   help="live = Angel One WebSocket; simulate = fake ticks (default)")
    p.add_argument("--config", default="config.json",
                   help="Path to config.json (default: ./config.json)")
    p.add_argument("--no-ui", action="store_true",
                   help="Headless mode (no rich UI, useful for daemons)")
    p.add_argument("--sim-rate", type=float, default=5.0,
                   help="Simulation ticks/sec per symbol (default: 5, "
                        "realistic NSE mid-liquid). Max recommended: 50.")
    p.add_argument("--sim-workers", type=int, default=4,
                   help="Simulation worker threads (default: 4)")
    return p._scanner_parse_args()


def check_market_hours() -> bool:
    """Rough IST market-hours check (9:15 AM - 3:30 PM Mon-Fri). Advisory only."""
    now = time.localtime()
    if now.tm_wday >= 5:   # Sat, Sun
        return False
    minutes = now.tm_hour * 60 + now.tm_min
    return (9 * 60 + 15) <= minutes <= (15 * 60 + 30)


# ---------------------------------------------------------------------------
# Unified CLI entrypoint
# ---------------------------------------------------------------------------

def main() -> int:
    import argparse

    p = argparse.ArgumentParser(
        description="NSE Book Dynamics Real-Time Scanner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 nse_book_scanner.py --demo\n"
            "  python3 nse_book_scanner.py --mode simulate\n"
            "  python3 nse_book_scanner.py --mode simulate --sim-rate 30\n"
            "  python3 nse_book_scanner.py --mode live --config config.json\n"
        ),
    )
    p.add_argument("--demo", action="store_true",
                   help="Engine का 8-scenario self-test चलाएँ (कोई config नहीं चाहिए)")
    p.add_argument("--mode", choices=["live", "simulate"], default="simulate",
                   help="live = Angel One WebSocket; simulate = fake ticks (default)")
    p.add_argument("--config", default="config.json",
                   help="Path to config.json (default: ./config.json)")
    p.add_argument("--no-ui", action="store_true",
                   help="Headless mode (no rich UI)")
    p.add_argument("--sim-rate", type=float, default=5.0,
                   help="Sim ticks/sec per symbol (default: 5)")
    p.add_argument("--sim-workers", type=int, default=4,
                   help="Sim worker threads (default: 4)")
    args = p.parse_args()

    if args.demo:
        _engine_demo()
        return 0

    # Delegate to scanner main with the same args
    return _scanner_main_wrapper(args)


def _print_prediction_breakdown(scanner) -> None:
    """Headless mode के लिए per-state accuracy breakdown print करता है (हर 30s पर)."""
    cfg = scanner.config
    horizon = cfg.prediction_display_horizon_s
    min_samples = cfg.prediction_min_samples_for_verdict
    stats_snap = scanner._prediction_tracker.get_stats_snapshot()

    display_states = ["STRONG_LONG", "LONG", "WEAK_LONG",
                      "WEAK_SHORT", "SHORT", "STRONG_SHORT"]

    # Any data yet?
    has_any = any(
        (state, horizon) in stats_snap and stats_snap[(state, horizon)].count > 0
        for state in display_states
    )
    if not has_any:
        return

    print(f"\n  ── Prediction Accuracy @ {int(horizon)}s "
          f"(cost −{cfg.transaction_cost_pct*100:.2f}%) ──")
    header = (f"    {'State':<14} {'N':>6}  {'Hit%':>6}  "
              f"{'AvgRet':>9}  {'NetEdge':>9}  Verdict")
    print(header)
    print("    " + "-" * (len(header) - 4))
    for state in display_states:
        b = stats_snap.get((state, horizon))
        if b is None or b.count == 0:
            continue
        hit_pct = b.hit_rate() * 100.0
        avg_ret = b.avg_return() * 100.0
        net_edge = b.avg_net_return() * 100.0

        if b.count < min_samples:
            verdict = f"need {min_samples - b.count} more"
        elif net_edge > 0.03:
            verdict = "✓ EDGE"
        elif net_edge > 0.0:
            verdict = "~ marginal"
        elif net_edge > -0.03:
            verdict = "✗ break-even"
        else:
            verdict = "✗ noise"
        print(f"    {state:<14} {b.count:>6}  {hit_pct:>5.1f}%  "
              f"{avg_ret:>+8.3f}%  {net_edge:>+8.3f}%  {verdict}")
    print()


def _scanner_main_wrapper(args):
    """Same body as the old _scanner_main() but accepting pre-parsed args."""
    import sys, threading, time, signal as _signal_mod
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"\n❌ Config error: {e}\n", file=sys.stderr)
        return 2

    setup_logging(config)
    logger.info("=" * 70)
    logger.info("NSE Scanner starting (mode=%s, symbols=%d)", args.mode, len(config.symbols))
    logger.info("=" * 70)

    scanner = Scanner(config)
    scanner.start()

    sim = None
    connector = None
    try:
        if args.mode == "live":
            if not SMARTAPI_AVAILABLE:
                print("\n❌ Live mode के लिए smartapi-python install करें:"
                      "\n    pip install -r requirements.txt\n", file=sys.stderr)
                return 3
            if not check_market_hours():
                logger.warning("Currently market hours (Mon-Fri 9:15-15:30 IST) के बाहर हैं।")
            connector = AngelOneConnector(config)
            connector.login()
            connector.load_scrip_master()
            resolved, missing = connector.resolve_tokens()
            if not resolved:
                logger.error("कोई भी symbol resolve नहीं हुआ।")
                return 4
            scanner.register_symbols(resolved)
            connector.start_websocket(list(resolved.values()), scanner.on_tick)
        else:
            logger.info("Simulation: %.1f tps × %d symbols = %.0f tps aggregate",
                        args.sim_rate, len(config.symbols),
                        args.sim_rate * len(config.symbols))
            sim = SimulatedFeed(config.symbols, scanner.on_tick,
                                ticks_per_symbol_per_sec=args.sim_rate)
            scanner.register_symbols(sim.token_map())
            sim.start(n_workers=args.sim_workers)

        stop_event = threading.Event()
        def _handler(signum, frame):
            logger.info("Shutdown signal received.")
            stop_event.set()
        _signal_mod.signal(_signal_mod.SIGINT, _handler)
        _signal_mod.signal(_signal_mod.SIGTERM, _handler)

        if args.no_ui or not RICH_AVAILABLE:
            if not RICH_AVAILABLE:
                logger.warning("rich library missing — headless mode. pip install rich")
            print("\nScanner running headless. Ctrl+C to stop.")
            print(f"Signals log: {config.signal_log_path}")
            print(f"System log:  {config.system_log_path}\n")
            while not stop_event.is_set():
                time.sleep(2.0)
                s = scanner._stats
                avg_us, p50_us, p99_us = s.latency_stats_us()
                bp = s.ticks_dropped_backpressure
                bp_str = f" ⚠ BACKPRESSURE-DROP={bp:,}" if bp > 0 else ""
                pt = scanner._prediction_tracker
                pt_summary = pt.summary()
                pt_pending = pt.pending_count()
                print(f"  ticks={s.ticks_received:,} ({s.ticks_per_sec():.0f}/s) "
                      f"signals={s.signals_computed:,} "
                      f"logged={s.signals_logged:,} "
                      f"errors={s.errors}{bp_str}  |  "
                      f"queue={s.queue_depth_current}/{s.queue_depth_max} "
                      f"latency p50={p50_us:.0f}µs p99={p99_us:.0f}µs  |  "
                      f"{pt_summary} (pending={pt_pending})",
                      flush=True)

                # -- Periodic per-state breakdown every 30s --
                if not hasattr(_scanner_main_wrapper, "_last_pt_breakdown"):
                    _scanner_main_wrapper._last_pt_breakdown = time.time()
                if time.time() - _scanner_main_wrapper._last_pt_breakdown > 30.0:
                    _scanner_main_wrapper._last_pt_breakdown = time.time()
                    _print_prediction_breakdown(scanner)
        else:
            ui = ConsoleUI(scanner, refresh_ms=config.ui_refresh_ms)
            ui_thread = threading.Thread(target=ui.run, name="ui-thread", daemon=True)
            ui_thread.start()
            while not stop_event.is_set():
                time.sleep(0.5)
            ui.stop()
            ui_thread.join(timeout=2.0)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt — shutting down.")
    except Exception as e:
        logger.exception("Fatal: %s", e)
        return 1
    finally:
        logger.info("Cleanup...")
        if sim is not None:
            sim.stop()
        if connector is not None:
            connector.stop()
        scanner.stop()
        logger.info("Bye.")

    return 0


if __name__ == "__main__":
    import sys as _sys
    _sys.exit(main())
