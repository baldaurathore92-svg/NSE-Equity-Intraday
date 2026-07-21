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
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Tuple

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
      - `timestamp` monotonically non-decreasing (out-of-order dropped by engine)
    """
    timestamp: float                 # epoch seconds (sub-second precision preferred)
    symbol: str
    ltp: float                       # last traded price
    ltq: int                         # last traded qty (optional, 0 OK)
    volume_traded: int               # cumulative day volume (monotonically increasing)
    total_buy_qty: int               # exchange-broadcast aggregate BOOK-WIDE
    total_sell_qty: int              # exchange-broadcast aggregate BOOK-WIDE
    bids: List[DepthLevel]           # top-N (typically 5), best-first
    asks: List[DepthLevel]           # top-N (typically 5), best-first

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
    threshold_strong: float = 8.0
    threshold_normal: float = 5.0
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

        # Spoof: recent pull events awaiting execution confirmation
        self._pull_events: Deque[Dict[str, Any]] = deque(maxlen=500)

        # Aggressor (tick rule) rolling 5s accumulators
        self._buy_vol_5s   = TimeSeriesBuffer(5.0)
        self._sell_vol_5s  = TimeSeriesBuffer(5.0)
        self._last_agg_side: AggressorSide = AggressorSide.NA

        # Sequence guard
        self._last_ts: float = -1.0

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
            self._last_agg_side = AggressorSide.NA

    # ================================================================
    # Validation
    # ================================================================

    def _validate(self, snap: MarketSnapshot) -> bool:
        if snap.timestamp <= self._last_ts:
            logger.warning(
                "Out-of-order or duplicate snapshot ts=%.6f (last=%.6f); dropping",
                snap.timestamp, self._last_ts,
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

    # ---- Scenario 8: Duplicate & out-of-order snapshots ----
    eng = BookDynamicsEngine()
    for i in range(5):
        eng.update(_demo_snap(
            T0 + i, ltp=150.00, ltq=5, vol=2_000 + i * 20,
            tbq=30_000, tsq=30_000,
            bids=[(149.95, 200), (149.90, 200), (149.85, 200), (149.80, 200), (149.75, 200)],
            asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
        ))
    dup = eng.update(_demo_snap(
        T0 + 4, ltp=150.00, ltq=5, vol=2_200,   # duplicate ts
        tbq=30_000, tsq=30_000,
        bids=[(149.95, 200), (149.90, 200), (149.85, 200), (149.80, 200), (149.75, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    ))
    ooo = eng.update(_demo_snap(
        T0 + 3, ltp=150.00, ltq=5, vol=2_300,   # out-of-order
        tbq=30_000, tsq=30_000,
        bids=[(149.95, 200), (149.90, 200), (149.85, 200), (149.80, 200), (149.75, 200)],
        asks=[(150.05, 200), (150.10, 200), (150.15, 200), (150.20, 200), (150.25, 200)],
    ))
    print("\n===== SCENARIO 8: Duplicate & Out-of-Order Ticks =====")
    print(f"  duplicate snapshot result: {dup}  (expected: None)")
    print(f"  out-of-order snapshot result: {ooo}  (expected: None)")

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
# 4. Angel One WebSocket production adapter
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
        try:
            # Timestamp: Angel One typically sends `exchange_timestamp` or
            # `exchange_feed_time_epoch_ms` in milliseconds since epoch.
            ts_ms = (
                msg.get("exchange_timestamp")
                or msg.get("exchange_feed_time_epoch_ms")
                or msg.get("last_traded_timestamp")
            )
            if ts_ms:
                ts = float(ts_ms) / 1000.0
            else:
                ts = time.time()

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
                timestamp=ts,
                symbol=symbol,
                ltp=ltp,
                ltq=ltq,
                volume_traded=vol,
                total_buy_qty=tbq,
                total_sell_qty=tsq,
                bids=bids,
                asks=asks,
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

    def start(self):
        self._recorder.open()
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

    def _render(self) -> Layout:
        bullish, bearish, stats = self.scanner.snapshot_for_ui()
        n_symbols = len(self.scanner._trackers)  # noqa: safe read

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=4),
            Layout(name="bullish"),
            Layout(name="bearish"),
        )
        layout["header"].update(self._build_stats_header(stats, n_symbols))
        layout["bullish"].update(self._build_side_table(
            bullish, f"🟢  Top {self.scanner.config.top_n_display} Bullish", is_bull=True,
        ))
        layout["bearish"].update(self._build_side_table(
            bearish, f"🔴  Top {self.scanner.config.top_n_display} Bearish", is_bull=False,
        ))
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
                print(f"  ticks={s.ticks_received:,} ({s.ticks_per_sec():.0f}/s) "
                      f"signals={s.signals_computed:,} "
                      f"logged={s.signals_logged:,} "
                      f"errors={s.errors}{bp_str}  |  "
                      f"queue={s.queue_depth_current}/{s.queue_depth_max} "
                      f"latency avg={avg_us:.0f}µs p50={p50_us:.0f}µs "
                      f"p99={p99_us:.0f}µs max={s.latency_max_us:.0f}µs",
                      flush=True)
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
