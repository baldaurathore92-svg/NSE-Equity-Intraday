#!/usr/bin/env python3
"""
live_hit_rate_analyzer.py
=========================
Real-Time Hit Rate Analyzer — Virtual Trade Tracking on LIVE Angel One Data.

PURPOSE
-------
यह script सिर्फ एक काम करता है — और अच्छे से करता है:
    "Real NSE market पर scanner के signals का actual hit rate measure करना,
     बिना कोई real order भेजे।"

WHAT IT DOES
------------
1. Connects to Angel One SmartAPI WebSocket (LIVE real NSE data)
2. Runs BookDynamicsEngine per symbol on every incoming tick
3. Every actionable signal fire पर current LTP capture करता है (virtual entry)
4. Waits at multiple horizons: 5s, 15s, 30s, 60s, 120s, 300s
5. उन horizons पर check करता है — signal-direction में price बढ़ी या नहीं?
6. Comprehensive hit rate breakdown:
   • By signal state (STRONG_LONG / LONG / WEAK_LONG / SHORT / STRONG_SHORT)
   • By horizon (कौन-सा holding time best है?)
   • By evidence bucket (30-50 / 50-70 / 70+)
   • By market regime (Phase 2)
   • By hour of day (opening / mid / closing)
   • By symbol (कौन-सा stock scanner पर सबसे predictable है?)
7. Cost-adjusted net edge (0.06% round-trip default)
8. Real-time console dashboard (rich UI)
9. EOD comprehensive report with HONEST verdict

⚠ NO REAL ORDERS PLACED. Pure measurement tool. Zero financial risk.

USAGE
-----
Basic (rich UI, 60-min session):
    python3 live_hit_rate_analyzer.py --config config.json

Full trading day (no UI, log to file):
    python3 live_hit_rate_analyzer.py --config config.json \\
        --duration-hours 6.5 --no-ui

Symbol subset:
    python3 live_hit_rate_analyzer.py --config config.json \\
        --symbols RELIANCE-EQ,TCS-EQ,HDFCBANK-EQ

Custom horizons (in seconds):
    python3 live_hit_rate_analyzer.py --config config.json \\
        --horizons 10,30,60,180,600

VPS deployment (recommended):
    tmux new -s hitrate
    cd ~/nse_scanner && source venv/bin/activate
    python3 live_hit_rate_analyzer.py --config config.json --duration-hours 6.5
    # Ctrl+B, D to detach

OUTPUT
------
Terminal        : Real-time dashboard (or periodic headless updates)
logs/hit_rate_predictions.jsonl    : Every evaluated prediction (audit trail)
logs/hit_rate_summary.txt           : End-of-day comprehensive report
"""

from __future__ import annotations

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
from typing import Any, Deque, Dict, List, Optional, Tuple

# Reuse from main scanner (single dependency)
from nse_book_scanner import (
    BookDynamicsEngine, DepthLevel, EngineConfig,
    MarketSnapshot, SignalResult, SignalState,
    _LONG_STATES, _SHORT_STATES, _ACTIONABLE_STATES,
    AngelOneConnector, AngelOneWSAdapter, ScannerConfig,
    load_config, setup_logging, SMARTAPI_AVAILABLE,
)

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
IST = timezone(timedelta(hours=5, minutes=30))

# NSE market hours
MARKET_OPEN_MINUTES = 9 * 60 + 15    # 09:15 IST
MARKET_CLOSE_MINUTES = 15 * 60 + 30  # 15:30 IST


# ============================================================
# 1. DATA CLASSES
# ============================================================

@dataclass
class PendingPrediction:
    """A signal captured at fire time, awaiting horizon evaluation."""
    symbol: str
    state: str
    smoothed_score: float
    evidence: float
    regime_label: str
    price_at_signal: float
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

    # Terminal state
    is_closed: bool = False
    close_reason: str = ""                   # "max_horizon" / "timeout"

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
        cost_pct: float,
        max_age_s: float = 600.0,
    ):
        self.horizons = sorted(float(h) for h in horizons_s)
        self.max_horizon = max(self.horizons) if self.horizons else 60.0
        self.cost = cost_pct
        self.max_age_s = max(max_age_s, self.max_horizon + 30.0)

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
        self.total_timeout_closed = 0

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

    def on_tick(self, symbol: str, current_price: float, current_ts: float) -> List[LiveSignal]:
        """
        Update all open signals for `symbol`. Returns list of signals that
        closed on this tick (max horizon reached).
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

                # Compute directional return
                raw = (current_price - sig.price_at_signal) / sig.price_at_signal
                directional = -raw if sig.state in _SHORT_STATES else raw
                elapsed = current_ts - sig.ts_fired

                sig.current_price = current_price
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

                # Close conditions
                if elapsed >= sig.max_horizon_s:
                    sig.is_closed = True
                    sig.close_reason = "max_horizon"
                    del self._open[sig_id]
                    self.recently_closed.append(sig)
                    newly_closed.append(sig)
                    self.total_closed += 1
                    self.total_max_horizon_closed += 1
                elif elapsed > self.max_age_s:
                    sig.is_closed = True
                    sig.close_reason = "timeout"
                    del self._open[sig_id]
                    self.recently_closed.append(sig)
                    newly_closed.append(sig)
                    self.total_closed += 1
                    self.total_timeout_closed += 1
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
                if r > self.cost:
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

    def excursion_stats(self) -> Dict[str, Dict[str, Any]]:
        """Aggregate MFE/MAE stats by state — useful for TP/SL calibration."""
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
            times_mfe = [s.time_to_mfe_s for s in sigs]
            times_mae = [s.time_to_mae_s for s in sigs]
            final_rets = [s.current_directional_return for s in sigs]
            result[state] = {
                "n": n,
                "avg_mfe_pct": (sum(mfes) / n) * 100,
                "avg_mae_pct": (sum(maes) / n) * 100,
                "avg_time_to_mfe_s": sum(times_mfe) / n,
                "avg_time_to_mae_s": sum(times_mae) / n,
                "avg_final_return_pct": (sum(final_rets) / n) * 100,
                "median_mfe_pct": sorted(mfes)[n // 2] * 100,
                "median_mae_pct": sorted(maes)[n // 2] * 100,
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
        log_path: str = "logs/hit_rate_predictions.jsonl",
        max_pending_age_s: float = 600.0,
        min_samples_for_verdict: int = 20,
        signal_dedup_seconds: float = 5.0,
    ):
        self.horizons = sorted(float(h) for h in horizons_s)
        self.cost = transaction_cost_pct
        self.max_pending_age = max_pending_age_s
        self.min_samples = min_samples_for_verdict
        self.signal_dedup_seconds = signal_dedup_seconds

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

        # Real-time live signal monitor (per-signal, for live UI)
        self.live_monitor = LiveSignalMonitor(
            horizons_s=self.horizons,
            cost_pct=self.cost,
            max_age_s=max_pending_age_s,
        )

        # Multi-dimensional stats buckets
        self._stats_lock = threading.RLock()
        self._stats_state_horizon: Dict[Tuple[str, float], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_evidence: Dict[Tuple[str, str], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_regime: Dict[Tuple[str, str], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_hour: Dict[Tuple[str, int], HitRateBucket] = defaultdict(HitRateBucket)
        self._stats_symbol: Dict[Tuple[str, str], HitRateBucket] = defaultdict(HitRateBucket)

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

    def record_signal(
        self, symbol: str, result: SignalResult, price: float, ts: float,
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
        if state not in _ACTIONABLE_STATES or price <= 0:
            return

        # -- DEDUP GATE --
        prev_state = self._dedup_last_state.get(symbol)
        state_changed = (prev_state != state)
        last_recorded_ts = self._dedup_last_ts.get((symbol, state), 0.0)
        time_since_last = ts - last_recorded_ts

        if not state_changed and time_since_last < self.signal_dedup_seconds:
            self.signals_deduped += 1
            return

        # Update dedup memory (record this event)
        self._dedup_last_ts[(symbol, state)] = ts
        self._dedup_last_state[symbol] = state

        # Hour of day in IST
        try:
            hour = datetime.fromtimestamp(ts, tz=IST).hour
        except (ValueError, OSError):
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
                    price_at_signal=price,
                    ts_fired=ts,
                    horizon_seconds=h,
                    hour_of_day=hour,
                ))

        # Also add to live monitor (per-signal, for real-time UI)
        self.live_monitor.add_signal(
            symbol=symbol,
            state=state,
            smoothed_score=result.smoothed_score,
            evidence=result.evidence_strength,
            regime_label=regime_label,
            price=price,
            ts=ts,
        )

        self.signals_recorded += 1

    def on_tick(self, symbol: str, current_price: float, current_ts: float) -> None:
        """
        Called on every tick. Two things happen:
          1. LiveSignalMonitor.on_tick — updates real-time state of ALL open
             signals for this symbol (current P&L, MFE, MAE, horizon snapshots)
          2. Evaluate any pending predictions whose horizons have expired
             → statistical stats update
        """
        # Real-time update (fast, always run)
        self.live_monitor.on_tick(symbol, current_price, current_ts)

        # Horizon-based statistical evaluation (with lock — UI thread also reads pending_count)
        with self._pending_lock:
            pending = self._pending.get(symbol)
            if not pending or current_price <= 0:
                return

            remaining: Deque[PendingPrediction] = deque()
            for pred in pending:
                age = current_ts - pred.ts_fired
                if age >= pred.horizon_seconds:
                    self._evaluate(pred, current_price, current_ts, timed_out=False)
                elif age > self.max_pending_age:
                    self._evaluate(pred, current_price, current_ts, timed_out=True)
                else:
                    remaining.append(pred)

            if remaining:
                self._pending[symbol] = remaining
            else:
                del self._pending[symbol]

    def _evaluate(
        self, pred: PendingPrediction, current_price: float,
        current_ts: float, timed_out: bool,
    ) -> None:
        """Evaluate a matured pending prediction."""
        raw_return = (current_price - pred.price_at_signal) / pred.price_at_signal
        # Sign-flip for SHORT so directional_return > 0 always means "correct"
        directional = -raw_return if pred.state in _SHORT_STATES else raw_return

        evidence_bucket = self._evidence_bucket(pred.evidence)

        with self._stats_lock:
            self._stats_state_horizon[(pred.state, pred.horizon_seconds)].add(directional, self.cost)
            self._stats_evidence[(pred.state, evidence_bucket)].add(directional, self.cost)
            self._stats_regime[(pred.state, pred.regime_label)].add(directional, self.cost)
            self._stats_hour[(pred.state, pred.hour_of_day)].add(directional, self.cost)
            self._stats_symbol[(pred.state, pred.symbol)].add(directional, self.cost)

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
                "price_at_horizon": round(current_price, 4),
                "raw_return_pct":       round(raw_return * 100, 4),
                "directional_return_pct": round(directional * 100, 4),
                "net_return_pct":       round((directional - self.cost) * 100, 4),
                "is_hit":               directional > 0,
                "is_net_profitable":    (directional - self.cost) > 0,
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

    def __init__(self, config: ScannerConfig, analyzer: HitRateAnalyzer):
        if not SMARTAPI_AVAILABLE:
            raise ImportError(
                "smartapi-python not installed. Run:\n"
                "    pip install -r requirements.txt"
            )
        self.config = config
        self.analyzer = analyzer
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

        # Shutdown
        self._shutdown_event = threading.Event()

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

            token_raw = msg.get("token")
            if token_raw is None:
                self.total_ticks_dropped += 1
                return
            token = int(token_raw)
            symbol = self.token_to_symbol.get(token)
            if symbol is None:
                self.total_ticks_dropped += 1
                return

            snap = AngelOneWSAdapter.parse(msg, symbol)
            if snap is None:
                self.total_ticks_dropped += 1
                return

            self.symbols_seen.add(symbol)
            self.last_prices[symbol] = snap.ltp

            # STEP 1: Evaluate any pending predictions for this symbol
            self.analyzer.on_tick(symbol, snap.ltp, snap.timestamp)

            # STEP 2: Engine update to get new signal
            engine = self.engines.get(symbol)
            if engine is None:
                engine = BookDynamicsEngine(config=EngineConfig())
                self.engines[symbol] = engine
            result = engine.update(snap)
            if result is None:
                return

            self.total_signals_computed += 1
            state = result.state.value
            self.signal_state_counts[state] += 1
            self.regime_counts[result.metrics.regime.label] += 1

            # STEP 3: Record actionable signals for hit rate tracking
            if state in _ACTIONABLE_STATES:
                self.analyzer.record_signal(symbol, result, snap.ltp, snap.timestamp)

        except Exception as e:
            logger.exception("_on_tick error: %s", e)
        finally:
            elapsed_us = (time.perf_counter() - t_start) * 1_000_000
            self._latency_samples.append(elapsed_us)
            if elapsed_us > self._latency_max:
                self._latency_max = elapsed_us

    def start(self) -> None:
        self.analyzer.open()
        self.started_at = time.time()
        tokens = list(self.token_to_symbol.keys())
        logger.info("Starting WebSocket for %d tokens…", len(tokens))
        self.connector.start_websocket(tokens, self._on_tick)

    def stop(self) -> None:
        try:
            self.connector.stop()
        except Exception:
            pass
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
            (f"Cost: -{self.analyzer.cost*100:.2f}%", "dim"),
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
            table.caption = "No open signals yet — waiting for scanner to fire…"
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
        table.add_column("Net Profit %", justify="right", width=13)
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
            print(f"             {status} {sig.symbol:<14} {sig.state:<12} "
                  f"score={sig.smoothed_score:+.1f}  age={age_s:>3}s  "
                  f"entry={sig.price_at_signal:>8.2f} → now={sig.current_price:>8.2f}  "
                  f"dir_ret={dir_pct:+.3f}%  "
                  f"MFE={sig.max_favorable_excursion*100:+.2f}% "
                  f"MAE={sig.max_adverse_excursion*100:+.2f}%")


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
    lines.append(f"  Predictions evaluated: {analyzer.predictions_evaluated:,}")
    lines.append(f"  Pending (not yet expired): {analyzer.pending_count():,}")
    lines.append(f"  Cost model          : -{analyzer.cost*100:.4f}% round-trip")

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
    stats_sh = analyzer.snapshot_state_horizon()
    lines.append(f"  {'State':<14} {'Horizon':>8} {'N':>7} {'Hit %':>8} "
                 f"{'NetProfit %':>13} {'AvgRet %':>10} {'NetEdge %':>11} {'Verdict':<20}")
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
        lines.append(f"  {'State':<14} {'N':>6} {'Avg MFE %':>11} {'Med MFE %':>11} "
                     f"{'Avg MAE %':>11} {'Med MAE %':>11} {'t→MFE (s)':>10} "
                     f"{'Final Ret %':>13}")
        lines.append("  " + "-" * (W - 2))
        for state in ["STRONG_LONG", "LONG", "WEAK_LONG",
                      "WEAK_SHORT", "SHORT", "STRONG_SHORT"]:
            s = exc_stats.get(state)
            if s is None:
                continue
            lines.append(
                f"  {state:<14} {s['n']:>6} "
                f"{s['avg_mfe_pct']:>+10.3f}% {s['median_mfe_pct']:>+10.3f}% "
                f"{s['avg_mae_pct']:>+10.3f}% {s['median_mae_pct']:>+10.3f}% "
                f"{s['avg_time_to_mfe_s']:>9.1f} "
                f"{s['avg_final_return_pct']:>+12.3f}%"
            )
        lines.append("")
        lines.append("  💡 Calibration hint:")
        lines.append("     - Set TAKE_PROFIT ≈ median MFE (most trades reach it before decay)")
        lines.append("     - Set STOP_LOSS  ≈ median MAE × 0.7 (avoid stopping out too early)")
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
                   help="Round-trip transaction cost (default: 0.0006 = 0.06%%)")
    p.add_argument("--symbols", default=None,
                   help="Comma-separated symbol subset (default: all from config)")
    p.add_argument("--min-samples", type=int, default=20,
                   help="Minimum samples for confident verdict (default: 20)")
    p.add_argument("--dedup-seconds", type=float, default=5.0,
                   help="Signal dedup window in seconds (default: 5.0). "
                        "Same state fires within this window are ignored to "
                        "prevent memory/disk blowup during sustained signals.")
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
        log_path=args.log_path,
        min_samples_for_verdict=args.min_samples,
        signal_dedup_seconds=args.dedup_seconds,
    )

    # Build session
    session = LiveHitRateSession(config=config, analyzer=analyzer)
    try:
        session.prepare()
    except Exception as e:
        logger.exception("Angel One login/setup failed: %s", e)
        return 4

    # Shutdown handling
    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        logger.info("Signal %s received; stopping…", signum)
        stop_event.set()

    _signal_mod.signal(_signal_mod.SIGINT, _handle_signal)
    _signal_mod.signal(_signal_mod.SIGTERM, _handle_signal)

    # Start session
    logger.info("═" * 82)
    logger.info(" ✅ Starting live tracking. Ctrl+C to stop gracefully.")
    logger.info("═" * 82)
    try:
        session.start()
    except Exception as e:
        logger.exception("Session start failed: %s", e)
        return 5

    # Main loop
    max_duration_sec = args.duration_hours * 3600
    start_ts = time.time()
    last_headless_status = start_ts

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
            ui.stop()
            ui_thread.join(timeout=2.0)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt.")
    finally:
        logger.info("Stopping session…")
        session.stop()

    # EOD report
    report = generate_eod_report(session, analyzer)
    print()
    print(report)
    print()

    # Save report to file
    try:
        Path(args.report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report_path).write_text(report, encoding="utf-8")
        logger.info(f"EOD report saved: {args.report_path}")
    except Exception as e:
        logger.warning(f"Could not save report to {args.report_path}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
