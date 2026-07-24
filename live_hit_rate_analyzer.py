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
from typing import Any, Deque, Dict, FrozenSet, List, Optional, Set, Tuple

# Reuse from main scanner (single dependency)
from nse_book_scanner import (
    BookDynamicsEngine, DepthLevel, EngineConfig,
    MarketSnapshot, ExecutionCostModel, SignalResult, SignalState,
    _LONG_STATES, _SHORT_STATES, _ACTIONABLE_STATES,
    _STRONG_STATES, _NORMAL_AND_STRONG_STATES,
    AngelOneConnector, AngelOneWSAdapter, ScannerConfig,
    load_config, setup_logging, SMARTAPI_AVAILABLE,
    # Optional signal quality gates (new):
    SessionStateManager, SessionPhase, RVOLCalculator,
    DEFAULT_TRADEABLE_PHASES,
)
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
IST = timezone(timedelta(hours=5, minutes=30))

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

        self._first_tick_alerted = False
        self._parse_failure_alerted = False
        self._lock = threading.Lock()

    def record_message(self, symbol: Optional[str],
                       parse_success: bool, parse_reason: str = "") -> None:
        with self._lock:
            self.msgs_received += 1
            if parse_success and symbol:
                self.msgs_parsed_ok += 1
                if self.first_tick_time is None:
                    self.first_tick_time = time.time()
                    elapsed = self.first_tick_time - self.started_at
                    logger.info("✅ FIRST VALID TICK received after %.1fs from %s",
                                elapsed, symbol)
                self.symbols_with_data.add(symbol)
            else:
                self.msgs_parse_failed += 1
                if parse_reason:
                    self.parse_failure_reasons[parse_reason] += 1

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
        """Background thread — check data flow health, warn if broken."""
        while not self._shutdown_event.is_set():
            time.sleep(5.0)
            try:
                self.health.check_health_and_warn()
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

    # -- 15-SECOND RULES (Gemini "Sniper" policy) --
    p.add_argument("--entry-confirmation-sec", type=float, default=15.0,
                   help="Signal recorded only after score continuously "
                        "qualifies for N seconds (default 15.0, set 0 to "
                        "disable). Cancels + rearms on direction flip or "
                        "score falling below threshold.")
    p.add_argument("--entry-score", type=float, default=4.0,
                   help="Min |smoothed_score| a signal must maintain during "
                        "the confirmation window (default 4.0 = calibrated "
                        "STRONG threshold; the older '8.0' in some prompts "
                        "is unreachable in live NSE data).")
    p.add_argument("--entry-evidence", type=float, default=30.0,
                   help="Min evidence_strength during confirmation (default 30)")
    p.add_argument("--survival-check-sec", type=float, default=15.0,
                   help="One-shot check N seconds after entry; if MFE is "
                        "below --survival-min-favor-pct, the signal is "
                        "closed at that moment (default 15.0, set 0 to "
                        "disable).")
    p.add_argument("--survival-min-favor-pct", type=float, default=0.0001,
                   help="Minimum favorable MFE %% within the survival "
                        "window (default 0.0001 = 0.01%%). Below this, the "
                        "signal is squared off at the survival mark.")
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

    # Build session (with optional diagnostic dump)
    session = LiveHitRateSession(
        config=config, analyzer=analyzer,
        diagnose=args.diagnose,
        dump_count=args.dump_count,
        dump_path=args.dump_path,
        engine_config=engine_config,
    )

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

    return 0


if __name__ == "__main__":
    sys.exit(main())
