#!/usr/bin/env python3
"""
paper_trader.py
===============
Realistic NSE Paper Trading Harness — Full Backtesting Simulation

यह script का purpose:
  1. Scanner को realistic NSE market conditions पर test करना
  2. Actual trade execution simulate करना (slippage, costs सहित)
  3. PnL, drawdown, Sharpe, win-rate का complete analysis
  4. Real deployment से पहले strategy का honest evidence

Realistic Simulator vs Old Simulator
------------------------------------
पुराने simulator में artificial "regime persistence" थी (20-200 ticks तक
same bull/bear mood चलता था), जिससे scanner को false 80%+ hit rate मिल रहा था।

अब का simulator:
  ✓ Random walk with NO fake regime persistence
  ✓ Fat-tailed returns (real NSE volatility)
  ✓ Rare structural moves (2-5% of ticks, mimicking real news/institutional flow)
  ✓ Realistic bid-ask spread dynamics
  ✓ Time-of-day volatility profile (opening/closing spikes)

Usage:
    python3 paper_trader.py --duration-min 60 --symbols 20
    python3 paper_trader.py --duration-min 390 --symbols 100  # full trading day

Output:
  - Comprehensive terminal report
  - logs/paper_trades.jsonl (every trade with entry/exit details)
  - logs/paper_equity.csv (equity curve for plotting)
"""

from __future__ import annotations

import argparse
import bisect
import json
import logging
import math
import random
import signal as _signal_mod
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Deque, Dict, FrozenSet, List, Optional, Set, Tuple

# Import scanner components (same folder)
from nse_book_scanner import (
    BookDynamicsEngine, DepthLevel, EngineConfig,
    MarketSnapshot, SignalResult, SignalState,
    RegimeState, RegimeDetector,
    _LONG_STATES, _SHORT_STATES, _ACTIONABLE_STATES,
    _STRONG_STATES, _NORMAL_AND_STRONG_STATES,
    NSE_CM_EXCHANGE_TYPE, SUBSCRIPTION_MODE_SNAP_QUOTE,
    # For live mode:
    ScannerConfig, load_config, setup_logging,
    AngelOneWSAdapter, AngelOneConnector,
    SMARTAPI_AVAILABLE,
    # Optional signal quality gates:
    SessionStateManager, SessionPhase, RVOLCalculator,
    DEFAULT_TRADEABLE_PHASES,
)


logger = logging.getLogger("paper_trader")


# ============================================================
# 1. REALISTIC NSE MARKET SIMULATOR
# ============================================================

@dataclass
class SymbolState:
    """Per-symbol simulator state."""
    symbol: str
    token: int
    price: float               # current mid price
    initial_price: float       # for report anchor
    volume: int = 0            # cumulative day volume
    tbq: int = 40000
    tsq: int = 40000

    # Structural move state (rare directional bursts)
    burst_ticks_left: int = 0
    burst_direction: int = 0   # +1 = up, -1 = down, 0 = none
    burst_magnitude: float = 0.0

    # Volatility profile (annualized σ ≈ 25% for typical NSE stock)
    daily_vol: float = 0.019   # ~1.9% daily σ
    tick_vol: float = 0.0007   # per-tick σ (calibrated)


class RealisticNSEFeed:
    """
    Statistically realistic NSE tick generator.

    Key differences from the naive simulator:
      - NO persistent bull/bear regime (each tick's direction is
        independent unless a structural burst is active)
      - Fat-tailed returns (Student-t distribution to model kurtosis)
      - Rare structural moves (~3% of ticks trigger 10-40 tick bursts)
      - Realistic bid-ask spread (5-15 bps)
      - Volume varies with price move magnitude
      - Time-of-day intensity (opening/closing higher activity)
    """

    def __init__(
        self,
        symbols: List[str],
        on_tick_callback,
        total_duration_seconds: float,
        ticks_per_symbol_per_sec: float = 5.0,
        seed: int = 42,
    ):
        self.symbols = symbols
        self.on_tick = on_tick_callback
        self.total_duration = total_duration_seconds
        self.rate = ticks_per_symbol_per_sec
        self.rng = random.Random(seed)

        # Per-symbol state
        self.state: Dict[str, SymbolState] = {}
        for i, s in enumerate(symbols):
            # Give each symbol a realistic starting price (₹100 to ₹5000)
            price = self.rng.uniform(150, 4500)
            self.state[s] = SymbolState(
                symbol=s, token=100000 + i,
                price=price, initial_price=price,
                daily_vol=self.rng.uniform(0.015, 0.028),   # 1.5-2.8% daily σ
            )
            # Per-tick σ = daily_σ / sqrt(N_ticks_per_day)
            # For rate=5 tps × 6.5 hrs = 117,000 ticks/day → sqrt ≈ 342
            self.state[s].tick_vol = self.state[s].daily_vol / 342.0

        self.sim_start_ts = 1_700_000_000.0   # arbitrary epoch
        self.ticks_generated = 0

    def _fat_tailed_return(self, sigma: float) -> float:
        """
        Student-t distributed return with df=4 (fat tails, like real markets).
        Standard normal is thin-tailed and underestimates extreme moves.
        """
        # Simple fat-tail: mix normal + occasional large moves (5% probability)
        if self.rng.random() < 0.05:
            # Fat-tail event: 3-5σ move
            magnitude = sigma * self.rng.uniform(3, 5)
            sign = 1 if self.rng.random() < 0.5 else -1
            return magnitude * sign
        return self.rng.gauss(0, sigma)

    def _time_of_day_multiplier(self, elapsed_frac: float) -> float:
        """
        Realistic NSE volatility profile:
          - Opening 15 min: 2.0× (high volatility)
          - Mid-day: 0.7× (calm)
          - Closing 15 min: 1.5× (moderate spike)
        """
        # elapsed_frac: 0.0 = market open, 1.0 = market close
        if elapsed_frac < 0.04:            # first ~15 min of 6.5 hr day
            return 2.0
        elif elapsed_frac < 0.15:           # first hour
            return 1.3
        elif elapsed_frac > 0.96:           # last 15 min
            return 1.5
        elif elapsed_frac > 0.90:           # last 40 min
            return 1.2
        elif 0.30 < elapsed_frac < 0.60:    # mid-day calm
            return 0.7
        return 1.0

    def _maybe_start_burst(self, state: SymbolState) -> None:
        """~3% chance per tick to start a structural directional burst."""
        if state.burst_ticks_left > 0:
            return
        if self.rng.random() < 0.003:      # 0.3% chance per tick per symbol
            state.burst_ticks_left = self.rng.randint(10, 40)
            state.burst_direction = 1 if self.rng.random() < 0.5 else -1
            state.burst_magnitude = state.tick_vol * self.rng.uniform(2.0, 4.0)

    def _generate_tick(self, symbol: str, sim_ts: float,
                       vol_multiplier: float) -> Dict:
        state = self.state[symbol]

        # Maybe trigger a structural burst
        self._maybe_start_burst(state)

        # Compute price move
        base_return = self._fat_tailed_return(state.tick_vol * vol_multiplier)
        burst_return = 0.0
        if state.burst_ticks_left > 0:
            burst_return = state.burst_direction * state.burst_magnitude
            state.burst_ticks_left -= 1

        total_return = base_return + burst_return
        state.price *= (1.0 + total_return)
        state.price = max(state.price, 1.0)   # floor

        # Volume: correlated with |return| (larger moves = more trades)
        trade_size = self.rng.randint(1, 300) + int(abs(total_return) * 50000)
        state.volume += trade_size

        # TBQ/TSQ update — mean-reverting drift with small noise
        # (NO persistent bias, unlike old simulator)
        state.tbq += self.rng.randint(-2000, 2000)
        state.tsq += self.rng.randint(-2000, 2000)
        # If burst active, book slightly follows the burst
        if state.burst_ticks_left > 0:
            if state.burst_direction > 0:
                state.tbq += self.rng.randint(0, 500)
                state.tsq += self.rng.randint(-500, 0)
            else:
                state.tbq += self.rng.randint(-500, 0)
                state.tsq += self.rng.randint(0, 500)
        state.tbq = max(10000, state.tbq)
        state.tsq = max(10000, state.tsq)

        # Bid-ask spread: 5-15 bps (realistic for liquid NSE stocks)
        spread_bps = self.rng.uniform(5, 15)
        spread = state.price * spread_bps / 10000.0
        # NSE tick size is 0.05
        tick = 0.05
        best_bid = round((state.price - spread / 2) / tick) * tick
        best_ask = round((state.price + spread / 2) / tick) * tick
        if best_ask <= best_bid:
            best_ask = best_bid + tick

        # Top-5 depth — random quantities around a base, slightly biased by
        # burst direction (real market makers adjust to flow)
        base_qty = 200
        if state.burst_ticks_left > 0:
            if state.burst_direction > 0:
                bid_factor = self.rng.uniform(1.2, 2.0)
                ask_factor = self.rng.uniform(0.5, 1.0)
            else:
                bid_factor = self.rng.uniform(0.5, 1.0)
                ask_factor = self.rng.uniform(1.2, 2.0)
        else:
            bid_factor = self.rng.uniform(0.8, 1.3)
            ask_factor = self.rng.uniform(0.8, 1.3)

        bids_data = []
        for lvl in range(5):
            p = round((best_bid - lvl * tick) * 100)   # paise
            q = int(base_qty * bid_factor * self.rng.uniform(0.7, 1.3))
            bids_data.append({"price": p, "quantity": q, "no of orders": 3, "flag": 0})

        asks_data = []
        for lvl in range(5):
            p = round((best_ask + lvl * tick) * 100)   # paise
            q = int(base_qty * ask_factor * self.rng.uniform(0.7, 1.3))
            asks_data.append({"price": p, "quantity": q, "no of orders": 3, "flag": 0})

        return {
            "token": str(state.token),
            "exchange_type": NSE_CM_EXCHANGE_TYPE,
            "subscription_mode": SUBSCRIPTION_MODE_SNAP_QUOTE,
            "last_traded_price": int(state.price * 100),
            "last_traded_quantity": trade_size,
            "volume_trade_for_the_day": state.volume,
            "total_buy_quantity": state.tbq,
            "total_sell_quantity": state.tsq,
            "best_5_buy_data": bids_data,
            "best_5_sell_data": asks_data,
            "exchange_timestamp": int(sim_ts * 1000),   # ms
            "upper_circuit_limit": int(state.price * 110),
            "lower_circuit_limit": int(state.price * 90),
        }

    def run(self) -> None:
        """
        Generate ticks as fast as CPU allows.
        Sim timestamps advance realistically (1/rate seconds per tick per sym).
        """
        n_rounds = int(self.rate * self.total_duration)
        dt = 1.0 / self.rate

        real_start = time.perf_counter()
        for r in range(n_rounds):
            sim_ts = self.sim_start_ts + r * dt
            elapsed_frac = r / n_rounds
            vol_mult = self._time_of_day_multiplier(elapsed_frac)

            for symbol in self.symbols:
                tick = self._generate_tick(symbol, sim_ts, vol_mult)
                self.on_tick(tick)
                self.ticks_generated += 1

            # Progress every 10% of sim time
            if n_rounds >= 10 and r % max(1, n_rounds // 10) == 0:
                real_elapsed = time.perf_counter() - real_start
                sim_elapsed_min = r * dt / 60
                logger.info(
                    "  Sim progress: %.0f%% (%.1f sim-min in %.1f real-sec, "
                    "%.0f tps generated)",
                    elapsed_frac * 100, sim_elapsed_min, real_elapsed,
                    self.ticks_generated / max(real_elapsed, 0.01),
                )

    def token_map(self) -> Dict[str, int]:
        return {s: st.token for s, st in self.state.items()}


# ============================================================
# 1b. LIVE NSE FEED — Real Angel One WebSocket
# ============================================================

class LiveNSEFeed:
    """
    Bridge from Angel One SmartWebSocketV2 → same callback interface as
    RealisticNSEFeed. Runs during NSE market hours; each incoming WS message
    is forwarded to `on_tick_callback` in the same format the paper trader
    expects (dict with token, prices in paise, best_5_buy/sell_data etc).

    Prerequisite: valid Angel One credentials in config.json.
    """

    def __init__(
        self,
        scanner_config: ScannerConfig,
        on_tick_callback,
        max_duration_seconds: float,
    ):
        if not SMARTAPI_AVAILABLE:
            raise ImportError(
                "smartapi-python + pyotp not installed. Run:\n"
                "    pip install -r requirements.txt"
            )
        self.config = scanner_config
        self.on_tick = on_tick_callback
        self.max_duration = max_duration_seconds

        self.connector = AngelOneConnector(scanner_config)
        self.token_to_symbol: Dict[int, str] = {}
        self.symbol_to_token: Dict[str, int] = {}

        self.ticks_generated = 0
        self._shutdown = threading.Event()

    def prepare(self) -> Dict[str, int]:
        """Login, load scrip master, resolve symbols. Returns symbol→token map."""
        self.connector.login()
        self.connector.load_scrip_master()
        resolved, missing = self.connector.resolve_tokens()
        if not resolved:
            raise RuntimeError("No symbols resolved from Angel One scrip master.")
        self.symbol_to_token = resolved
        self.token_to_symbol = {t: s for s, t in resolved.items()}
        logger.info("LiveNSEFeed prepared: %d symbols resolved.", len(resolved))
        return resolved

    def token_map(self) -> Dict[str, int]:
        return self.symbol_to_token

    def _on_ws_message(self, msg: Dict) -> None:
        """
        WS message ka format Angel One-specific है (paise, exact field names).
        Yeh method paper trader ke expected format में convert करता है।
        SmartWebSocketV2 already dict देता है — बस forward कर देते हैं।
        """
        try:
            self.on_tick(msg)
            self.ticks_generated += 1
        except Exception as e:
            logger.exception("LiveNSEFeed on_tick error: %s", e)

    def run(self) -> None:
        """Blocking: connect WS, subscribe, wait max_duration."""
        tokens = list(self.symbol_to_token.values())
        logger.info("Starting live WebSocket for %d tokens, "
                    "max duration %.0f min...",
                    len(tokens), self.max_duration / 60.0)

        # Start WS in background thread
        self.connector.start_websocket(tokens, self._on_ws_message)

        # Wait up to max_duration OR until shutdown flag
        start = time.perf_counter()
        try:
            while not self._shutdown.is_set():
                elapsed = time.perf_counter() - start
                if elapsed >= self.max_duration:
                    logger.info("Max duration reached (%.0f min); stopping.",
                                self.max_duration / 60.0)
                    break
                # Progress update every 60s
                if int(elapsed) % 60 == 0 and elapsed > 0:
                    tps = self.ticks_generated / max(elapsed, 1)
                    logger.info("  Live progress: %.1f min elapsed, "
                                "%d ticks received, %.1f tps aggregate",
                                elapsed / 60.0, self.ticks_generated, tps)
                time.sleep(5.0)
        finally:
            self.connector.stop()

    def stop(self) -> None:
        self._shutdown.set()


# ============================================================
# 2. PAPER TRADE EXECUTION
# ============================================================

@dataclass
class Position:
    """An open paper-trade position."""
    trade_id: int
    symbol: str
    side: str                   # "LONG" or "SHORT"
    entry_price: float          # actual fill price after slippage
    entry_ts: float
    entry_ltp: float            # LTP at signal fire (before slippage)
    entry_state: str            # signal state at entry
    entry_score: float
    entry_evidence: float
    quantity: int               # shares
    stop_loss_price: float
    take_profit_price: float
    max_hold_ts: float          # entry_ts + max_hold_seconds


@dataclass
class ClosedTrade:
    """A completed round-trip trade."""
    trade_id: int
    symbol: str
    side: str
    entry_price: float
    entry_ts: float
    entry_state: str
    entry_score: float
    exit_price: float
    exit_ts: float
    exit_reason: str            # "signal_reverse" / "stop_loss" / "take_profit" / "max_hold" / "eod"
    quantity: int
    gross_pnl: float            # ₹ before costs
    cost: float                 # ₹ total round-trip cost
    net_pnl: float              # ₹ after costs
    return_pct: float           # net_pnl / (entry_price * quantity) × 100
    hold_seconds: float


class CooldownManager:
    """
    Whipsaw protection — enforces minimum wait after exit before re-entering
    same symbol.

    Two cooldown types:
      - Regular cooldown: after any exit, wait N seconds before entering same
        symbol (any direction).
      - Direction-flip cooldown: after exit, entering OPPOSITE direction
        requires flip_multiplier × cooldown_seconds (usually 2×).
        Reason: flipping direction right after exit is often noise.

    Post-stop-loss cooldown can be extended (stop_loss_multiplier) — signals
    right after a stop-out are especially unreliable (market showed us wrong).

    Thread-safe with RLock for shared use across HitRateAnalyzer + PaperExecutor
    in dual-analyzer mode.
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

        # Per-symbol last exit tracking
        self._last_exit_ts: Dict[str, float] = {}
        self._last_exit_side: Dict[str, str] = {}   # "LONG" or "SHORT"
        self._last_exit_reason: Dict[str, str] = {}

        # Stats
        self.total_exits_recorded = 0
        self.total_entries_blocked = 0
        self.blocks_by_reason: Dict[str, int] = defaultdict(int)

        self._lock = threading.RLock()

    def record_exit(self, symbol: str, side: str, reason: str, ts: float) -> None:
        """Called by executor when a position closes."""
        with self._lock:
            self._last_exit_ts[symbol] = ts
            self._last_exit_side[symbol] = side
            self._last_exit_reason[symbol] = reason
            self.total_exits_recorded += 1

    def can_enter(self, symbol: str, side: str, ts: float) -> Tuple[bool, str]:
        """
        Check if entering `side` on `symbol` at `ts` is allowed.
        Returns (allowed: bool, reason: str).

        `side` = "LONG" or "SHORT".
        """
        with self._lock:
            last_ts = self._last_exit_ts.get(symbol)
            if last_ts is None:
                return True, "no_prior_exit"

            elapsed = ts - last_ts
            last_side = self._last_exit_side.get(symbol, "")
            last_reason = self._last_exit_reason.get(symbol, "")

            # Determine required cooldown
            base = self.cooldown_seconds

            # Direction flip: opposite of last side
            if last_side and last_side != side:
                required = base * self.flip_multiplier
                if elapsed < required:
                    self.total_entries_blocked += 1
                    self.blocks_by_reason["direction_flip"] += 1
                    return False, (f"flip_cooldown ({elapsed:.0f}s < "
                                   f"{required:.0f}s, last exit was {last_side})")

            # Post stop-loss: longer cooldown
            if last_reason == "stop_loss":
                required = base * self.stop_loss_multiplier
                if elapsed < required:
                    self.total_entries_blocked += 1
                    self.blocks_by_reason["post_stop_loss"] += 1
                    return False, (f"post_sl_cooldown ({elapsed:.0f}s < "
                                   f"{required:.0f}s)")

            # Regular cooldown (same-direction or first exit reason category)
            if elapsed < base:
                self.total_entries_blocked += 1
                self.blocks_by_reason["regular"] += 1
                return False, f"cooldown ({elapsed:.0f}s < {base:.0f}s)"

        return True, "cooldown_expired"

    def stats(self) -> Dict[str, Any]:
        """Snapshot for reporting."""
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


class PaperExecutor:
    """
    Simulates order execution and position management.

    Trading rules:
      - Enter on STRONG signals (score ≥ +threshold or ≤ -threshold)
      - Position size: risk_per_trade × capital / stop_loss_distance
      - Slippage: entry at LTP × (1 + slippage_bps for LONG, - for SHORT)
      - Cooldown: after exit, N seconds before re-entering same symbol
        (2× for direction flip, 1.5× post stop-loss)
      - Exit reasons:
          1. Signal reverses (LONG position + SHORT signal fires) → exit
          2. Stop-loss hit → exit
          3. Take-profit hit → exit
          4. Max hold time elapsed → exit
          5. End of session → exit all
    """

    def __init__(
        self,
        capital: float = 100_000.0,
        risk_per_trade_pct: float = 0.01,
        max_concurrent_positions: int = 5,
        # ⚠ Calibrated to new engine thresholds (STRONG=4.0):
        #    - Score 4.0 is STRONG_LONG boundary → paper trader takes STRONG signals
        #    - Evidence 30 = achievable at score 3.0 with 100% agreement
        #    - Previously (5.0 / 40.0) required score ≥ 4 with 100% agreement,
        #      effectively unreachable given old engine threshold_strong=8.0
        entry_score_threshold: float = 4.0,
        entry_min_evidence: float = 30.0,
        slippage_bps: float = 10.0,          # 0.10% slippage
        cost_pct_round_trip: float = 0.0006,  # 0.06%
        stop_loss_pct: float = 0.0030,        # 0.30%
        take_profit_pct: float = 0.0080,      # 0.80% (favorable R:R = 2.67)
        max_hold_seconds: float = 300.0,      # 5 min
        # -- TIME STOP (Gemini's suggestion — scalp exit) --
        # After time_stop_seconds elapsed, if trade hasn't moved
        # time_stop_min_favor_pct% in our favor, EXIT immediately.
        # 0 = disabled. Recommended: 15s / 0.05% favor.
        # (Kills losers fast — cost saver in noise trades.)
        time_stop_seconds: float = 0.0,
        time_stop_min_favor_pct: float = 0.0005,   # 0.05% required favor
        trades_log_path: str = "logs/paper_trades.jsonl",
        equity_log_path: str = "logs/paper_equity.csv",
        # ---- Phase 2 regime-adaptive parameters ----
        regime_adaptive: bool = False,
        regime_skip_random: bool = True,      # skip signals in RANDOM regime
        regime_invert_mean_reverting: bool = True,  # invert LONG↔SHORT in MEAN_REVERTING
        regime_high_vol_threshold_multiplier: float = 1.3,   # widen entry gate in HIGH_VOL
        regime_high_vol_size_multiplier: float = 0.5,        # halve position size in HIGH_VOL
        regime_low_vol_threshold_multiplier: float = 0.85,   # tighten in LOW_VOL
    ):
        self.capital = capital
        self.starting_capital = capital
        self.risk_per_trade_pct = risk_per_trade_pct
        self.max_concurrent = max_concurrent_positions
        self.entry_score_threshold = entry_score_threshold
        self.entry_min_evidence = entry_min_evidence
        self.slippage_bps = slippage_bps
        self.cost_pct = cost_pct_round_trip
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self.max_hold = max_hold_seconds
        # Time stop settings (0 = disabled)
        self.time_stop_seconds = float(time_stop_seconds)
        self.time_stop_min_favor_pct = float(time_stop_min_favor_pct)
        self.time_stop_exits: int = 0     # counter for reporting

        # Phase 2 regime tuning
        self.regime_adaptive = regime_adaptive
        self.regime_skip_random = regime_skip_random
        self.regime_invert_mean_reverting = regime_invert_mean_reverting
        self.regime_high_vol_mult = regime_high_vol_threshold_multiplier
        self.regime_high_vol_size = regime_high_vol_size_multiplier
        self.regime_low_vol_mult = regime_low_vol_threshold_multiplier

        # -- Cooldown manager (whipsaw protection) --
        # Can be shared across multiple executors in dual-analyzer mode.
        # If None, no cooldown enforcement.
        self.cooldown: Optional[CooldownManager] = None
        self.entries_blocked_by_cooldown: int = 0

        # -- Session phase gate (optional) --
        # Blocks entries during LUNCH / PRE_OPEN / CLOSING and after
        # 15:15 no-entry cutoff. Set via attach_session_manager() below,
        # or directly (dual-analyzer shares one across both tools).
        self.session_manager: Optional[SessionStateManager] = None
        self.allowed_phases: FrozenSet[SessionPhase] = DEFAULT_TRADEABLE_PHASES
        self.entries_blocked_by_session: int = 0

        # -- RVOL gate (optional) --
        # Blocks entries when relative volume is below min_rvol threshold.
        # min_rvol = 0.0 → disabled (default).
        self.rvol_calculator: Optional[RVOLCalculator] = None
        self.min_rvol: float = 0.0
        self.strict_rvol_warmup: bool = False
        self.entries_blocked_by_low_rvol: int = 0
        self.entries_allowed_during_rvol_warmup: int = 0

        # -- State filter (default: all actionable, i.e., WEAK/LONG/STRONG) --
        # Can be restricted to STRONG only or STRONG+LONG/SHORT.
        # Set via attribute after __init__.
        self.allowed_signal_states: set = set(_ACTIONABLE_STATES)
        self.entries_blocked_by_state_filter: int = 0

        self.positions: Dict[str, Position] = {}    # symbol → position
        self.closed_trades: List[ClosedTrade] = []
        self.equity_curve: List[Tuple[float, float]] = [(0.0, capital)]
        self.peak_equity = capital
        self.max_drawdown = 0.0
        self.next_trade_id = 1

        self._trades_log = None
        self._equity_log = None
        self._trades_log_path = Path(trades_log_path)
        self._equity_log_path = Path(equity_log_path)

        # Stats
        self.entries_attempted = 0
        self.entries_rejected_slots = 0
        self.entries_rejected_capital = 0
        # Regime-related counters (only relevant when regime_adaptive=True)
        self.regime_rejected_random = 0
        self.regime_inverted_signals = 0
        self.regime_high_vol_reject = 0

    def open(self) -> None:
        self._trades_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._equity_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._trades_log = open(self._trades_log_path, "w", encoding="utf-8", buffering=1)
        self._equity_log = open(self._equity_log_path, "w", encoding="utf-8", buffering=1)
        self._equity_log.write("timestamp,equity,open_positions\n")

    def close_files(self) -> None:
        if self._trades_log:
            self._trades_log.close()
        if self._equity_log:
            self._equity_log.close()

    # -- Optional signal-quality-gate attachment helpers --

    def attach_session_manager(
        self,
        session_manager: SessionStateManager,
        allowed_phases: Optional[FrozenSet[SessionPhase]] = None,
    ) -> None:
        """
        Enable session-phase filtering. Entries during non-allowed phases
        (LUNCH etc.) or after 15:15 no-entry cutoff will be blocked.
        """
        self.session_manager = session_manager
        if allowed_phases is not None:
            self.allowed_phases = allowed_phases

    def attach_rvol_calculator(
        self,
        rvol_calculator: RVOLCalculator,
        min_rvol: float = 1.5,
        strict_warmup: bool = False,
    ) -> None:
        """
        Enable RVOL filtering. Entries when current RVOL < min_rvol will
        be blocked. During RVOL warm-up, entries are allowed unless
        strict_warmup=True.
        """
        self.rvol_calculator = rvol_calculator
        self.min_rvol = float(min_rvol)
        self.strict_rvol_warmup = bool(strict_warmup)

    def on_signal(self, symbol: str, result: SignalResult, ltp: float, ts: float) -> None:
        """
        Called when scanner fires an actionable signal for a symbol.
        Phase 2 regime-adaptive logic applied here when regime_adaptive=True.
        """
        state = result.state.value

        # ---- STATE FILTER (early, cheap) ----
        # If user configured allowed_signal_states (e.g., STRONG-only), skip
        # signals whose state isn't in that set BEFORE any other processing.
        if state not in self.allowed_signal_states:
            self.entries_blocked_by_state_filter += 1
            return

        # ---- Phase 2 — Regime-adaptive signal filtering ----
        # Use the regime attached to this signal's metrics
        regime = result.metrics.regime
        eff_score_threshold = self.entry_score_threshold
        position_size_multiplier = 1.0

        if self.regime_adaptive:
            # 1. Skip if regime not confident yet (warm-up period)
            if not regime.is_confident:
                # Still take signals during warm-up but with default thresholds
                # (Alternative: return here for stricter behavior)
                pass

            # 2. RANDOM regime → skip (no directional edge)
            if self.regime_skip_random and regime.is_confident and \
               regime.trend == "RANDOM":
                self.regime_rejected_random += 1
                return

            # 3. MEAN_REVERTING regime → invert LONG↔SHORT (contrarian trade)
            if self.regime_invert_mean_reverting and regime.is_confident and \
               regime.trend == "MEAN_REVERTING":
                if state in _LONG_STATES:
                    state = state.replace("LONG", "SHORT")
                elif state in _SHORT_STATES:
                    state = state.replace("SHORT", "LONG")
                self.regime_inverted_signals += 1

            # 4. HIGH_VOL → require stronger signal + smaller size
            if regime.volatility == "HIGH":
                eff_score_threshold *= self.regime_high_vol_mult
                position_size_multiplier *= self.regime_high_vol_size

            # 5. LOW_VOL → allow slightly weaker signals
            elif regime.volatility == "LOW":
                eff_score_threshold *= self.regime_low_vol_mult

        # Signal-driven exit: reverse-side signal on open position
        if symbol in self.positions:
            pos = self.positions[symbol]
            if (pos.side == "LONG" and state in _SHORT_STATES) or \
               (pos.side == "SHORT" and state in _LONG_STATES):
                self._close_position(symbol, ltp, ts, "signal_reverse")
                return   # don't immediately reopen — wait for next signal

        # No open position → maybe enter
        if symbol in self.positions:
            return   # already have position on this symbol

        # Entry criteria (using effective threshold if regime-adaptive)
        if state not in _ACTIONABLE_STATES:
            return
        if abs(result.smoothed_score) < eff_score_threshold:
            if self.regime_adaptive and regime.volatility == "HIGH":
                self.regime_high_vol_reject += 1
            return
        if result.evidence_strength < self.entry_min_evidence:
            return

        self.entries_attempted += 1

        # Slot check
        if len(self.positions) >= self.max_concurrent:
            self.entries_rejected_slots += 1
            return

        # Determine direction (state may have been inverted by regime logic)
        side = "LONG" if state in _LONG_STATES else "SHORT"

        # -- COOLDOWN CHECK (before slot check) --
        # After exit, prevent immediate re-entry (whipsaw protection).
        if self.cooldown is not None:
            allowed, cd_reason = self.cooldown.can_enter(symbol, side, ts)
            if not allowed:
                self.entries_blocked_by_cooldown += 1
                return

        # -- SESSION PHASE GATE (optional) --
        # Block entries during LUNCH / PRE_OPEN / CLOSING (or user-defined)
        # and past no-entry cutoff (default 15:15 IST).
        if self.session_manager is not None:
            ok, _reason = self.session_manager.is_tradeable(
                ts, allowed_phases=self.allowed_phases,
                enforce_no_new_entry_cutoff=True,
            )
            if not ok:
                self.entries_blocked_by_session += 1
                return

        # -- RVOL GATE (optional) --
        # Block entries when relative volume is below threshold.
        # Only active when min_rvol > 0 AND rvol_calculator is set.
        if self.rvol_calculator is not None and self.min_rvol > 0.0:
            current_rvol = self.rvol_calculator.get_rvol(symbol, ts)
            if current_rvol is None:
                # RVOL not warmed up yet
                if self.strict_rvol_warmup:
                    self.entries_blocked_by_low_rvol += 1
                    return
                self.entries_allowed_during_rvol_warmup += 1
            elif current_rvol < self.min_rvol:
                self.entries_blocked_by_low_rvol += 1
                return

        # Slippage-adjusted entry price
        slip = self.slippage_bps / 10000.0
        if side == "LONG":
            entry_price = ltp * (1 + slip)
            stop_loss = entry_price * (1 - self.stop_loss_pct)
            take_profit = entry_price * (1 + self.take_profit_pct)
        else:
            entry_price = ltp * (1 - slip)
            stop_loss = entry_price * (1 + self.stop_loss_pct)
            take_profit = entry_price * (1 - self.take_profit_pct)

        # Position size: risk_per_trade × capital / stop_loss_distance_in_₹
        # Apply regime-based position sizing multiplier (e.g., halved in HIGH_VOL)
        stop_distance_rs = abs(entry_price - stop_loss)
        risk_capital = self.capital * self.risk_per_trade_pct * position_size_multiplier
        quantity = int(risk_capital / stop_distance_rs)
        if quantity < 1:
            self.entries_rejected_capital += 1
            return
        # Notional cap (don't use > 20% capital on one trade)
        notional = quantity * entry_price
        if notional > self.capital * 0.20:
            quantity = int((self.capital * 0.20) / entry_price)
            if quantity < 1:
                self.entries_rejected_capital += 1
                return

        pos = Position(
            trade_id=self.next_trade_id,
            symbol=symbol, side=side,
            entry_price=entry_price, entry_ts=ts, entry_ltp=ltp,
            entry_state=state, entry_score=result.smoothed_score,
            entry_evidence=result.evidence_strength,
            quantity=quantity, stop_loss_price=stop_loss,
            take_profit_price=take_profit,
            max_hold_ts=ts + self.max_hold,
        )
        self.positions[symbol] = pos
        self.next_trade_id += 1

    def on_tick(self, symbol: str, ltp: float, ts: float) -> None:
        """Check open position for stop-loss / take-profit / time-stop / max-hold exit."""
        pos = self.positions.get(symbol)
        if pos is None:
            return

        # Check stop-loss / take-profit
        if pos.side == "LONG":
            if ltp <= pos.stop_loss_price:
                self._close_position(symbol, ltp, ts, "stop_loss")
                return
            if ltp >= pos.take_profit_price:
                self._close_position(symbol, ltp, ts, "take_profit")
                return
        else:
            if ltp >= pos.stop_loss_price:
                self._close_position(symbol, ltp, ts, "stop_loss")
                return
            if ltp <= pos.take_profit_price:
                self._close_position(symbol, ltp, ts, "take_profit")
                return

        # -- TIME STOP (early scalp exit) --
        # Gemini's suggestion: if within N seconds the trade hasn't
        # moved X% in our favor, exit. This kills lingering losers before
        # they hit full SL, reducing avg loss size.
        if self.time_stop_seconds > 0:
            age = ts - pos.entry_ts
            if age >= self.time_stop_seconds:
                # Compute current directional return (signed: + means favor)
                if pos.side == "LONG":
                    favor_pct = (ltp - pos.entry_price) / pos.entry_price
                else:
                    favor_pct = (pos.entry_price - ltp) / pos.entry_price

                if favor_pct < self.time_stop_min_favor_pct:
                    self.time_stop_exits += 1
                    self._close_position(symbol, ltp, ts, "time_stop")
                    return

        # Max hold time
        if ts >= pos.max_hold_ts:
            self._close_position(symbol, ltp, ts, "max_hold")

    def force_close_all(self, ts: float, last_prices: Dict[str, float]) -> None:
        """Called at end of session — close any remaining open positions."""
        for symbol in list(self.positions.keys()):
            price = last_prices.get(symbol, self.positions[symbol].entry_price)
            self._close_position(symbol, price, ts, "eod")

    def _close_position(self, symbol: str, ltp: float, ts: float, reason: str) -> None:
        pos = self.positions.pop(symbol)

        # Slippage on exit (in the unfavorable direction)
        slip = self.slippage_bps / 10000.0
        if pos.side == "LONG":
            exit_price = ltp * (1 - slip)
            gross_pnl = (exit_price - pos.entry_price) * pos.quantity
        else:
            exit_price = ltp * (1 + slip)
            gross_pnl = (pos.entry_price - exit_price) * pos.quantity

        # Costs on notional (round-trip)
        avg_notional = (pos.entry_price + exit_price) / 2 * pos.quantity
        cost = avg_notional * self.cost_pct
        net_pnl = gross_pnl - cost

        return_pct = (net_pnl / (pos.entry_price * pos.quantity)) * 100.0

        trade = ClosedTrade(
            trade_id=pos.trade_id, symbol=symbol, side=pos.side,
            entry_price=pos.entry_price, entry_ts=pos.entry_ts,
            entry_state=pos.entry_state, entry_score=pos.entry_score,
            exit_price=exit_price, exit_ts=ts, exit_reason=reason,
            quantity=pos.quantity, gross_pnl=gross_pnl,
            cost=cost, net_pnl=net_pnl,
            return_pct=return_pct,
            hold_seconds=ts - pos.entry_ts,
        )
        self.closed_trades.append(trade)

        # -- Record exit to cooldown manager (whipsaw protection) --
        if self.cooldown is not None:
            self.cooldown.record_exit(symbol=symbol, side=pos.side,
                                       reason=reason, ts=ts)

        # Update capital + equity curve
        self.capital += net_pnl
        self.equity_curve.append((ts, self.capital))

        # Track drawdown
        if self.capital > self.peak_equity:
            self.peak_equity = self.capital
        dd = (self.peak_equity - self.capital) / self.peak_equity
        if dd > self.max_drawdown:
            self.max_drawdown = dd

        # Log
        if self._trades_log:
            self._trades_log.write(
                json.dumps(asdict(trade), separators=(",", ":"), default=str) + "\n"
            )
        if self._equity_log:
            self._equity_log.write(
                f"{ts:.3f},{self.capital:.2f},{len(self.positions)}\n"
            )


# ============================================================
# 3. TRADING SESSION ORCHESTRATOR
# ============================================================

class PaperTradingSession:
    """
    ties together: RealisticNSEFeed → BookDynamicsEngine (per symbol)
                    → PaperExecutor → final report.

    Design: single-threaded synchronous loop (simplicity over concurrency).
    """

    def __init__(
        self, symbols: List[str], duration_seconds: float,
        ticks_per_sym_per_sec: float = 5.0,
        capital: float = 100_000.0,
        # Calibrated to engine STRONG threshold 4.0 (see PaperExecutor comment)
        entry_score_threshold: float = 4.0,
        entry_min_evidence: float = 30.0,
        seed: int = 42,
        # ---- Feed selection ----
        feed_mode: str = "simulate",              # "simulate" | "live"
        scanner_config: Optional[ScannerConfig] = None,  # Required if feed_mode="live"
        # ---- Phase 2 regime-adaptive ----
        regime_adaptive: bool = False,
    ):
        self.symbols = symbols
        self.duration = duration_seconds
        self.rate = ticks_per_sym_per_sec
        self.feed_mode = feed_mode

        # Per-symbol engine + last-known price (will be populated after feed prep)
        self.engines: Dict[str, BookDynamicsEngine] = {}
        self.last_prices: Dict[str, float] = {}

        # Token → symbol map (populated based on feed mode)
        self.token_to_symbol: Dict[int, str] = {}

        # Executor
        self.executor = PaperExecutor(
            capital=capital,
            entry_score_threshold=entry_score_threshold,
            entry_min_evidence=entry_min_evidence,
            regime_adaptive=regime_adaptive,
        )

        # ---- Feed setup ----
        if feed_mode == "live":
            if scanner_config is None:
                raise ValueError("scanner_config required for feed_mode='live'")
            self.feed = LiveNSEFeed(
                scanner_config=scanner_config,
                on_tick_callback=self._on_tick,
                max_duration_seconds=duration_seconds,
            )
            # Symbol list comes from scanner_config, not passed symbols list
            self.symbols = scanner_config.symbols
        else:
            self.feed = RealisticNSEFeed(
                symbols=symbols, on_tick_callback=self._on_tick,
                total_duration_seconds=duration_seconds,
                ticks_per_symbol_per_sec=ticks_per_sym_per_sec,
                seed=seed,
            )

        # Signal stats (for report)
        self.signal_counts: Dict[str, int] = defaultdict(int)
        self.signals_actionable: int = 0
        self.signals_high_evidence: int = 0
        # Regime stats
        self.regime_counts: Dict[str, int] = defaultdict(int)

        self.last_sim_ts: float = 0.0

    def _on_tick(self, msg: Dict) -> None:
        """Called by feed for every generated tick."""
        try:
            token = int(msg["token"])
            symbol = self.token_to_symbol.get(token)
            if symbol is None:
                return

            # Parse using scanner's adapter logic (inline for speed)
            snap = self._parse_msg(msg, symbol)
            if snap is None:
                return

            self.last_sim_ts = snap.timestamp
            self.last_prices[symbol] = snap.ltp

            # Feed RVOL tracker with cumulative day volume (no-op if not attached)
            if self.executor.rvol_calculator is not None:
                self.executor.rvol_calculator.on_tick(
                    symbol, snap.volume_traded, snap.timestamp,
                )

            # Executor's price-based checks (SL/TP/max-hold)
            self.executor.on_tick(symbol, snap.ltp, snap.timestamp)

            # Engine update — skip if engine not initialized (live mode edge case)
            engine = self.engines.get(symbol)
            if engine is None:
                return
            result = engine.update(snap)
            if result is None:
                return

            # Signal stats
            state_val = result.state.value
            self.signal_counts[state_val] += 1
            # Regime tracking (from Phase 2)
            self.regime_counts[result.metrics.regime.label] += 1

            if state_val in _ACTIONABLE_STATES:
                self.signals_actionable += 1
                if result.evidence_strength >= self.executor.entry_min_evidence:
                    self.signals_high_evidence += 1

            # Executor's signal-based logic
            if state_val in _ACTIONABLE_STATES:
                self.executor.on_signal(symbol, result, snap.ltp, snap.timestamp)

        except Exception as e:
            logger.exception("Tick handling error: %s", e)

    @staticmethod
    def _parse_msg(msg: Dict, symbol: str) -> Optional[MarketSnapshot]:
        try:
            ts = float(msg["exchange_timestamp"]) / 1000.0
            ltp = float(msg["last_traded_price"]) * 0.01
            if ltp <= 0:
                return None
            bids = []
            for lv in msg["best_5_buy_data"][:5]:
                p = float(lv["price"]) * 0.01
                q = int(lv["quantity"])
                if p > 0 and q > 0:
                    bids.append(DepthLevel(price=p, quantity=q))
            asks = []
            for lv in msg["best_5_sell_data"][:5]:
                p = float(lv["price"]) * 0.01
                q = int(lv["quantity"])
                if p > 0 and q > 0:
                    asks.append(DepthLevel(price=p, quantity=q))
            if not bids or not asks:
                return None
            return MarketSnapshot(
                timestamp=ts, symbol=symbol,
                ltp=ltp, ltq=int(msg.get("last_traded_quantity") or 0),
                volume_traded=int(msg.get("volume_trade_for_the_day") or 0),
                total_buy_qty=int(msg.get("total_buy_quantity") or 0),
                total_sell_qty=int(msg.get("total_sell_quantity") or 0),
                bids=bids, asks=asks,
                upper_circuit=float(msg.get("upper_circuit_limit") or 0) * 0.01 or None,
                lower_circuit=float(msg.get("lower_circuit_limit") or 0) * 0.01 or None,
            )
        except Exception:
            return None

    def _initialize_engines(self) -> None:
        """Create BookDynamicsEngine per symbol AFTER feed prep (needed for live)."""
        for symbol in self.symbols:
            if symbol not in self.engines:
                self.engines[symbol] = BookDynamicsEngine(config=EngineConfig())
        # Populate token map from feed
        for symbol, token in self.feed.token_map().items():
            self.token_to_symbol[token] = symbol

    def run(self) -> None:
        # For live feed, prepare (login + resolve tokens) BEFORE opening executor
        if self.feed_mode == "live":
            logger.info("Preparing live Angel One feed…")
            resolved = self.feed.prepare()
            # Live symbols come from scanner_config, filter to resolved only
            self.symbols = list(resolved.keys())

        self._initialize_engines()

        self.executor.open()
        real_start = time.perf_counter()

        # SIGINT handler for graceful shutdown during live session
        def _handler(signum, frame):
            logger.info("Shutdown signal received; stopping feed…")
            if hasattr(self.feed, "stop"):
                self.feed.stop()
        _signal_mod.signal(_signal_mod.SIGINT, _handler)
        _signal_mod.signal(_signal_mod.SIGTERM, _handler)

        try:
            self.feed.run()
            # End of session: close all remaining positions
            self.executor.force_close_all(self.last_sim_ts, self.last_prices)
        finally:
            self.executor.close_files()
        real_elapsed = time.perf_counter() - real_start
        if self.feed_mode == "live":
            logger.info("Live session complete: %.1f real minutes.",
                        real_elapsed / 60.0)
        else:
            logger.info("Sim session complete in %.1f real seconds "
                        "(%.1f simulated minutes).",
                        real_elapsed, self.duration / 60.0)


# ============================================================
# 4. COMPREHENSIVE FINAL REPORT
# ============================================================

def generate_report(session: PaperTradingSession) -> str:
    ex = session.executor
    trades = ex.closed_trades
    n_trades = len(trades)
    capital = ex.capital
    starting_capital = ex.starting_capital
    total_pnl = capital - starting_capital
    total_return_pct = total_pnl / starting_capital * 100.0

    lines: List[str] = []
    W = 78
    lines.append("═" * W)
    lines.append("  📊 PAPER TRADING FINAL REPORT")
    lines.append("═" * W)
    lines.append("")
    lines.append(f"  Session duration    : {session.duration/60.0:>8.1f} sim-minutes "
                 f"({session.duration/3600.0:.2f} hours)")
    lines.append(f"  Symbols tracked     : {len(session.symbols):>8}")
    lines.append(f"  Ticks generated     : {session.feed.ticks_generated:>8,}")
    lines.append(f"  Starting capital    : ₹ {starting_capital:>10,.2f}")
    lines.append(f"  Ending capital      : ₹ {capital:>10,.2f}")
    total_style = "✓" if total_pnl > 0 else "✗"
    lines.append(f"  Net P&L             : ₹ {total_pnl:>+10,.2f}  ({total_return_pct:+.3f}%)  {total_style}")
    lines.append(f"  Max drawdown        : {ex.max_drawdown*100:>9.2f}%")
    lines.append(f"  Peak equity         : ₹ {ex.peak_equity:>10,.2f}")

    # -- Signal statistics --
    lines.append("")
    lines.append("─" * W)
    lines.append("  📡 SIGNAL STATISTICS")
    lines.append("─" * W)
    total_signals = sum(session.signal_counts.values())
    lines.append(f"  Total scanner signals fired : {total_signals:>10,}")
    lines.append(f"  Actionable signals          : {session.signals_actionable:>10,}")
    lines.append(f"  High-evidence signals       : {session.signals_high_evidence:>10,}")
    lines.append("")
    lines.append(f"  {'State':<16}{'Count':>10}{'%':>8}")
    for state, count in sorted(session.signal_counts.items(),
                                key=lambda x: -x[1]):
        pct = count / total_signals * 100 if total_signals else 0
        lines.append(f"  {state:<16}{count:>10,}{pct:>7.1f}%")

    # -- Entry statistics --
    lines.append("")
    lines.append("─" * W)
    lines.append("  🎯 TRADE ENTRY STATISTICS")
    lines.append("─" * W)
    lines.append(f"  Entry attempts (score/evidence pass)  : {ex.entries_attempted:>7,}")
    lines.append(f"  Rejected (max concurrent positions)   : {ex.entries_rejected_slots:>7,}")
    lines.append(f"  Rejected (insufficient capital)       : {ex.entries_rejected_capital:>7,}")
    lines.append(f"  Blocked by cooldown                   : {ex.entries_blocked_by_cooldown:>7,}"
                 f"  ({'ENABLED' if ex.cooldown else 'gate disabled'})")
    lines.append(f"  Blocked by session phase              : {ex.entries_blocked_by_session:>7,}"
                 f"  ({'ENABLED' if ex.session_manager else 'gate disabled'})")
    lines.append(f"  Blocked by low RVOL                   : {ex.entries_blocked_by_low_rvol:>7,}"
                 f"  ({'ENABLED @ ' + str(ex.min_rvol) if ex.min_rvol > 0 else 'gate disabled'})")
    _sf = "all actionable" if ex.allowed_signal_states == set(_ACTIONABLE_STATES) \
        else ("STRONG only" if ex.allowed_signal_states == set(_STRONG_STATES)
              else ("STRONG+LONG/SHORT" if ex.allowed_signal_states == set(_NORMAL_AND_STRONG_STATES)
                    else "custom"))
    lines.append(f"  Blocked by state filter               : {ex.entries_blocked_by_state_filter:>7,}"
                 f"  (filter: {_sf})")
    if ex.time_stop_seconds > 0:
        lines.append(f"  Time-stop exits                       : {ex.time_stop_exits:>7,}"
                     f"  (@ {ex.time_stop_seconds:.0f}s, "
                     f"min favor {ex.time_stop_min_favor_pct*100:.3f}%)")
    if ex.rvol_calculator is not None:
        lines.append(f"  Entries allowed during RVOL warmup    : {ex.entries_allowed_during_rvol_warmup:>7,}")
    lines.append(f"  Executed trades (round-trip complete) : {n_trades:>7,}")

    # -- Signal-quality-gate detail sections --
    if ex.session_manager is not None:
        ss = ex.session_manager.stats()
        lines.append("")
        lines.append("─" * W)
        lines.append("  🕐 SESSION PHASE STATS")
        lines.append("─" * W)
        lines.append(f"  Current phase        : {ss['current_phase']}")
        lines.append(f"  Total transitions    : {ss['phase_transitions']}")
        lines.append(f"  Allowed phases       : "
                     f"{', '.join(p.name for p in sorted(ex.allowed_phases, key=lambda x: x.name))}")
        lines.append(f"  No-entry cutoff      : {ss['no_new_entry_after']}")
        lines.append(f"  Holidays configured  : {', '.join(ss['holidays_configured']) or '(none)'}")

    if ex.rvol_calculator is not None:
        rs = ex.rvol_calculator.stats()
        lines.append("")
        lines.append("─" * W)
        lines.append("  📊 RELATIVE VOLUME (RVOL) STATS")
        lines.append("─" * W)
        lines.append(f"  Min RVOL threshold   : {ex.min_rvol:.2f}")
        lines.append(f"  Symbols tracked      : {rs['symbols_tracked']}")
        lines.append(f"  Symbols warmed up    : {rs['symbols_warmed_up']}")
        lines.append(f"  Ticks processed      : {rs['ticks_processed']:,}")
        lines.append(f"  Session resets       : {rs['session_resets']}")
        lines.append(f"  Anomalies capped     : {rs['anomalies_capped']}")

    if n_trades == 0:
        lines.append("")
        lines.append("  ⚠ No trades executed. Adjust entry_score_threshold or")
        lines.append("     entry_min_evidence, or run longer session.")
        lines.append("═" * W)
        return "\n".join(lines)

    # -- Overall trade performance --
    wins = [t for t in trades if t.net_pnl > 0]
    losses = [t for t in trades if t.net_pnl <= 0]
    win_rate = len(wins) / n_trades * 100
    avg_win = sum(t.net_pnl for t in wins) / len(wins) if wins else 0.0
    avg_loss = sum(t.net_pnl for t in losses) / len(losses) if losses else 0.0
    profit_factor = (sum(t.net_pnl for t in wins) / abs(sum(t.net_pnl for t in losses))
                     if losses and sum(t.net_pnl for t in losses) != 0 else float('inf'))
    avg_return_pct = sum(t.return_pct for t in trades) / n_trades
    avg_hold = sum(t.hold_seconds for t in trades) / n_trades

    # Sharpe-ish (using return_pct as sample)
    returns = [t.return_pct for t in trades]
    mean_r = sum(returns) / len(returns)
    var_r = sum((r - mean_r) ** 2 for r in returns) / len(returns)
    std_r = var_r ** 0.5 if var_r > 0 else 0.0001
    sharpe_trade = mean_r / std_r
    # Annualize (rough): trades per year × sharpe / sqrt(trades per year)
    # Simpler: just report per-trade Sharpe

    lines.append("")
    lines.append("─" * W)
    lines.append("  💰 TRADE PERFORMANCE")
    lines.append("─" * W)
    lines.append(f"  Total trades              : {n_trades:>8,}")
    lines.append(f"  Winners                   : {len(wins):>8,}   ({win_rate:.1f}%)")
    lines.append(f"  Losers                    : {len(losses):>8,}   ({100-win_rate:.1f}%)")
    lines.append(f"  Avg winner                : ₹ {avg_win:>+8,.2f}")
    lines.append(f"  Avg loser                 : ₹ {avg_loss:>+8,.2f}")
    if avg_loss != 0:
        payoff = abs(avg_win / avg_loss)
        lines.append(f"  Payoff ratio (win/loss)   : {payoff:>8.2f}x")
    lines.append(f"  Profit factor             : {profit_factor:>8.2f}")
    lines.append(f"  Avg return per trade      : {avg_return_pct:>+8.3f}%")
    lines.append(f"  Avg hold time             : {avg_hold:>8.1f} seconds "
                 f"({avg_hold/60:.1f} min)")
    lines.append(f"  Trade Sharpe (per-trade)  : {sharpe_trade:>8.2f}")

    # -- Regime stats (Phase 2) --
    if ex.regime_adaptive:
        lines.append("")
        lines.append("─" * W)
        lines.append("  🌀 REGIME STATISTICS (Phase 2)")
        lines.append("─" * W)
        lines.append(f"  Signals rejected — RANDOM regime      : {ex.regime_rejected_random:>6,}")
        lines.append(f"  Signals INVERTED — MEAN_REVERTING     : {ex.regime_inverted_signals:>6,}")
        lines.append(f"  Signals rejected — HIGH_VOL threshold : {ex.regime_high_vol_reject:>6,}")

        # Top 5 regime combinations observed
        if session.regime_counts:
            total = sum(session.regime_counts.values())
            lines.append("")
            lines.append(f"  Top regime combinations seen (of {total:,} snapshots):")
            for regime_label, count in sorted(session.regime_counts.items(),
                                               key=lambda x: -x[1])[:6]:
                pct = count / total * 100
                lines.append(f"    {regime_label:<15} {count:>10,} ({pct:>5.1f}%)")

    # -- Exit reason breakdown --
    lines.append("")
    lines.append("─" * W)
    lines.append("  🚪 EXIT REASON BREAKDOWN")
    lines.append("─" * W)
    exit_stats: Dict[str, Dict] = defaultdict(lambda: {"n": 0, "pnl": 0.0, "wins": 0})
    for t in trades:
        s = exit_stats[t.exit_reason]
        s["n"] += 1
        s["pnl"] += t.net_pnl
        if t.net_pnl > 0:
            s["wins"] += 1
    lines.append(f"  {'Reason':<18}{'Trades':>8}{'Wins':>7}{'Win %':>8}"
                 f"{'Total PnL':>14}{'Avg PnL':>12}")
    for reason, s in sorted(exit_stats.items(), key=lambda x: -x[1]["n"]):
        n = s["n"]
        w = s["wins"]
        pct = w / n * 100 if n else 0
        avg = s["pnl"] / n if n else 0
        lines.append(f"  {reason:<18}{n:>8}{w:>7}{pct:>7.1f}%"
                     f"₹{s['pnl']:>+12,.2f} ₹{avg:>+10,.2f}")

    # -- Entry state performance --
    lines.append("")
    lines.append("─" * W)
    lines.append("  🎨 PERFORMANCE BY SIGNAL STATE")
    lines.append("─" * W)
    state_stats: Dict[str, Dict] = defaultdict(lambda: {"n": 0, "pnl": 0.0, "wins": 0})
    for t in trades:
        s = state_stats[t.entry_state]
        s["n"] += 1
        s["pnl"] += t.net_pnl
        if t.net_pnl > 0:
            s["wins"] += 1
    lines.append(f"  {'Signal State':<18}{'Trades':>8}{'Wins':>7}{'Win %':>8}"
                 f"{'Total PnL':>14}{'Avg PnL':>12}")
    for state in ["STRONG_LONG", "LONG", "STRONG_SHORT", "SHORT"]:
        s = state_stats.get(state, {"n": 0, "pnl": 0.0, "wins": 0})
        n = s["n"]
        if n == 0:
            continue
        w = s["wins"]
        pct = w / n * 100
        avg = s["pnl"] / n
        lines.append(f"  {state:<18}{n:>8}{w:>7}{pct:>7.1f}%"
                     f"₹{s['pnl']:>+12,.2f} ₹{avg:>+10,.2f}")

    # -- Top / Bottom trades --
    lines.append("")
    lines.append("─" * W)
    lines.append("  🏆 TOP 5 WINNERS")
    lines.append("─" * W)
    for t in sorted(trades, key=lambda x: -x.net_pnl)[:5]:
        lines.append(f"  {t.symbol:<14} {t.side:<6} {t.entry_state:<14} "
                     f"@{t.entry_price:>8.2f} → {t.exit_price:>8.2f}  "
                     f"₹{t.net_pnl:>+9,.2f}  ({t.return_pct:+.2f}%)  "
                     f"[{t.exit_reason}]")

    lines.append("")
    lines.append("  💥 BOTTOM 5 LOSERS")
    lines.append("─" * W)
    for t in sorted(trades, key=lambda x: x.net_pnl)[:5]:
        lines.append(f"  {t.symbol:<14} {t.side:<6} {t.entry_state:<14} "
                     f"@{t.entry_price:>8.2f} → {t.exit_price:>8.2f}  "
                     f"₹{t.net_pnl:>+9,.2f}  ({t.return_pct:+.2f}%)  "
                     f"[{t.exit_reason}]")

    # -- HONEST VERDICT --
    lines.append("")
    lines.append("═" * W)
    lines.append("  📌 HONEST VERDICT")
    lines.append("═" * W)

    if total_pnl > 0 and win_rate > 55 and profit_factor > 1.5:
        lines.append("  ✅ STRONG STRATEGY: Profitable + high win rate + good payoff.")
        lines.append("     But: simulator is not real NSE. Deploy to VPS paper trading")
        lines.append("     for 2-4 weeks before real capital.")
    elif total_pnl > 0 and profit_factor > 1.0:
        lines.append("  🟡 MARGINAL EDGE: Slight profit. Real NSE could go either way.")
        lines.append("     Recommendation: Longer sim + parameter tuning before live.")
    elif total_pnl > -starting_capital * 0.02:
        lines.append("  ⚠ BREAKEVEN/SMALL LOSS: Strategy struggling in realistic conditions.")
        lines.append("     Real NSE almost certainly loses money after real costs.")
        lines.append("     Recommendation: DO NOT deploy. Tune strategy first.")
    else:
        lines.append("  ❌ LOSING STRATEGY: Significant loss in realistic simulation.")
        lines.append("     DO NOT deploy real money. Fundamental strategy review needed.")

    lines.append("")
    lines.append("  Files generated:")
    lines.append(f"    - {ex._trades_log_path}    ({len(trades)} trade records, JSONL)")
    lines.append(f"    - {ex._equity_log_path}   (equity curve CSV)")
    lines.append("═" * W)

    return "\n".join(lines)


# ============================================================
# 5. CLI ENTRYPOINT
# ============================================================

def _default_symbols() -> List[str]:
    """Sample of 20 liquid Nifty stocks."""
    return [
        "RELIANCE-EQ", "TCS-EQ", "HDFCBANK-EQ", "INFY-EQ", "ICICIBANK-EQ",
        "HINDUNILVR-EQ", "ITC-EQ", "LT-EQ", "SBIN-EQ", "BHARTIARTL-EQ",
        "KOTAKBANK-EQ", "AXISBANK-EQ", "MARUTI-EQ", "BAJFINANCE-EQ",
        "HCLTECH-EQ", "SUNPHARMA-EQ", "WIPRO-EQ", "TATASTEEL-EQ",
        "ASIANPAINT-EQ", "TITAN-EQ",
    ]


def main() -> int:
    p = argparse.ArgumentParser(
        description="NSE Paper Trading Harness — Simulate OR Live on Real Angel One Feed",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick 30-min realistic simulation (no broker needed)
  python3 paper_trader.py --duration-min 30

  # With Phase 2 regime-adaptive logic
  python3 paper_trader.py --duration-min 60 --regime-adaptive

  # LIVE paper trading on real Angel One data (during NSE market hours)
  python3 paper_trader.py --feed live --config config.json --duration-min 390

  # LIVE + regime-adaptive (most realistic real-world test)
  python3 paper_trader.py --feed live --config config.json --regime-adaptive

  # Aggressive simulation parameters
  python3 paper_trader.py --entry-score 3 --entry-evidence 25 --regime-adaptive
""",
    )
    # Feed selection
    p.add_argument("--feed", choices=["simulate", "live"], default="simulate",
                   help="Feed source: 'simulate' (fake ticks) or 'live' "
                        "(real Angel One WebSocket). Default: simulate.")
    p.add_argument("--config", default="config.json",
                   help="Config file for live mode (default: config.json). "
                        "Ignored for simulate mode.")

    # Session parameters
    p.add_argument("--duration-min", type=float, default=60.0,
                   help="Session duration in minutes. Simulate: sim-time. "
                        "Live: real wall-clock time. Default: 60.")
    p.add_argument("--symbols", type=int, default=20,
                   help="Number of symbols (simulate mode only, ignored for live). "
                        "Default: 20.")
    p.add_argument("--rate", type=float, default=5.0,
                   help="Ticks per symbol per second (simulate mode only). Default: 5.")
    p.add_argument("--capital", type=float, default=100000.0,
                   help="Starting capital in ₹. Default: 100,000.")

    # Entry thresholds
    p.add_argument("--entry-score", type=float, default=4.0,
                   help="Min |smoothed_score| to enter (default: 4.0 = "
                        "engine STRONG threshold; use 3.0 for more entries, "
                        "5.0+ for stricter)")
    p.add_argument("--entry-evidence", type=float, default=30.0,
                   help="Min evidence strength to enter (default: 30 = "
                        "achievable at score 3.0 with 100%% agreement; "
                        "raise to 40+ for stricter, lower to 20 for more)")

    # Phase 2
    p.add_argument("--regime-adaptive", action="store_true",
                   help="Enable Phase 2 regime detector: skip RANDOM regime, "
                        "invert signals in MEAN_REVERTING, adapt thresholds & "
                        "size by volatility regime.")

    # -- Signal state filter --
    p.add_argument("--strong-only", action="store_true",
                   help="Trade ONLY STRONG_LONG + STRONG_SHORT signals. "
                        "Skips WEAK/LONG/SHORT entirely.")
    p.add_argument("--skip-weak", action="store_true",
                   help="Trade STRONG + LONG/SHORT (skip only WEAK).")

    # -- Time stop (scalp exit) --
    p.add_argument("--time-stop-sec", type=float, default=0.0,
                   help="Time stop in seconds (default 0 = disabled). "
                        "If trade hasn't moved --time-stop-min-favor%% in "
                        "our favor by this time, exit immediately. "
                        "Recommended: 15-30s (kills lingering losers, "
                        "'alpha decay' protection).")
    p.add_argument("--time-stop-min-favor-pct", type=float, default=0.0005,
                   help="Minimum favorable move %% required to skip time "
                        "stop (default 0.0005 = 0.05%%). Below this at time "
                        "stop trigger → exit immediately.")

    # -- Signal quality gates (OPTIONAL — all default to disabled) --
    p.add_argument("--session-filter", action="store_true",
                   help="Enable NSE session phase filter. Block entries "
                        "during LUNCH, PRE_OPEN, CLOSING, and past 15:15 "
                        "no-entry cutoff. Default: disabled.")
    p.add_argument("--allowed-phases",
                   default="OPENING,MORNING,AFTERNOON",
                   help="Comma-separated phase names allowed for entry when "
                        "--session-filter is set. Default: "
                        "OPENING,MORNING,AFTERNOON")
    p.add_argument("--holidays", default="",
                   help="Comma-separated trading holidays (YYYY-MM-DD).")
    p.add_argument("--no-entry-cutoff", default="15:15",
                   help="HH:MM after which no new entries (IST). Default 15:15.")

    p.add_argument("--min-rvol", type=float, default=0.0,
                   help="Minimum RVOL required for entry. 0.0 = disabled. "
                        "1.5 = require 1.5× 20-min average. Higher = more "
                        "selective. Recommended after live data collection: "
                        "1.2 to 2.0.")
    p.add_argument("--rvol-window-minutes", type=int, default=20,
                   help="RVOL rolling window in minutes (default: 20)")
    p.add_argument("--rvol-warmup-buckets", type=int, default=5,
                   help="RVOL warm-up buckets needed (default: 5)")
    p.add_argument("--rvol-strict-warmup", action="store_true",
                   help="Block entries during RVOL warm-up (safest). "
                        "Default: allow warm-up entries.")

    p.add_argument("--seed", type=int, default=42,
                   help="Random seed for simulate mode (default: 42)")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    scanner_config = None
    if args.feed == "live":
        # Load Angel One config
        try:
            scanner_config = load_config(args.config)
        except Exception as e:
            print(f"\n❌ Config error: {e}\n", file=sys.stderr)
            print("For live mode, you need config.json with Angel One credentials.",
                  file=sys.stderr)
            print("   cp config.example.json config.json  &&  nano config.json\n",
                  file=sys.stderr)
            return 2
        symbols = scanner_config.symbols
        aggregate_tps_note = "TBD (real feed rate)"
    else:
        # Simulate: build symbol list
        all_syms = _default_symbols()
        if args.symbols <= len(all_syms):
            symbols = all_syms[:args.symbols]
        else:
            symbols = all_syms + [f"SYM{i:03d}-EQ"
                                  for i in range(len(all_syms), args.symbols)]
        aggregate_tps_note = f"{args.rate * len(symbols):.0f}"

    logger.info("═" * 78)
    if args.feed == "live":
        logger.info(" 🚀 NSE PAPER TRADING — LIVE MODE (Real Angel One Feed)")
    else:
        logger.info(" 🚀 NSE PAPER TRADING — SIMULATE MODE (Fake Realistic Feed)")
    logger.info("═" * 78)
    logger.info(f"  Feed mode      : {args.feed}")
    logger.info(f"  Duration       : {args.duration_min:.0f} minutes")
    logger.info(f"  Symbols        : {len(symbols)}")
    if args.feed == "simulate":
        logger.info(f"  Tick rate      : {args.rate:.1f} tps/symbol "
                    f"(aggregate {aggregate_tps_note} tps)")
    logger.info(f"  Starting capital: ₹ {args.capital:,.0f}")
    logger.info(f"  Entry threshold: |score|≥{args.entry_score:.1f}, "
                f"evidence≥{args.entry_evidence:.0f}")
    logger.info(f"  Regime-adaptive: {'ON' if args.regime_adaptive else 'OFF'} "
                f"(Phase 2)")
    logger.info("═" * 78)

    session = PaperTradingSession(
        symbols=symbols,
        duration_seconds=args.duration_min * 60.0,
        ticks_per_sym_per_sec=args.rate,
        capital=args.capital,
        entry_score_threshold=args.entry_score,
        entry_min_evidence=args.entry_evidence,
        seed=args.seed,
        feed_mode=args.feed,
        scanner_config=scanner_config,
        regime_adaptive=args.regime_adaptive,
    )

    # -- Attach optional state filter (STRONG-only / skip-weak) --
    if args.strong_only:
        session.executor.allowed_signal_states = set(_STRONG_STATES)
        logger.info(f"  State filter   : STRONG_LONG + STRONG_SHORT only")
    elif args.skip_weak:
        session.executor.allowed_signal_states = set(_NORMAL_AND_STRONG_STATES)
        logger.info(f"  State filter   : STRONG + LONG/SHORT (WEAK skipped)")

    # -- Time stop (alpha decay killer) --
    if args.time_stop_sec > 0:
        session.executor.time_stop_seconds = args.time_stop_sec
        session.executor.time_stop_min_favor_pct = args.time_stop_min_favor_pct
        logger.info(f"  Time stop      : {args.time_stop_sec:.0f}s "
                    f"(exit if favor < {args.time_stop_min_favor_pct*100:.3f}%)")

    # -- Attach optional signal-quality gates to executor --
    if args.session_filter:
        try:
            hh, mm = args.no_entry_cutoff.split(":")
            from datetime import time as _dt_time
            cutoff = _dt_time(int(hh), int(mm))
        except (ValueError, AttributeError):
            print(f"\n❌ Invalid --no-entry-cutoff '{args.no_entry_cutoff}' "
                  f"(expected HH:MM)\n", file=sys.stderr)
            return 2
        holidays = [h.strip() for h in args.holidays.split(",") if h.strip()]
        session_mgr = SessionStateManager(
            holidays=holidays, no_new_entry_after=cutoff,
        )
        # Parse allowed phases
        allowed: Set[SessionPhase] = set()
        for name in args.allowed_phases.split(","):
            name = name.strip().upper()
            if not name:
                continue
            try:
                allowed.add(SessionPhase[name])
            except KeyError:
                print(f"\n❌ Invalid phase name '{name}'\n", file=sys.stderr)
                return 2
        session.executor.attach_session_manager(
            session_mgr, allowed_phases=frozenset(allowed) if allowed else None,
        )
        logger.info(f"  Session filter : ENABLED "
                    f"(allowed={sorted(p.name for p in allowed)}, "
                    f"cutoff={args.no_entry_cutoff})")

    if args.min_rvol > 0.0:
        rvol_calc = RVOLCalculator(
            window_minutes=args.rvol_window_minutes,
            warmup_buckets=args.rvol_warmup_buckets,
        )
        session.executor.attach_rvol_calculator(
            rvol_calc,
            min_rvol=args.min_rvol,
            strict_warmup=args.rvol_strict_warmup,
        )
        logger.info(f"  RVOL gate      : ENABLED "
                    f"(min={args.min_rvol:.2f}, window={args.rvol_window_minutes}min"
                    f"{', STRICT warmup' if args.rvol_strict_warmup else ''})")

    real_start = time.perf_counter()
    session.run()
    real_elapsed = time.perf_counter() - real_start

    report = generate_report(session)
    print()
    print(report)
    print()
    if args.feed == "simulate":
        logger.info(f"Real wall-clock time: {real_elapsed:.1f}s "
                    f"(sim speedup: {args.duration_min*60.0/real_elapsed:.0f}x)")
    else:
        logger.info(f"Live session wall-clock: {real_elapsed/60.0:.1f} minutes")

    return 0


if __name__ == "__main__":
    sys.exit(main())
