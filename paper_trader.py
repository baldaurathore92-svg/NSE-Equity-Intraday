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
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

# Import scanner components (same folder)
from nse_book_scanner import (
    BookDynamicsEngine, DepthLevel, EngineConfig,
    MarketSnapshot, SignalResult, SignalState,
    _LONG_STATES, _SHORT_STATES, _ACTIONABLE_STATES,
    NSE_CM_EXCHANGE_TYPE, SUBSCRIPTION_MODE_SNAP_QUOTE,
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


class PaperExecutor:
    """
    Simulates order execution and position management.

    Trading rules:
      - Enter on STRONG signals (score ≥ +threshold or ≤ -threshold)
      - Position size: risk_per_trade × capital / stop_loss_distance
      - Slippage: entry at LTP × (1 + slippage_bps for LONG, - for SHORT)
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
        entry_score_threshold: float = 5.0,
        entry_min_evidence: float = 40.0,
        slippage_bps: float = 10.0,          # 0.10% slippage
        cost_pct_round_trip: float = 0.0006,  # 0.06%
        stop_loss_pct: float = 0.0030,        # 0.30%
        take_profit_pct: float = 0.0080,      # 0.80% (favorable R:R = 2.67)
        max_hold_seconds: float = 300.0,      # 5 min
        trades_log_path: str = "logs/paper_trades.jsonl",
        equity_log_path: str = "logs/paper_equity.csv",
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

    def on_signal(self, symbol: str, result: SignalResult, ltp: float, ts: float) -> None:
        """Called when scanner fires an actionable signal for a symbol."""
        state = result.state.value

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

        # Entry criteria
        if state not in _ACTIONABLE_STATES:
            return
        if abs(result.smoothed_score) < self.entry_score_threshold:
            return
        if result.evidence_strength < self.entry_min_evidence:
            return

        self.entries_attempted += 1

        # Slot check
        if len(self.positions) >= self.max_concurrent:
            self.entries_rejected_slots += 1
            return

        # Determine direction
        side = "LONG" if state in _LONG_STATES else "SHORT"

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
        stop_distance_rs = abs(entry_price - stop_loss)
        risk_capital = self.capital * self.risk_per_trade_pct
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
        """Check open position for stop-loss / take-profit / max-hold exit."""
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
        entry_score_threshold: float = 5.0,
        entry_min_evidence: float = 40.0,
        seed: int = 42,
    ):
        self.symbols = symbols
        self.duration = duration_seconds
        self.rate = ticks_per_sym_per_sec

        # Per-symbol engine + last-known price
        self.engines: Dict[str, BookDynamicsEngine] = {
            s: BookDynamicsEngine(config=EngineConfig()) for s in symbols
        }
        self.last_prices: Dict[str, float] = {}

        # Token → symbol map for the simulator
        self.token_to_symbol: Dict[int, str] = {}

        # Executor
        self.executor = PaperExecutor(
            capital=capital,
            entry_score_threshold=entry_score_threshold,
            entry_min_evidence=entry_min_evidence,
        )

        # Simulator
        self.feed = RealisticNSEFeed(
            symbols=symbols, on_tick_callback=self._on_tick,
            total_duration_seconds=duration_seconds,
            ticks_per_symbol_per_sec=ticks_per_sym_per_sec,
            seed=seed,
        )

        # Populate token map
        for symbol, token in self.feed.token_map().items():
            self.token_to_symbol[token] = symbol

        # Signal stats (for report)
        self.signal_counts: Dict[str, int] = defaultdict(int)
        self.signals_actionable: int = 0
        self.signals_high_evidence: int = 0

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

            # Executor's price-based checks (SL/TP/max-hold)
            self.executor.on_tick(symbol, snap.ltp, snap.timestamp)

            # Engine update
            result = self.engines[symbol].update(snap)
            if result is None:
                return

            # Signal stats
            state_val = result.state.value
            self.signal_counts[state_val] += 1
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

    def run(self) -> None:
        self.executor.open()
        real_start = time.perf_counter()
        try:
            self.feed.run()
            # End of session: close all remaining positions
            self.executor.force_close_all(self.last_sim_ts, self.last_prices)
        finally:
            self.executor.close_files()
        real_elapsed = time.perf_counter() - real_start
        logger.info("Session complete in %.1f real seconds "
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
    lines.append(f"  Executed trades (round-trip complete) : {n_trades:>7,}")

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
        description="NSE Paper Trading Harness — Realistic Simulation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick 30-min sim on 20 symbols
  python3 paper_trader.py --duration-min 30

  # Full trading day (6.5 hours) on 100 symbols
  python3 paper_trader.py --duration-min 390 --symbols 100

  # Aggressive parameters (weak-signal trading)
  python3 paper_trader.py --entry-score 3 --entry-evidence 25

  # Conservative — only strong signals
  python3 paper_trader.py --entry-score 7 --entry-evidence 60
""",
    )
    p.add_argument("--duration-min", type=float, default=60.0,
                   help="Simulated market time in minutes (default: 60)")
    p.add_argument("--symbols", type=int, default=20,
                   help="Number of symbols to trade (default: 20)")
    p.add_argument("--rate", type=float, default=5.0,
                   help="Ticks per symbol per second (default: 5)")
    p.add_argument("--capital", type=float, default=100000.0,
                   help="Starting capital in ₹ (default: 100,000)")
    p.add_argument("--entry-score", type=float, default=5.0,
                   help="Min |score| to enter (default: 5.0)")
    p.add_argument("--entry-evidence", type=float, default=40.0,
                   help="Min evidence strength to enter (default: 40)")
    p.add_argument("--seed", type=int, default=42,
                   help="Random seed for reproducibility (default: 42)")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    # Symbols
    all_syms = _default_symbols()
    if args.symbols <= len(all_syms):
        symbols = all_syms[:args.symbols]
    else:
        # Extend with generated names
        symbols = all_syms + [f"SYM{i:03d}-EQ" for i in range(len(all_syms),
                                                                args.symbols)]

    logger.info("═" * 78)
    logger.info(" 🚀 NSE PAPER TRADING — Realistic Simulation")
    logger.info("═" * 78)
    logger.info(f"  Duration       : {args.duration_min:.0f} sim-minutes")
    logger.info(f"  Symbols        : {len(symbols)}")
    logger.info(f"  Tick rate      : {args.rate:.1f} tps/symbol "
                f"(aggregate {args.rate * len(symbols):.0f} tps)")
    logger.info(f"  Starting capital: ₹ {args.capital:,.0f}")
    logger.info(f"  Entry threshold: |score|≥{args.entry_score:.1f}, "
                f"evidence≥{args.entry_evidence:.0f}")
    logger.info("═" * 78)

    session = PaperTradingSession(
        symbols=symbols,
        duration_seconds=args.duration_min * 60.0,
        ticks_per_sym_per_sec=args.rate,
        capital=args.capital,
        entry_score_threshold=args.entry_score,
        entry_min_evidence=args.entry_evidence,
        seed=args.seed,
    )

    real_start = time.perf_counter()
    session.run()
    real_elapsed = time.perf_counter() - real_start

    report = generate_report(session)
    print()
    print(report)
    print()
    logger.info(f"Real wall-clock time: {real_elapsed:.1f}s "
                f"(sim speedup: {args.duration_min*60.0/real_elapsed:.0f}x)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
