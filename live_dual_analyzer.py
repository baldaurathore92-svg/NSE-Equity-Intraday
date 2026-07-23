#!/usr/bin/env python3
"""
live_dual_analyzer.py
=====================
UNIFIED live analyzer — runs BOTH HitRateAnalyzer AND PaperExecutor on the
SAME live Angel One tick stream simultaneously, with shared CooldownManager
so both measure identical "would-actually-be-tradeable" signals.

WHY DUAL ANALYSIS?
------------------
The two tools answer complementary questions:

  HitRateAnalyzer answers: "Are the scanner's signals directionally correct?"
    → Pure prediction accuracy at horizons (5s, 15s, 30s, 60s, 120s, 300s)
    → No trade simulation, no capital, no SL/TP
    → Best-case scenario measurement

  PaperExecutor answers:   "Do the signals actually make money after costs?"
    → Full trade simulation: entry slippage, SL, TP, max_hold, costs
    → Real capital tracking, drawdown, Sharpe
    → Realistic outcome measurement

Running BOTH on same signals reveals:
  ✓ Both show edge   → Strategy is real, ready for small-capital deploy
  ⚠ Hit rate good, paper P&L bad → Signals correct but costs eat edge
  ⚠ Hit rate bad, paper P&L good → SL/TP saving weak signals (lucky)
  ✗ Both show loss   → Strategy needs fundamental rework

USAGE
-----
Basic (rich UI, 60-min session):
    python3 live_dual_analyzer.py --config config.json

Full trading day headless (VPS):
    python3 live_dual_analyzer.py --config config.json \\
        --duration-hours 6.5 --no-ui

With diagnostics (first-time run):
    python3 live_dual_analyzer.py --config config.json --diagnose

Custom cooldown + regime-adaptive:
    python3 live_dual_analyzer.py --config config.json \\
        --cooldown-seconds 180 --regime-adaptive

TICK PATH (per WS message)
--------------------------
    Angel One WebSocket
          ↓
     [Health Monitor]         ← silent-failure detection
          ↓
     [AngelOneWSAdapter]      ← parse to MarketSnapshot
          ↓
     [BookDynamicsEngine]     ← 17 metrics + Phase 2 regime
          ↓
     [Split: same tick to BOTH]
        ↓                ↓
  [HitRateAnalyzer] [PaperExecutor]
        ↓                ↓
   Horizon evals    SL/TP checks + entries
        ↓                ↓
        └────┬───────────┘
             ↓
     [Shared CooldownManager]
        (both use same gate — apples-to-apples)
             ↓
      [Combined EOD Report]
"""

from __future__ import annotations

import argparse
import json
import logging
import signal as _signal_mod
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

# Reuse from other modules
from nse_book_scanner import (
    BookDynamicsEngine, DepthLevel, EngineConfig,
    MarketSnapshot, SignalResult, SignalState,
    _LONG_STATES, _SHORT_STATES, _ACTIONABLE_STATES,
    AngelOneConnector, AngelOneWSAdapter, ScannerConfig,
    load_config, setup_logging, SMARTAPI_AVAILABLE,
)
from live_hit_rate_analyzer import (
    HitRateAnalyzer, LiveSignalMonitor, LiveSignal,
    HitRateBucket, PendingPrediction,
    DataFlowHealthMonitor, RawMessageDumper, _diagnose_parse_failure,
    is_market_hours, seconds_until_market_close, IST,
)
from paper_trader import (
    PaperExecutor, Position, ClosedTrade, CooldownManager,
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


logger = logging.getLogger("dual_analyzer")


# ============================================================
# DUAL ANALYZER SESSION — main orchestrator
# ============================================================

class DualAnalyzerSession:
    """
    Runs HitRateAnalyzer + PaperExecutor concurrently on same live feed.

    Design:
      - Single WebSocket connection to Angel One (efficient)
      - Single BookDynamicsEngine per symbol (shared analysis)
      - Shared CooldownManager (both tools see same set of tradeable signals)
      - Shared DataFlowHealthMonitor (silent-failure detection)
      - Both tools update independently on each tick

    Thread model:
      - WebSocket callback thread → _on_tick (both analyzers updated)
      - UI thread → reads snapshots (locked read)
      - Health monitor thread → periodic warnings
    """

    def __init__(
        self,
        config: ScannerConfig,
        # -- HitRateAnalyzer params --
        horizons_s: List[float] = None,
        transaction_cost_pct: float = 0.0006,
        min_samples_for_verdict: int = 20,
        signal_dedup_seconds: float = 5.0,
        # -- PaperExecutor params --
        capital: float = 100_000.0,
        entry_score_threshold: float = 5.0,
        entry_min_evidence: float = 40.0,
        risk_per_trade_pct: float = 0.01,
        stop_loss_pct: float = 0.0030,
        take_profit_pct: float = 0.0080,
        max_hold_seconds: float = 300.0,
        max_concurrent_positions: int = 5,
        regime_adaptive: bool = False,
        # -- Shared cooldown --
        cooldown_seconds: float = 120.0,
        cooldown_flip_multiplier: float = 2.0,
        cooldown_sl_multiplier: float = 1.5,
        # -- Logging --
        hit_rate_log_path: str = "logs/hit_rate_predictions.jsonl",
        paper_trades_log_path: str = "logs/paper_trades.jsonl",
        paper_equity_log_path: str = "logs/paper_equity.csv",
        report_path: str = "logs/dual_eod_report.txt",
        # -- Diagnostic mode --
        diagnose: bool = False,
        dump_count: int = 100,
        dump_path: str = "logs/raw_ws_dump.jsonl",
    ):
        if not SMARTAPI_AVAILABLE:
            raise ImportError(
                "smartapi-python not installed. Run:\n"
                "    pip install -r requirements.txt"
            )

        self.config = config
        horizons_s = horizons_s or [5.0, 15.0, 30.0, 60.0, 120.0, 300.0]

        # -- SHARED COOLDOWN (both tools use same gate) --
        self.cooldown = CooldownManager(
            cooldown_seconds=cooldown_seconds,
            flip_multiplier=cooldown_flip_multiplier,
            stop_loss_multiplier=cooldown_sl_multiplier,
        )

        # -- HitRateAnalyzer with shared cooldown --
        self.hit_analyzer = HitRateAnalyzer(
            horizons_s=horizons_s,
            transaction_cost_pct=transaction_cost_pct,
            log_path=hit_rate_log_path,
            min_samples_for_verdict=min_samples_for_verdict,
            signal_dedup_seconds=signal_dedup_seconds,
            cooldown=self.cooldown,   # ← SHARED
        )

        # -- PaperExecutor with shared cooldown --
        self.paper_executor = PaperExecutor(
            capital=capital,
            entry_score_threshold=entry_score_threshold,
            entry_min_evidence=entry_min_evidence,
            risk_per_trade_pct=risk_per_trade_pct,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            max_hold_seconds=max_hold_seconds,
            max_concurrent_positions=max_concurrent_positions,
            cost_pct_round_trip=transaction_cost_pct,
            trades_log_path=paper_trades_log_path,
            equity_log_path=paper_equity_log_path,
            regime_adaptive=regime_adaptive,
        )
        # Attach shared cooldown to executor
        self.paper_executor.cooldown = self.cooldown

        # -- Angel One connector --
        self.connector = AngelOneConnector(config)
        self.token_to_symbol: Dict[int, str] = {}

        # Per-symbol engines (shared across both analyzers)
        self.engines: Dict[str, BookDynamicsEngine] = {}
        self.symbols_seen: set = set()
        self.last_prices: Dict[str, float] = {}

        # Global tick stats
        self.started_at: Optional[float] = None
        self.total_ticks_received = 0
        self.total_ticks_dropped = 0
        self.total_signals_computed = 0
        self.signal_state_counts: Dict[str, int] = defaultdict(int)
        self.regime_counts: Dict[str, int] = defaultdict(int)

        # Latency tracking
        self._latency_samples: Deque[float] = deque(maxlen=1000)
        self._latency_max = 0.0

        # -- Defensive diagnostics --
        self.health = DataFlowHealthMonitor(
            expected_symbols_count=len(config.symbols),
        )
        self.dumper: Optional[RawMessageDumper] = None
        if diagnose:
            self.dumper = RawMessageDumper(Path(dump_path), max_dumps=dump_count)

        self._health_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()
        self.report_path = report_path

    def prepare_stages(self) -> bool:
        """Login + scrip master + tokens. Returns True on success."""
        print()
        print("=" * 72)
        print("  STAGE 1/4 — Logging in to Angel One SmartAPI…")
        print("=" * 72)
        try:
            self.connector.login()
            print("  ✅ Logged in successfully")
        except Exception as e:
            print(f"  ❌ Login failed: {e}")
            logger.exception("Login failed")
            return False

        print()
        print("=" * 72)
        print("  STAGE 2/4 — Loading scrip master + resolving tokens…")
        print("=" * 72)
        try:
            self.connector.load_scrip_master()
            resolved, missing = self.connector.resolve_tokens()
            if not resolved:
                print("  ❌ No symbols resolved. Check config.symbols.")
                return False
            self.token_to_symbol = {t: s for s, t in resolved.items()}
            print(f"  ✅ Resolved {len(resolved)}/{len(self.config.symbols)} symbols")
            if missing:
                miss_str = ", ".join(missing[:5]) + ("…" if len(missing) > 5 else "")
                print(f"  ⚠  Missing (skipped): {miss_str}")
        except Exception as e:
            print(f"  ❌ Token resolution failed: {e}")
            logger.exception("Token resolution failed")
            return False

        if self.dumper is not None:
            print()
            print("=" * 72)
            print("  🔍 DIAGNOSTIC MODE ACTIVE")
            print("=" * 72)
            print(f"  Will save first {self.dumper.max_dumps} raw WS messages.")
            print(f"  Location: {self.dumper.dump_path}")
            print("  First 5 also echoed to console — verify field names visually.")

        return True

    def start(self) -> bool:
        """Start WebSocket + health monitor. Returns True on success."""
        self.hit_analyzer.open()
        if self.dumper is not None:
            self.dumper.open()
        self.started_at = time.time()
        self.health.started_at = self.started_at
        self.paper_executor.open()

        tokens = list(self.token_to_symbol.keys())
        print()
        print("=" * 72)
        print("  STAGE 3/4 — Starting WebSocket subscription…")
        print("=" * 72)
        try:
            self.connector.start_websocket(tokens, self._on_tick)
            print(f"  ✅ WebSocket subscription initiated for {len(tokens)} tokens")
        except Exception as e:
            print(f"  ❌ WebSocket start failed: {e}")
            logger.exception("WebSocket start failed")
            return False

        # Background health monitor
        self._health_thread = threading.Thread(
            target=self._health_check_loop, name="health-monitor", daemon=True,
        )
        self._health_thread.start()

        print()
        print("=" * 72)
        print("  STAGE 4/4 — Dual live tracking active. Ctrl+C to stop.")
        print("=" * 72)
        print("  Running BOTH: Hit Rate Analyzer + Paper Trader (shared cooldown)")
        return True

    def _health_check_loop(self) -> None:
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
        self.hit_analyzer.close()
        # Force-close all open paper positions
        self.paper_executor.force_close_all(time.time(), self.last_prices)
        self.paper_executor.close_files()

    def _on_tick(self, msg: Dict[str, Any]) -> None:
        """
        Single tick → both analyzers. Called from WebSocket thread.
        Latency budget: <500µs to keep up with peak tick rates.
        """
        t_start = time.perf_counter()
        try:
            self.total_ticks_received += 1

            # -- Route token → symbol --
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
                self.health.record_message(None, False, f"unknown_token ({token})")
                if self.dumper:
                    self.dumper.dump(msg, False, "unknown_token")
                return

            # -- Parse to MarketSnapshot --
            snap = AngelOneWSAdapter.parse(msg, symbol)
            if snap is None:
                self.total_ticks_dropped += 1
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

            # -- STEP 1: BOTH analyzers process the tick (SL/TP + horizon eval) --
            # Hit rate analyzer: evaluate pending predictions (real-time update)
            self.hit_analyzer.on_tick(symbol, snap.ltp, snap.timestamp)
            # Paper executor: check SL/TP/max_hold for open positions
            self.paper_executor.on_tick(symbol, snap.ltp, snap.timestamp)

            # -- STEP 2: Engine update (single per symbol, shared) --
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

            # -- STEP 3: If actionable, feed BOTH analyzers --
            # Cooldown check happens INSIDE each analyzer (via shared self.cooldown)
            if state in _ACTIONABLE_STATES:
                self.hit_analyzer.record_signal(symbol, result, snap.ltp, snap.timestamp)
                self.paper_executor.on_signal(symbol, result, snap.ltp, snap.timestamp)

        except Exception as e:
            logger.exception("_on_tick error: %s", e)
        finally:
            elapsed_us = (time.perf_counter() - t_start) * 1_000_000
            self._latency_samples.append(elapsed_us)
            if elapsed_us > self._latency_max:
                self._latency_max = elapsed_us

    def latency_stats(self) -> Tuple[float, float, float]:
        if not self._latency_samples:
            return (0.0, 0.0, 0.0)
        s = sorted(self._latency_samples)
        n = len(s)
        return (sum(s) / n, s[n // 2], s[min(n - 1, int(n * 0.99))])

    def ticks_per_second(self) -> float:
        if self.started_at is None:
            return 0.0
        return self.total_ticks_received / max(time.time() - self.started_at, 1.0)


# ============================================================
# DUAL CONSOLE UI (Rich)
# ============================================================

class DualAnalyzerUI:
    """
    5-panel live dashboard showing BOTH analyzers side-by-side.

    Layout:
        ┌─────────── HEADER (session stats, latency, cooldown) ───────────┐
        ├─────────────────────────┬─────────────────────────────────────┤
        │ LIVE HIT RATE VERDICT   │ LIVE PAPER TRADER (open positions)  │
        │ Open signals + MFE/MAE  │ Open positions + P&L                │
        ├─────────────────────────┴─────────────────────────────────────┤
        │  HIT RATE by state × horizon (statistical, from evaluated)    │
        ├─────────────────────────────────────────────────────────────────┤
        │  PAPER TRADE stats (win rate, PnL, sharpe, drawdown)          │
        └─────────────────────────────────────────────────────────────────┘
    """

    STATE_STYLE = {
        "STRONG_LONG":  "bold green",
        "LONG":         "green",
        "WEAK_LONG":    "dim green",
        "STRONG_SHORT": "bold red",
        "SHORT":        "red",
        "WEAK_SHORT":   "dim red",
    }

    def __init__(self, session: DualAnalyzerSession, refresh_ms: int = 1000):
        if not RICH_AVAILABLE:
            raise ImportError("rich not installed: pip install rich")
        self.session = session
        self.refresh_hz = max(1, 1000 // refresh_ms)
        self.console = Console()
        self._shutdown = threading.Event()

    def _header_panel(self) -> Panel:
        s = self.session
        elapsed = int(time.time() - (s.started_at or time.time()))
        h, rem = divmod(elapsed, 3600)
        m, sec = divmod(rem, 60)
        avg_us, p50_us, p99_us = s.latency_stats()

        # Live verdict from hit rate monitor
        hv = s.hit_analyzer.live_monitor.live_verdict()
        cd_stats = s.cooldown.stats()
        pe = s.paper_executor
        n_open_pos = len(pe.positions)
        pnl = pe.capital - pe.starting_capital
        pnl_pct = pnl / pe.starting_capital * 100

        line1 = Text.assemble(
            ("🔬  DUAL ANALYZER  ", "bold cyan"),
            ("(HitRate + PaperTrader on shared cooldown)", "dim cyan"),
            ("   |   ", "dim"),
            (f"Uptime: {h:02d}:{m:02d}:{sec:02d}", "white"),
            ("   |   ", "dim"),
            (f"Ticks: {s.total_ticks_received:,}", "white"),
            (" ", "dim"),
            (f"({s.ticks_per_second():.0f}/s)", "dim"),
            ("   |   ", "dim"),
            (f"Signals: {s.total_signals_computed:,}", "white"),
        )
        line2 = Text.assemble(
            ("⚡ LIVE: ", "bold magenta"),
            (f"{hv['total_open']} open signals ", "white"),
            (f"({hv['winning']}✓/{hv['losing']}✗) ", "green"),
            (f"{hv['hit_rate_pct']:.0f}% right now", "cyan"),
            ("   |   ", "dim"),
            ("💰 PAPER: ", "bold yellow"),
            (f"{n_open_pos} pos, ", "white"),
            (f"₹{pnl:+,.2f} ", "green" if pnl >= 0 else "red"),
            (f"({pnl_pct:+.3f}%)", "green" if pnl >= 0 else "red"),
        )
        line3 = Text.assemble(
            ("Cooldown: ", "bold cyan"),
            (f"{cd_stats['cooldown_seconds']:.0f}s", "white"),
            ("  ·  ", "dim"),
            (f"{cd_stats['total_exits_recorded']} exits, "
             f"{cd_stats['total_entries_blocked']} blocked", "white"),
            ("   |   ", "dim"),
            (f"Latency p50={p50_us:.0f}µs p99={p99_us:.0f}µs", "dim"),
            ("   |   ", "dim"),
            (f"Health: parsed={s.health.msgs_parsed_ok:,}, "
             f"failed={s.health.msgs_parse_failed:,}", "dim"),
        )
        return Panel(Align.center(Text.assemble(line1, "\n", line2, "\n", line3)),
                     border_style="cyan")

    def _live_signals_panel(self) -> Table:
        """Left half of middle row — live open signals from HitRateAnalyzer."""
        signals = self.session.hit_analyzer.live_monitor.snapshot_open(top_n=15)
        cost_frac = self.session.hit_analyzer.cost

        table = Table(
            title="⚡  HIT RATE LIVE  —  Open Signals",
            title_style="bold magenta",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("Symbol", style="cyan", width=13)
        table.add_column("State", width=13)
        table.add_column("Age", justify="right", width=5)
        table.add_column("Dir Ret", justify="right", width=9)
        table.add_column("MFE", justify="right", width=8)
        table.add_column("Status", width=13)

        if not signals:
            table.caption = "No open signals yet…"
            table.caption_style = "dim"
            return table

        for sig in signals:
            state_style = self.STATE_STYLE.get(sig.state, "white")
            dir_pct = sig.current_directional_return * 100
            mfe_pct = sig.max_favorable_excursion * 100
            age_s = int(sig.seconds_elapsed)
            age_str = f"{age_s}s" if age_s < 60 else f"{age_s // 60}m{age_s % 60}s"

            if sig.current_directional_return > cost_frac:
                status_text, status_style = "✓✓ PROFIT", "bold green"
            elif sig.current_directional_return > 0:
                status_text, status_style = "✓ winning", "green"
            elif sig.current_directional_return > -0.001:
                status_text, status_style = "~ flat", "yellow"
            else:
                status_text, status_style = "✗ losing", "red"

            mfe_display = ("—" if sig.time_to_mfe_s < 0
                           else f"{mfe_pct:+.2f}%")
            dir_style = "green" if dir_pct > 0 else "red"

            table.add_row(
                sig.symbol,
                Text(sig.state, style=state_style),
                age_str,
                Text(f"{dir_pct:+.3f}%", style=dir_style),
                Text(mfe_display, style="green" if sig.time_to_mfe_s >= 0 else "dim"),
                Text(status_text, style=status_style),
            )
        return table

    def _open_positions_panel(self) -> Table:
        """Right half of middle row — PaperExecutor open positions with live P&L."""
        pe = self.session.paper_executor
        positions = list(pe.positions.values())

        table = Table(
            title="💰  PAPER TRADER  —  Open Positions",
            title_style="bold yellow",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("Symbol", style="cyan", width=13)
        table.add_column("Side", width=6)
        table.add_column("Qty", justify="right", width=6)
        table.add_column("Entry", justify="right", width=9)
        table.add_column("SL / TP", justify="right", width=17)
        table.add_column("Live P&L", justify="right", width=10)
        table.add_column("Status", width=10)

        if not positions:
            table.caption = "No open positions yet…"
            table.caption_style = "dim"
            return table

        for pos in positions[:15]:
            last_price = self.session.last_prices.get(pos.symbol, pos.entry_price)
            if pos.side == "LONG":
                unrealized = (last_price - pos.entry_price) * pos.quantity
                dist_to_sl_pct = (last_price - pos.stop_loss_price) / pos.entry_price * 100
            else:
                unrealized = (pos.entry_price - last_price) * pos.quantity
                dist_to_sl_pct = (pos.stop_loss_price - last_price) / pos.entry_price * 100

            side_style = "green" if pos.side == "LONG" else "red"
            pnl_style = "green" if unrealized > 0 else "red"

            if dist_to_sl_pct < 0.1:
                status, status_style = "🚨 near SL", "bold red"
            elif unrealized > 0:
                status, status_style = "✓ winning", "green"
            else:
                status, status_style = "~ open", "yellow"

            table.add_row(
                pos.symbol,
                Text(pos.side, style=side_style),
                f"{pos.quantity}",
                f"{pos.entry_price:.2f}",
                f"{pos.stop_loss_price:.2f}/{pos.take_profit_price:.2f}",
                Text(f"₹{unrealized:+,.1f}", style=pnl_style),
                Text(status, style=status_style),
            )
        return table

    def _hit_rate_stats_table(self) -> Table:
        """Bottom-upper: statistical hit rate by state × horizon."""
        stats = self.session.hit_analyzer.snapshot_state_horizon()
        table = Table(
            title="📊  HIT RATE (statistical, evaluated at horizons)",
            title_style="bold magenta",
            expand=True,
            show_lines=False,
            header_style="bold",
        )
        table.add_column("State", style="cyan", width=13)
        table.add_column("Horizon", justify="right", width=8)
        table.add_column("N", justify="right", width=6)
        table.add_column("Hit %", justify="right", width=8)
        table.add_column("Net Edge %", justify="right", width=11)
        table.add_column("Verdict", width=18)

        state_order = ["STRONG_LONG", "LONG", "WEAK_LONG",
                       "WEAK_SHORT", "SHORT", "STRONG_SHORT"]
        any_data = False
        for state in state_order:
            style = self.STATE_STYLE.get(state, "white")
            for h in self.session.hit_analyzer.horizons:
                b = stats.get((state, h))
                if b is None or b.count == 0:
                    continue
                any_data = True
                verdict_text, verdict_style = self.session.hit_analyzer.verdict(b)
                edge = b.avg_net_edge * 100
                edge_style = ("bold green" if edge > 0.02
                              else "yellow" if edge > 0 else "red")
                table.add_row(
                    Text(state, style=style),
                    f"{int(h)}s",
                    f"{b.count:,}",
                    f"{b.hit_rate*100:.1f}%",
                    Text(f"{edge:+.3f}%", style=edge_style),
                    Text(verdict_text, style=verdict_style),
                )
        if not any_data:
            table.caption = "Waiting for horizon expiries…"
            table.caption_style = "dim"
        return table

    def _paper_stats_panel(self) -> Panel:
        """Bottom-lower: PaperExecutor summary stats."""
        pe = self.session.paper_executor
        trades = pe.closed_trades
        n = len(trades)

        if n == 0:
            content = Text("No closed trades yet — waiting for first SL/TP/exit…",
                           style="dim")
        else:
            wins = [t for t in trades if t.net_pnl > 0]
            losses = [t for t in trades if t.net_pnl <= 0]
            win_rate = len(wins) / n * 100
            avg_win = sum(t.net_pnl for t in wins) / len(wins) if wins else 0
            avg_loss = sum(t.net_pnl for t in losses) / len(losses) if losses else 0
            pf = (sum(t.net_pnl for t in wins) / abs(sum(t.net_pnl for t in losses))
                  if losses and sum(t.net_pnl for t in losses) != 0 else 0)
            total_pnl = pe.capital - pe.starting_capital

            # Exit reason breakdown
            exit_counts: Dict[str, int] = defaultdict(int)
            for t in trades:
                exit_counts[t.exit_reason] += 1

            pnl_style = "bold green" if total_pnl > 0 else "bold red"
            wr_style = "green" if win_rate > 50 else "red"

            content = Text.assemble(
                (f"💰 PAPER TRADES: ", "bold yellow"),
                (f"{n} closed", "white"),
                ("   |   ", "dim"),
                ("Win rate: ", "white"),
                (f"{win_rate:.1f}% ", wr_style),
                (f"({len(wins)}W / {len(losses)}L)", "dim"),
                ("   |   ", "dim"),
                ("Total P&L: ", "white"),
                (f"₹{total_pnl:+,.2f}", pnl_style),
                ("   |   ", "dim"),
                ("Max DD: ", "white"),
                (f"{pe.max_drawdown*100:.2f}%", "yellow"),
                ("\n", ""),
                (f"  Avg win: ₹{avg_win:+.2f}", "green"),
                ("   |   ", "dim"),
                (f"Avg loss: ₹{avg_loss:+.2f}", "red"),
                ("   |   ", "dim"),
                (f"Profit factor: {pf:.2f}", "white"),
                ("   |   ", "dim"),
                (f"Blocked by cooldown: {pe.entries_blocked_by_cooldown}", "cyan"),
                ("\n", ""),
                (f"  Exits: ", "dim"),
                (" · ".join(f"{r}={c}" for r, c in sorted(exit_counts.items())), "white"),
            )
        return Panel(content, title="Paper Trade Stats", border_style="yellow")

    def _render(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=5),
            Layout(name="live_pair", ratio=3),
            Layout(name="hit_stats", ratio=2),
            Layout(name="paper_stats", size=6),
        )
        # Middle row: hit rate live | paper open positions (side by side)
        layout["live_pair"].split_row(
            Layout(name="hit_live"),
            Layout(name="paper_live"),
        )
        layout["header"].update(self._header_panel())
        layout["hit_live"].update(self._live_signals_panel())
        layout["paper_live"].update(self._open_positions_panel())
        layout["hit_stats"].update(self._hit_rate_stats_table())
        layout["paper_stats"].update(self._paper_stats_panel())
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
# HEADLESS STATUS + COMBINED EOD REPORT
# ============================================================

def _print_headless_status(s: DualAnalyzerSession) -> None:
    hv = s.hit_analyzer.live_monitor.live_verdict()
    pe = s.paper_executor
    _, p50_us, p99_us = s.latency_stats()
    pnl = pe.capital - pe.starting_capital
    pnl_pct = pnl / pe.starting_capital * 100

    ts_str = datetime.now(IST).strftime("%H:%M:%S")
    print(
        f"[{ts_str} IST] ticks={s.total_ticks_received:,} "
        f"({s.ticks_per_second():.0f}/s) "
        f"signals={s.total_signals_computed:,}  |  "
        f"⚡ HITRATE: {hv['total_open']} open "
        f"({hv['winning']}✓/{hv['losing']}✗ = {hv['hit_rate_pct']:.0f}% now)  "
        f"|  💰 PAPER: {len(pe.positions)} pos, {len(pe.closed_trades)} closed, "
        f"₹{pnl:+,.0f} ({pnl_pct:+.2f}%)  "
        f"|  🔒 Cooldown blocks: hit={s.hit_analyzer.signals_blocked_by_cooldown}, "
        f"paper={pe.entries_blocked_by_cooldown}  "
        f"|  lat p50/p99={p50_us:.0f}/{p99_us:.0f}µs",
        flush=True,
    )


def generate_dual_eod_report(s: DualAnalyzerSession) -> str:
    """Combined EOD report — both hit rate + paper trade stats."""
    from live_hit_rate_analyzer import generate_eod_report as gen_hit_report
    from paper_trader import generate_report as gen_paper_report

    W = 82
    lines: List[str] = []
    lines.append("═" * W)
    lines.append("  🔬 DUAL ANALYZER — FINAL COMBINED REPORT")
    lines.append("═" * W)
    lines.append("  Both HitRateAnalyzer and PaperExecutor ran on the SAME live")
    lines.append("  Angel One tick stream with a SHARED CooldownManager.")
    lines.append("  Any performance difference is REAL, not sampling artifact.")
    lines.append("═" * W)

    # Session
    elapsed = time.time() - (s.started_at or time.time())
    h, rem = divmod(int(elapsed), 3600)
    m, sec = divmod(rem, 60)
    lines.append("")
    lines.append(f"  Session duration    : {h:02d}:{m:02d}:{sec:02d}")
    lines.append(f"  Symbols tracked     : {len(s.symbols_seen)}")
    lines.append(f"  Total ticks         : {s.total_ticks_received:,}")
    lines.append(f"  Signals computed    : {s.total_signals_computed:,}")

    # Data flow health
    lines.append("")
    lines.append("─" * W)
    lines.append("  📶 DATA FLOW QUALITY")
    lines.append("─" * W)
    lines.extend(s.health.summary_report_lines())

    # Cooldown stats
    lines.append("")
    lines.append("─" * W)
    lines.append("  🔒 COOLDOWN MANAGER (shared)")
    lines.append("─" * W)
    cd = s.cooldown.stats()
    lines.append(f"  Base cooldown      : {cd['cooldown_seconds']:.0f} seconds")
    lines.append(f"  Flip multiplier    : {cd['flip_multiplier']}× "
                 f"(direction change = {cd['cooldown_seconds']*cd['flip_multiplier']:.0f}s)")
    lines.append(f"  Post-SL multiplier : {cd['stop_loss_multiplier']}× "
                 f"({cd['cooldown_seconds']*cd['stop_loss_multiplier']:.0f}s)")
    lines.append(f"  Symbols tracked    : {cd['symbols_tracked']:,}")
    lines.append(f"  Total exits recorded: {cd['total_exits_recorded']:,}")
    lines.append(f"  Total entries blocked: {cd['total_entries_blocked']:,}")
    if cd["blocks_by_reason"]:
        lines.append("  Block breakdown:")
        for reason, count in cd["blocks_by_reason"].items():
            lines.append(f"    {reason:<20} {count:,}")
    lines.append(f"  HitRate signals blocked : {s.hit_analyzer.signals_blocked_by_cooldown:,}")
    lines.append(f"  PaperExec entries blocked: {s.paper_executor.entries_blocked_by_cooldown:,}")

    # -- HIT RATE analyzer report --
    lines.append("")
    lines.append("═" * W)
    lines.append("  📊 PART A — HIT RATE ANALYSIS (pure signal accuracy)")
    lines.append("═" * W)
    # Build a fake session-like object for gen_hit_report
    class _FakeSession:
        def __init__(self, s):
            self.started_at = s.started_at
            self.symbols_seen = s.symbols_seen
            self.total_ticks_received = s.total_ticks_received
            self.total_signals_computed = s.total_signals_computed
            self.signal_state_counts = s.signal_state_counts
            self.regime_counts = s.regime_counts
            self.health = s.health
    hit_report = gen_hit_report(_FakeSession(s), s.hit_analyzer)
    lines.append(hit_report)

    # -- PAPER TRADER report --
    lines.append("")
    lines.append("═" * W)
    lines.append("  💰 PART B — PAPER TRADING RESULTS (actual P&L simulation)")
    lines.append("═" * W)
    # Adapter for paper_trader.generate_report
    class _FakePaperSession:
        def __init__(self, s):
            self.executor = s.paper_executor
            self.symbols = list(s.symbols_seen)
            self.duration = time.time() - (s.started_at or time.time())
            self.signal_counts = s.signal_state_counts
            self.signals_actionable = sum(
                c for st, c in s.signal_state_counts.items()
                if st in _ACTIONABLE_STATES
            )
            self.signals_high_evidence = self.signals_actionable
            self.regime_counts = s.regime_counts
            class _Feed:
                ticks_generated = s.total_ticks_received
            self.feed = _Feed()
    paper_report = gen_paper_report(_FakePaperSession(s))
    lines.append(paper_report)

    # -- COMBINED VERDICT --
    lines.append("")
    lines.append("═" * W)
    lines.append("  🎯 COMBINED VERDICT")
    lines.append("═" * W)
    stats = s.hit_analyzer.snapshot_state_horizon()
    total_hit_count = sum(b.count for b in stats.values())
    total_net_edge = (sum(b.sum_net_return for b in stats.values()) /
                       total_hit_count if total_hit_count else 0.0)

    pe = s.paper_executor
    n_trades = len(pe.closed_trades)
    paper_pnl = pe.capital - pe.starting_capital
    paper_wr = 0.0
    if n_trades > 0:
        wins = sum(1 for t in pe.closed_trades if t.net_pnl > 0)
        paper_wr = wins / n_trades * 100

    lines.append("")
    lines.append(f"  Hit rate signals evaluated: {total_hit_count:,}")
    lines.append(f"  Hit rate avg net edge     : {total_net_edge*100:+.4f}%")
    lines.append(f"  Paper trades executed     : {n_trades:,}")
    lines.append(f"  Paper trade win rate      : {paper_wr:.1f}%")
    lines.append(f"  Paper trade net P&L       : ₹{paper_pnl:+,.2f}")
    lines.append("")

    if total_hit_count < 20 or n_trades < 5:
        lines.append("  ⚠ INSUFFICIENT DATA — run longer for confident verdict")
    else:
        # Both metrics analyzed together
        hit_positive = total_net_edge > 0.0002   # +0.02%
        paper_positive = paper_pnl > 0

        if hit_positive and paper_positive:
            lines.append("  ✅✅ BOTH POSITIVE — Strategy shows real edge on real data")
            lines.append("     RECOMMENDED: Continue 5-10 more days validation, then")
            lines.append("     small-capital deployment (₹10-25k).")
        elif hit_positive and not paper_positive:
            lines.append("  ⚠  MIXED: Signals directionally correct but paper trades lose")
            lines.append("     LIKELY CAUSE: Costs + slippage eating marginal edge.")
            lines.append("     TRY: Higher entry threshold (--entry-score 7+), only STRONG signals")
        elif not hit_positive and paper_positive:
            lines.append("  ⚠  SUSPICIOUS: Paper profit despite marginal hit rate")
            lines.append("     LIKELY CAUSE: Lucky SL/TP outcomes or small sample")
            lines.append("     TRY: Longer test (2-4 weeks) before trusting this")
        else:
            lines.append("  ❌ BOTH NEGATIVE — Strategy does not work on real data")
            lines.append("     DO NOT deploy capital. Fundamental review needed.")
            lines.append("     Consider: different strategy, or accept this doesn't work")
    lines.append("═" * W)

    return "\n".join(lines)


# ============================================================
# CLI + MAIN
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Dual Live Analyzer — HitRate + PaperTrader on shared feed.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default 1-hour session with rich UI
  python3 live_dual_analyzer.py --config config.json

  # Full trading day headless (VPS)
  python3 live_dual_analyzer.py --config config.json --duration-hours 6.5 --no-ui

  # First-time diagnostic run
  python3 live_dual_analyzer.py --config config.json --diagnose --duration-hours 0.25

  # Custom cooldown (3 min base, 6 min for direction flip)
  python3 live_dual_analyzer.py --config config.json --cooldown-seconds 180

  # Conservative — only STRONG signals + wider cooldown
  python3 live_dual_analyzer.py --config config.json \\
      --entry-score 7 --entry-evidence 60 --cooldown-seconds 240
""",
    )
    p.add_argument("--config", default="config.json")
    p.add_argument("--duration-hours", type=float, default=1.0)
    p.add_argument("--no-ui", action="store_true")
    p.add_argument("--skip-market-hours-check", action="store_true")

    # HitRateAnalyzer params
    p.add_argument("--horizons", default="5,15,30,60,120,300",
                   help="Comma-separated horizons in seconds")
    p.add_argument("--cost-pct", type=float, default=0.0006,
                   help="Round-trip cost (default 0.06%%)")
    p.add_argument("--min-samples", type=int, default=20)
    p.add_argument("--dedup-seconds", type=float, default=5.0)

    # PaperExecutor params
    p.add_argument("--capital", type=float, default=100000.0)
    p.add_argument("--entry-score", type=float, default=5.0)
    p.add_argument("--entry-evidence", type=float, default=40.0)
    p.add_argument("--stop-loss-pct", type=float, default=0.0030)
    p.add_argument("--take-profit-pct", type=float, default=0.0080)
    p.add_argument("--max-hold-sec", type=float, default=300.0)
    p.add_argument("--max-positions", type=int, default=5)
    p.add_argument("--regime-adaptive", action="store_true")

    # Shared cooldown
    p.add_argument("--cooldown-seconds", type=float, default=120.0,
                   help="Base cooldown seconds after any exit (default 120)")
    p.add_argument("--flip-multiplier", type=float, default=2.0,
                   help="Direction-flip cooldown multiplier (default 2.0)")
    p.add_argument("--sl-multiplier", type=float, default=1.5,
                   help="Post-stop-loss cooldown multiplier (default 1.5)")

    # Diagnostic
    p.add_argument("--diagnose", action="store_true")
    p.add_argument("--dump-count", type=int, default=100)
    p.add_argument("--dump-path", default="logs/raw_ws_dump.jsonl")

    # Report paths
    p.add_argument("--report-path", default="logs/dual_eod_report.txt")

    p.add_argument("--symbols", default=None,
                   help="Optional comma-separated symbol subset")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"\n❌ Config error: {e}\n", file=sys.stderr)
        return 2

    if args.symbols:
        subset = {s.strip() for s in args.symbols.split(",") if s.strip()}
        config.symbols = [s for s in config.symbols if s in subset] + \
                         [s for s in subset if s not in config.symbols]

    setup_logging(config)

    try:
        horizons = [float(h.strip()) for h in args.horizons.split(",")]
    except ValueError:
        print(f"❌ Invalid --horizons: {args.horizons}", file=sys.stderr)
        return 2

    print()
    print("═" * 82)
    print(" 🔬 NSE DUAL ANALYZER — HitRate + PaperTrader concurrent live run")
    print("═" * 82)
    print(f"  Config file      : {args.config}")
    print(f"  Symbols          : {len(config.symbols)}")
    print(f"  Session duration : {args.duration_hours} hours")
    print(f"  Horizons         : {horizons} seconds")
    print(f"  Cost model       : -{args.cost_pct*100:.4f}% round-trip")
    print(f"  Entry threshold  : |score|≥{args.entry_score}, evidence≥{args.entry_evidence}")
    print(f"  SL / TP          : {args.stop_loss_pct*100:.2f}% / {args.take_profit_pct*100:.2f}%")
    print(f"  Cooldown         : {args.cooldown_seconds:.0f}s (flip {args.flip_multiplier}×, "
          f"SL {args.sl_multiplier}×)")
    print(f"  Regime adaptive  : {'ON' if args.regime_adaptive else 'OFF'}")
    print(f"  Diagnostic mode  : {'ON' if args.diagnose else 'OFF'}")
    print("═" * 82)

    if not SMARTAPI_AVAILABLE:
        print("\n❌ smartapi-python not installed. pip install -r requirements.txt\n",
              file=sys.stderr)
        return 3

    if not args.skip_market_hours_check and not is_market_hours():
        logger.warning("Outside NSE market hours. WebSocket may deliver no data.")

    # Build session
    session = DualAnalyzerSession(
        config=config,
        horizons_s=horizons,
        transaction_cost_pct=args.cost_pct,
        min_samples_for_verdict=args.min_samples,
        signal_dedup_seconds=args.dedup_seconds,
        capital=args.capital,
        entry_score_threshold=args.entry_score,
        entry_min_evidence=args.entry_evidence,
        stop_loss_pct=args.stop_loss_pct,
        take_profit_pct=args.take_profit_pct,
        max_hold_seconds=args.max_hold_sec,
        max_concurrent_positions=args.max_positions,
        regime_adaptive=args.regime_adaptive,
        cooldown_seconds=args.cooldown_seconds,
        cooldown_flip_multiplier=args.flip_multiplier,
        cooldown_sl_multiplier=args.sl_multiplier,
        report_path=args.report_path,
        diagnose=args.diagnose,
        dump_count=args.dump_count,
        dump_path=args.dump_path,
    )

    # Prepare (login + tokens)
    if not session.prepare_stages():
        return 4

    # Shutdown handler
    stop_event = threading.Event()
    def _handle_signal(signum, frame):
        logger.info("Signal %s received; stopping…", signum)
        stop_event.set()
    _signal_mod.signal(_signal_mod.SIGINT, _handle_signal)
    _signal_mod.signal(_signal_mod.SIGTERM, _handle_signal)

    # Start
    if not session.start():
        return 5

    max_sec = args.duration_hours * 3600
    start_ts = time.time()
    last_status = start_ts

    try:
        if args.no_ui or not RICH_AVAILABLE:
            if not RICH_AVAILABLE:
                logger.warning("rich not installed — headless mode")
            print("\nHeadless mode — status prints every 10s. Ctrl+C to stop.")
            while not stop_event.is_set():
                time.sleep(2.0)
                if (time.time() - start_ts) >= max_sec:
                    logger.info("Max duration reached")
                    break
                if not args.skip_market_hours_check and seconds_until_market_close() <= 0:
                    logger.info("Market close reached")
                    break
                if time.time() - last_status >= 10.0:
                    last_status = time.time()
                    _print_headless_status(session)
        else:
            ui = DualAnalyzerUI(session, refresh_ms=1000)
            ui_thread = threading.Thread(target=ui.run, name="ui", daemon=True)
            ui_thread.start()
            while not stop_event.is_set():
                time.sleep(1.0)
                if (time.time() - start_ts) >= max_sec:
                    break
                if not args.skip_market_hours_check and seconds_until_market_close() <= 0:
                    break
            ui.stop()
            ui_thread.join(timeout=2.0)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt")
    finally:
        logger.info("Stopping session…")
        session.stop()

    # Combined EOD report
    report = generate_dual_eod_report(session)
    print()
    print(report)
    print()

    try:
        Path(args.report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report_path).write_text(report, encoding="utf-8")
        logger.info(f"Combined EOD report saved: {args.report_path}")
    except Exception as e:
        logger.warning(f"Report save failed: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
