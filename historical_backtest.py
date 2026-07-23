#!/usr/bin/env python3
"""
historical_backtest.py
======================
Chronological Backtester for Recorded NSE Tick Data.

WHAT THIS DOES
--------------
1. Reads gzip JSONL tick files produced by tick_recorder.py
2. Reconstructs MarketSnapshot objects (real prices, real Level-2 depth,
   real order flow — captured live from Angel One WebSocket)
3. Feeds them chronologically through per-symbol BookDynamicsEngine
4. Runs PaperExecutor on top (virtual trades with slippage + costs)
5. Generates comprehensive report with REAL hit rate on REAL data

USAGE
-----
Backtest everything in a data directory:
    python3 historical_backtest.py --data-dir data/

Backtest with Phase 2 regime-adaptive:
    python3 historical_backtest.py --data-dir data/ --regime-adaptive

Backtest specific symbols only:
    python3 historical_backtest.py --data-dir data/ \\
        --symbols RELIANCE-EQ,TCS-EQ,HDFCBANK-EQ

Backtest specific date range:
    python3 historical_backtest.py --data-dir data/ \\
        --from-date 2026-07-22 --to-date 2026-07-26

Tune entry thresholds:
    python3 historical_backtest.py --data-dir data/ \\
        --entry-score 6 --entry-evidence 50 --regime-adaptive

WORKFLOW
--------
    Day 1-5: python3 tick_recorder.py --config config.json  (records live NSE)
    Day 6:   python3 historical_backtest.py --data-dir data/  (analyze results)

Multiple runs can be done on the same recorded data with different parameters
to find the best configuration — that's the value of recorded data.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set

# Reuse scanner + paper trader components
from nse_book_scanner import (
    BookDynamicsEngine, DepthLevel, EngineConfig,
    MarketSnapshot, SignalResult, SignalState,
    _LONG_STATES, _SHORT_STATES, _ACTIONABLE_STATES,
)
from paper_trader import (
    PaperExecutor, Position, ClosedTrade,
    generate_report,
)


logger = logging.getLogger("historical_backtest")


# ============================================================
# 1. RECORDED TICK READER — Stream from gzip JSONL files
# ============================================================

class RecordedTickReader:
    """
    Discovers and streams tick records from data-dir in chronological order.

    File layout (produced by tick_recorder.py):
        data-dir/
            YYYY-MM-DD/
                ticks_YYYY-MM-DD_HH.jsonl.gz

    Files are named such that lexicographic order == chronological order.
    Within each file, ticks are appended in WS-delivery order (near-real-time),
    but we sort explicitly per-file to handle any WS reordering.
    """

    def __init__(
        self,
        data_dir: Path,
        symbols_filter: Optional[Set[str]] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ):
        self.data_dir = Path(data_dir)
        self.symbols_filter = symbols_filter
        self.from_date = from_date
        self.to_date = to_date
        self.files = self._discover_files()

        # Stats populated during iteration
        self.total_bytes_read = 0
        self.total_records_read = 0
        self.total_records_skipped = 0

    def _discover_files(self) -> List[Path]:
        """Find all recorded tick files in data-dir tree, sorted chronologically."""
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

        # Recursive glob for hourly files
        files = sorted(self.data_dir.rglob("ticks_*.jsonl.gz"))

        # Optional date filtering
        if self.from_date or self.to_date:
            filtered = []
            for f in files:
                fdate = self._extract_date_from_filename(f)
                if fdate is None:
                    continue
                if self.from_date and fdate < self.from_date:
                    continue
                if self.to_date and fdate > self.to_date:
                    continue
                filtered.append(f)
            files = filtered

        return files

    @staticmethod
    def _extract_date_from_filename(path: Path) -> Optional[date]:
        """Parse date from filename like 'ticks_2026-07-23_09.jsonl.gz'."""
        try:
            # Filename: ticks_YYYY-MM-DD_HH.jsonl.gz
            stem = path.stem  # ticks_YYYY-MM-DD_HH.jsonl
            parts = stem.split("_")
            if len(parts) >= 3:
                return date.fromisoformat(parts[1])
        except (ValueError, IndexError):
            pass
        return None

    def total_size_mb(self) -> float:
        """Return total size of all discovered files in MB."""
        return sum(f.stat().st_size for f in self.files) / (1024 * 1024)

    def iterate_ticks(self) -> Iterator[Dict[str, Any]]:
        """
        Yield tick records in chronological order across all files.

        Per-file strategy:
          - Read entire hour into memory (~50-100 MB uncompressed typical)
          - Filter by symbol
          - Sort by timestamp (defensive — files should already be near-sorted)
          - Yield one-by-one

        Between files: files are already in chronological order (hourly).
        """
        for file_path in self.files:
            self.total_bytes_read += file_path.stat().st_size
            hour_records: List[Dict[str, Any]] = []

            try:
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            self.total_records_skipped += 1
                            continue
                        # Symbol filter
                        if self.symbols_filter is not None:
                            if rec.get("sym") not in self.symbols_filter:
                                self.total_records_skipped += 1
                                continue
                        hour_records.append(rec)
                        self.total_records_read += 1
            except Exception as e:
                logger.warning("Skipping unreadable file %s: %s", file_path, e)
                continue

            # Sort within-hour by exchange timestamp (defensive)
            hour_records.sort(key=lambda r: r.get("ts", 0) or 0)

            # Yield
            for rec in hour_records:
                yield rec

            # Free memory before next hour
            del hour_records


# ============================================================
# 2. HISTORICAL BACKTESTER — Replay recorded data through engines
# ============================================================

class HistoricalBacktester:
    """
    Feeds recorded ticks through per-symbol BookDynamicsEngine and
    PaperExecutor. Same signal/trade logic as live paper trading, but
    driven by recorded data instead of live WebSocket.
    """

    def __init__(
        self,
        reader: RecordedTickReader,
        capital: float = 100_000.0,
        entry_score_threshold: float = 5.0,
        entry_min_evidence: float = 40.0,
        regime_adaptive: bool = False,
        trades_log_path: str = "logs/backtest_trades.jsonl",
        equity_log_path: str = "logs/backtest_equity.csv",
        progress_every_ticks: int = 100_000,
    ):
        self.reader = reader
        self.progress_every = progress_every_ticks

        # Per-symbol engine (created lazily)
        self.engines: Dict[str, BookDynamicsEngine] = {}

        # Executor
        self.executor = PaperExecutor(
            capital=capital,
            entry_score_threshold=entry_score_threshold,
            entry_min_evidence=entry_min_evidence,
            regime_adaptive=regime_adaptive,
            trades_log_path=trades_log_path,
            equity_log_path=equity_log_path,
        )

        # Stats
        self.signal_counts: Dict[str, int] = defaultdict(int)
        self.regime_counts: Dict[str, int] = defaultdict(int)
        self.signals_actionable: int = 0
        self.signals_high_evidence: int = 0

        # Symbol tracking
        self.symbols_seen: Set[str] = set()
        self.last_prices: Dict[str, float] = {}
        self.last_sim_ts: float = 0.0

        # Adapter for the generate_report() function
        self.duration = 0.0

    def run(self) -> None:
        """Execute the backtest — chronological tick replay."""
        real_start = time.perf_counter()
        self.executor.open()

        ticks_processed = 0
        first_ts = None
        try:
            for record in self.reader.iterate_ticks():
                snap = self._parse_record(record)
                if snap is None:
                    continue
                if first_ts is None:
                    first_ts = snap.timestamp

                self._process_snapshot(snap)
                ticks_processed += 1

                if ticks_processed % self.progress_every == 0:
                    real_elapsed = time.perf_counter() - real_start
                    sim_elapsed = snap.timestamp - first_ts
                    logger.info(
                        "  Progress: %s ticks · sim-time %.1f hrs · "
                        "real-time %.1f sec · %.0f ticks/sec",
                        f"{ticks_processed:,}", sim_elapsed / 3600,
                        real_elapsed, ticks_processed / max(real_elapsed, 0.01),
                    )

            # End of data: close all remaining positions using last-seen prices
            self.executor.force_close_all(self.last_sim_ts, self.last_prices)
        finally:
            self.executor.close_files()

        real_elapsed = time.perf_counter() - real_start
        if first_ts is not None:
            self.duration = self.last_sim_ts - first_ts
        logger.info(
            "Backtest complete: %s ticks over %.1f sim-hrs, "
            "processed in %.1f real-sec (%.0fx speedup)",
            f"{ticks_processed:,}",
            self.duration / 3600,
            real_elapsed,
            (self.duration / max(real_elapsed, 0.01)) if self.duration else 0,
        )

    def _parse_record(self, record: Dict[str, Any]) -> Optional[MarketSnapshot]:
        """Convert a recorded JSONL record → MarketSnapshot."""
        try:
            ts_ms = record.get("ts")
            if ts_ms is None:
                return None
            ts = float(ts_ms) / 1000.0

            symbol = record.get("sym")
            if not symbol:
                return None

            ltp_paise = record.get("ltp")
            if not ltp_paise or ltp_paise <= 0:
                return None
            ltp = float(ltp_paise) * 0.01

            bids = []
            for pq in record.get("b", []) or []:
                if not isinstance(pq, (list, tuple)) or len(pq) < 2:
                    continue
                p, q = pq[0], pq[1]
                if p and q and p > 0 and q > 0:
                    bids.append(DepthLevel(price=float(p) * 0.01, quantity=int(q)))
            asks = []
            for pq in record.get("a", []) or []:
                if not isinstance(pq, (list, tuple)) or len(pq) < 2:
                    continue
                p, q = pq[0], pq[1]
                if p and q and p > 0 and q > 0:
                    asks.append(DepthLevel(price=float(p) * 0.01, quantity=int(q)))
            if not bids or not asks:
                return None

            upper_circuit = record.get("uc")
            lower_circuit = record.get("lc")

            return MarketSnapshot(
                timestamp=ts,
                symbol=symbol,
                ltp=ltp,
                ltq=int(record.get("ltq") or 0),
                volume_traded=int(record.get("vtt") or 0),
                total_buy_qty=int(record.get("tbq") or 0),
                total_sell_qty=int(record.get("tsq") or 0),
                bids=bids,
                asks=asks,
                upper_circuit=(float(upper_circuit) * 0.01) if upper_circuit else None,
                lower_circuit=(float(lower_circuit) * 0.01) if lower_circuit else None,
            )
        except (KeyError, ValueError, TypeError):
            return None

    def _process_snapshot(self, snap: MarketSnapshot) -> None:
        """Feed snapshot through engine + executor."""
        symbol = snap.symbol
        self.symbols_seen.add(symbol)
        self.last_sim_ts = snap.timestamp
        self.last_prices[symbol] = snap.ltp

        # Executor price-based exits (SL/TP/max-hold) BEFORE engine update
        self.executor.on_tick(symbol, snap.ltp, snap.timestamp)

        # Lazy engine creation
        engine = self.engines.get(symbol)
        if engine is None:
            engine = BookDynamicsEngine(config=EngineConfig())
            self.engines[symbol] = engine

        result = engine.update(snap)
        if result is None:
            return

        state_val = result.state.value
        self.signal_counts[state_val] += 1
        self.regime_counts[result.metrics.regime.label] += 1

        if state_val in _ACTIONABLE_STATES:
            self.signals_actionable += 1
            if result.evidence_strength >= self.executor.entry_min_evidence:
                self.signals_high_evidence += 1
            self.executor.on_signal(symbol, result, snap.ltp, snap.timestamp)

    # -----------------------------------------------------------
    # Adapter — makes this compatible with paper_trader.generate_report()
    # which expects a PaperTradingSession-like object
    # -----------------------------------------------------------

    @property
    def symbols(self) -> List[str]:
        return sorted(self.symbols_seen)

    @property
    def feed(self):
        """Fake feed object with .ticks_generated attribute."""
        # Return simple namespace with the attribute generate_report needs
        return _FeedInfoAdapter(self.reader.total_records_read)


class _FeedInfoAdapter:
    """Minimal adapter so generate_report() can read ticks_generated."""
    def __init__(self, count: int):
        self.ticks_generated = count


# ============================================================
# 3. CLI ENTRYPOINT
# ============================================================

def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Historical Backtester — Chronological replay of recorded "
                    "NSE tick data through the Book Dynamics Scanner.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Typical workflow:
  Day 1-5: tick_recorder.py records live NSE ticks (5 trading days)
  Day 6:   Run backtest on accumulated data:
           python3 historical_backtest.py --data-dir data/ --regime-adaptive

Compare configurations:
  # Aggressive
  python3 historical_backtest.py --data-dir data/ --entry-score 3

  # Conservative
  python3 historical_backtest.py --data-dir data/ --entry-score 7

  # With regime detector
  python3 historical_backtest.py --data-dir data/ --entry-score 5 --regime-adaptive
""",
    )
    p.add_argument("--data-dir", required=True,
                   help="Directory containing recorded gzip JSONL tick files")
    p.add_argument("--symbols", default=None,
                   help="Comma-separated symbol filter (default: all)")
    p.add_argument("--from-date", type=_parse_date, default=None,
                   help="Start date, YYYY-MM-DD (default: all)")
    p.add_argument("--to-date", type=_parse_date, default=None,
                   help="End date, YYYY-MM-DD (default: all)")

    # Executor params (same as paper_trader.py)
    p.add_argument("--capital", type=float, default=100000.0,
                   help="Starting capital ₹ (default: 100,000)")
    p.add_argument("--entry-score", type=float, default=5.0,
                   help="Min |smoothed_score| to enter (default: 5.0)")
    p.add_argument("--entry-evidence", type=float, default=40.0,
                   help="Min evidence strength to enter (default: 40)")
    p.add_argument("--regime-adaptive", action="store_true",
                   help="Enable Phase 2 regime-adaptive signal filtering")

    p.add_argument("--trades-log", default="logs/backtest_trades.jsonl",
                   help="Path for trade audit log (default: logs/backtest_trades.jsonl)")
    p.add_argument("--equity-log", default="logs/backtest_equity.csv",
                   help="Path for equity curve CSV (default: logs/backtest_equity.csv)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    logger.info("═" * 78)
    logger.info(" 📼 HISTORICAL BACKTEST — Chronological Replay of Recorded Data")
    logger.info("═" * 78)

    # Build reader
    symbols_filter = None
    if args.symbols:
        symbols_filter = {s.strip() for s in args.symbols.split(",") if s.strip()}

    try:
        reader = RecordedTickReader(
            data_dir=Path(args.data_dir),
            symbols_filter=symbols_filter,
            from_date=args.from_date,
            to_date=args.to_date,
        )
    except FileNotFoundError as e:
        print(f"\n❌ {e}\n", file=sys.stderr)
        print("Please run tick_recorder.py first to record live NSE data:", file=sys.stderr)
        print(f"    python3 tick_recorder.py --config config.json --output-dir {args.data_dir}\n",
              file=sys.stderr)
        return 2

    if not reader.files:
        print(f"\n❌ No tick files found in {args.data_dir}\n", file=sys.stderr)
        return 3

    logger.info(f"  Data directory  : {args.data_dir}")
    logger.info(f"  Files found     : {len(reader.files)}")
    logger.info(f"  Total size      : {reader.total_size_mb():.1f} MB (compressed)")
    if symbols_filter:
        logger.info(f"  Symbol filter   : {len(symbols_filter)} symbols")
    if args.from_date:
        logger.info(f"  From date       : {args.from_date}")
    if args.to_date:
        logger.info(f"  To date         : {args.to_date}")
    logger.info(f"  Entry threshold : |score|≥{args.entry_score}, evidence≥{args.entry_evidence}")
    logger.info(f"  Regime-adaptive : {'ON (Phase 2)' if args.regime_adaptive else 'OFF'}")
    logger.info(f"  Starting capital: ₹ {args.capital:,.0f}")
    logger.info("═" * 78)

    # Build backtester
    backtester = HistoricalBacktester(
        reader=reader,
        capital=args.capital,
        entry_score_threshold=args.entry_score,
        entry_min_evidence=args.entry_evidence,
        regime_adaptive=args.regime_adaptive,
        trades_log_path=args.trades_log,
        equity_log_path=args.equity_log,
    )

    # Run
    real_start = time.perf_counter()
    backtester.run()
    real_elapsed = time.perf_counter() - real_start

    # Report — reuse paper_trader.generate_report by passing the backtester
    # (it has the same attribute surface — see adapter properties above)
    report = generate_report(backtester)
    print()
    print(report)
    print()

    logger.info(f"Total backtest wall-clock: {real_elapsed:.1f}s")
    logger.info(f"Trade audit log: {args.trades_log}")
    logger.info(f"Equity curve  : {args.equity_log}")
    logger.info("═" * 78)
    logger.info(" 💡 TIP: Try different parameter combinations on the SAME data:")
    logger.info(f"    python3 {sys.argv[0]} --data-dir {args.data_dir} --entry-score 3")
    logger.info(f"    python3 {sys.argv[0]} --data-dir {args.data_dir} --entry-score 7 --regime-adaptive")
    logger.info("═" * 78)

    return 0


if __name__ == "__main__":
    sys.exit(main())
