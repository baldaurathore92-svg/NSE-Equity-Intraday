#!/usr/bin/env python3
"""
tick_recorder.py
================
Live NSE Cash Market Tick Recorder — captures **real Level-2 SnapQuote data**
from Angel One WebSocket into gzip-compressed JSONL for later backtesting.

WHY THIS EXISTS
---------------
Realistic simulators use random-walk mathematics. Real NSE market has actual
order flow intent: institutional accumulation, spoofing, iceberg orders,
cross-symbol correlation, news reactions. These CANNOT be simulated —
only recorded and replayed.

USAGE
-----
Basic (record until market close):
    python3 tick_recorder.py --config config.json --output-dir data/

Record for specific duration:
    python3 tick_recorder.py --config config.json --output-dir data/ --duration-hours 4

Record with symbol subset:
    python3 tick_recorder.py --config config.json --output-dir data/ \\
        --symbols RELIANCE-EQ,TCS-EQ,HDFCBANK-EQ

Run as background service (survives SSH disconnect):
    tmux new -s recorder
    python3 tick_recorder.py --config config.json --output-dir data/
    # Ctrl+B, D to detach

OUTPUT FORMAT
-------------
Directory structure:
    data/
    ├── 2026-07-23/
    │   ├── ticks_2026-07-23_09.jsonl.gz    (~50-100 MB/hour)
    │   ├── ticks_2026-07-23_10.jsonl.gz
    │   ...
    │   └── ticks_2026-07-23_15.jsonl.gz
    ├── 2026-07-24/
    ...

Each JSONL line (compact format, prices in paise):
    {"ts": 1721720400123, "sym": "RELIANCE-EQ", "ltp": 253055, "ltq": 5,
     "vtt": 1234567, "tbq": 45000, "tsq": 43000,
     "b": [[253000, 100], [252950, 200], ...],
     "a": [[253100, 100], [253150, 200], ...],
     "uc": 278361, "lc": 227750}

DISK USAGE
----------
~30-50 bytes per tick compressed. 100 Nifty symbols × 5 tps × 6.5 hours
≈ 350-600 MB/day. 5-day recording: ~2-3 GB total.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import signal as _signal_mod
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Reuse scanner components
from nse_book_scanner import (
    AngelOneConnector, ScannerConfig, load_config, setup_logging,
    SMARTAPI_AVAILABLE,
)

logger = logging.getLogger("tick_recorder")

# NSE market hours (IST). Recorder auto-stops at MARKET_CLOSE unless
# --duration-hours or --run-until Ctrl+C is used.
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MIN = 30

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


# ============================================================
# STORAGE — Gzip JSONL with hourly rotation
# ============================================================

class GzipJSONLRecorder:
    """
    Simple, robust tick storage backend.

    Design:
      - One gzip file per hour (naturally rotates)
      - Compact JSON schema (short field names, prices as integer paise)
      - Thread-safe (WS callbacks come from multiple threads)
      - Line-buffered for durability (crash-safe up to last line)
      - Async flushing not needed — gzip level 6 is fast enough
    """

    def __init__(self, output_dir: Path, symbol_map: Dict[str, int]):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Reverse map: numeric token → symbol name (WS gives token as string)
        self.token_to_symbol: Dict[str, str] = {str(t): s for s, t in symbol_map.items()}

        self._current_file = None
        self._current_hour_key: Optional[str] = None
        self._current_path: Optional[Path] = None
        self._lock = threading.Lock()

        # Stats
        self.ticks_written = 0
        self.ticks_dropped = 0
        self.bytes_written = 0
        self.file_rotations = 0
        self.start_ts = time.time()

    def _get_or_rotate_file(self):
        """Get current gzip file handle, rotating hourly."""
        now = datetime.now(IST)
        hour_key = now.strftime("%Y-%m-%d_%H")

        if hour_key != self._current_hour_key:
            # Close previous file
            if self._current_file is not None:
                try:
                    self._current_file.close()
                except Exception:
                    pass

            # Create new file (per-day subdirectory)
            day_dir = self.output_dir / now.strftime("%Y-%m-%d")
            day_dir.mkdir(parents=True, exist_ok=True)
            new_path = day_dir / f"ticks_{hour_key}.jsonl.gz"

            # Append mode — safe to resume if recorder restarts within same hour
            self._current_file = gzip.open(
                new_path, "at", encoding="utf-8", compresslevel=6,
            )
            self._current_hour_key = hour_key
            self._current_path = new_path
            self.file_rotations += 1
            logger.info("Rotated to file: %s", new_path)

        return self._current_file

    def on_tick(self, msg: Dict[str, Any]) -> None:
        """
        Called by AngelOneConnector for every WebSocket message.
        Writes one compact JSONL line, handling errors gracefully.
        """
        try:
            token = msg.get("token")
            if token is None:
                self.ticks_dropped += 1
                return

            symbol = self.token_to_symbol.get(str(token))
            if symbol is None:
                # Unknown token — usually means WS delivered a tick for a symbol
                # we didn't subscribe to (rare); count but skip
                self.ticks_dropped += 1
                return

            # Extract bids/asks in compact format
            bids_raw = msg.get("best_5_buy_data", []) or []
            asks_raw = msg.get("best_5_sell_data", []) or []
            bids = [[int(lv.get("price", 0)), int(lv.get("quantity", 0))]
                    for lv in bids_raw[:5] if isinstance(lv, dict)]
            asks = [[int(lv.get("price", 0)), int(lv.get("quantity", 0))]
                    for lv in asks_raw[:5] if isinstance(lv, dict)]

            record = {
                "ts": msg.get("exchange_timestamp"),
                "sym": symbol,
                "ltp": msg.get("last_traded_price"),
                "ltq": msg.get("last_traded_quantity"),
                "vtt": msg.get("volume_trade_for_the_day"),
                "tbq": msg.get("total_buy_quantity"),
                "tsq": msg.get("total_sell_quantity"),
                "b": bids,
                "a": asks,
                "uc": msg.get("upper_circuit_limit"),
                "lc": msg.get("lower_circuit_limit"),
            }

            line = json.dumps(record, separators=(",", ":"), default=str) + "\n"

            with self._lock:
                f = self._get_or_rotate_file()
                f.write(line)
                self.ticks_written += 1
                self.bytes_written += len(line)

        except Exception as e:
            self.ticks_dropped += 1
            logger.debug("Tick write failed: %s", e)

    def close(self) -> None:
        with self._lock:
            if self._current_file is not None:
                try:
                    self._current_file.close()
                    logger.info("Closed file: %s (%d ticks total)",
                                self._current_path, self.ticks_written)
                except Exception as e:
                    logger.error("Error closing file: %s", e)
                self._current_file = None

    def stats(self) -> Dict[str, Any]:
        elapsed = time.time() - self.start_ts
        return {
            "ticks_written": self.ticks_written,
            "ticks_dropped": self.ticks_dropped,
            "bytes_written": self.bytes_written,
            "mb_written": self.bytes_written / (1024 * 1024),
            "elapsed_sec": elapsed,
            "avg_tps": self.ticks_written / max(elapsed, 1),
            "file_rotations": self.file_rotations,
        }


# ============================================================
# MAIN RECORDER LOOP
# ============================================================

def is_market_hours() -> bool:
    """True if current IST time is within NSE market hours (9:15-15:30)."""
    now = datetime.now(IST)
    if now.weekday() >= 5:   # Saturday, Sunday
        return False
    market_open = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN,
                               second=0, microsecond=0)
    market_close = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN,
                                second=0, microsecond=0)
    return market_open <= now <= market_close


def time_until_market_close() -> float:
    """Seconds until 15:30 IST (or 0 if already past)."""
    now = datetime.now(IST)
    market_close = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN,
                                second=0, microsecond=0)
    if now >= market_close:
        return 0.0
    return (market_close - now).total_seconds()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="NSE Real Tick Recorder — record live Angel One Level-2 "
                    "SnapQuote data to gzip JSONL for backtesting.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Typical workflow:
  1. Start recording (during market hours, or overnight to catch open):
       python3 tick_recorder.py --config config.json --output-dir data/

  2. Repeat for 5 days (Mon-Fri) — recorder auto-stops at 15:30 IST daily.

  3. Backtest on recorded data:
       python3 historical_backtest.py --data-dir data/

VPS deployment (recommended):
  # Run in tmux session so SSH disconnect doesn't stop it
  tmux new -s recorder
  python3 tick_recorder.py --config config.json --output-dir ~/nse_data
  # Ctrl+B, D to detach; SSH out safely; return with 'tmux attach -t recorder'
""",
    )
    p.add_argument("--config", default="config.json",
                   help="Angel One config file (default: config.json)")
    p.add_argument("--output-dir", default="data/",
                   help="Directory to write recorded ticks (default: data/)")
    p.add_argument("--duration-hours", type=float, default=None,
                   help="Stop recording after N hours (default: auto-stop at market close)")
    p.add_argument("--symbols", default=None,
                   help="Comma-separated symbol subset (default: all from config)")
    p.add_argument("--skip-market-hours-check", action="store_true",
                   help="Don't check market hours (useful for testing outside NSE hours)")
    p.add_argument("--stats-interval-sec", type=float, default=60.0,
                   help="Print statistics every N seconds (default: 60)")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Load config
    try:
        config = load_config(args.config)
    except Exception as e:
        print(f"\n❌ Config error: {e}\n", file=sys.stderr)
        return 2

    # Override symbol subset if provided
    if args.symbols:
        subset = [s.strip() for s in args.symbols.split(",") if s.strip()]
        # Filter config.symbols to just these
        config.symbols = [s for s in config.symbols if s in subset] + \
                         [s for s in subset if s not in config.symbols]

    setup_logging(config)
    logger.info("═" * 78)
    logger.info(" 📼 NSE TICK RECORDER — Real Level-2 SnapQuote Capture")
    logger.info("═" * 78)
    logger.info(f"  Output directory : {args.output_dir}")
    logger.info(f"  Symbols          : {len(config.symbols)}")
    logger.info(f"  Duration         : {args.duration_hours or 'until market close'} hours")

    # Prerequisites
    if not SMARTAPI_AVAILABLE:
        print("\n❌ smartapi-python not installed. Run:\n"
              "    pip install -r requirements.txt\n", file=sys.stderr)
        return 3

    # Market hours check
    if not args.skip_market_hours_check:
        if not is_market_hours():
            logger.warning("Currently outside NSE market hours (Mon-Fri 9:15-15:30 IST).")
            logger.warning("Recorder will start but WebSocket may deliver no ticks.")
            logger.warning("Use --skip-market-hours-check to suppress this warning.")

    # Login + resolve tokens
    connector = AngelOneConnector(config)
    try:
        connector.login()
        connector.load_scrip_master()
        resolved, missing = connector.resolve_tokens()
    except Exception as e:
        logger.exception("Angel One connection failed: %s", e)
        return 4

    if not resolved:
        logger.error("No symbols resolved. Check config.symbols against Angel One scrip master.")
        return 5

    logger.info(f"  Resolved         : {len(resolved)}/{len(config.symbols)} symbols")
    if missing:
        logger.warning(f"  Missing (skipped): {len(missing)} symbols")

    # Recorder
    recorder = GzipJSONLRecorder(Path(args.output_dir), resolved)

    # Signal handlers for graceful shutdown
    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        logger.info(f"Signal {signum} received; stopping recorder…")
        stop_event.set()

    _signal_mod.signal(_signal_mod.SIGINT, _handle_signal)
    _signal_mod.signal(_signal_mod.SIGTERM, _handle_signal)

    # Start WebSocket
    logger.info("Starting WebSocket subscription…")
    try:
        connector.start_websocket(list(resolved.values()), recorder.on_tick)
    except Exception as e:
        logger.exception("WebSocket start failed: %s", e)
        return 6

    logger.info("═" * 78)
    logger.info(" ✅ Recording started. Ctrl+C to stop gracefully.")
    logger.info("═" * 78)

    # Main loop — stats + stop conditions
    start_ts = time.time()
    last_stats = start_ts
    duration_limit_sec = (args.duration_hours * 3600) if args.duration_hours else None

    try:
        while not stop_event.is_set():
            time.sleep(1.0)

            # Duration limit
            if duration_limit_sec is not None:
                elapsed = time.time() - start_ts
                if elapsed >= duration_limit_sec:
                    logger.info("Duration limit (%.1f hrs) reached; stopping.",
                                args.duration_hours)
                    break

            # Market close auto-stop
            if not args.skip_market_hours_check:
                remaining = time_until_market_close()
                if remaining <= 0:
                    logger.info("Market close (15:30 IST) reached; stopping.")
                    break

            # Periodic stats
            if time.time() - last_stats >= args.stats_interval_sec:
                last_stats = time.time()
                s = recorder.stats()
                logger.info(
                    "  📊 %d ticks (%.0f tps avg) · %.1f MB written · "
                    "%d dropped · %d files rotated",
                    s["ticks_written"], s["avg_tps"], s["mb_written"],
                    s["ticks_dropped"], s["file_rotations"],
                )

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    finally:
        logger.info("Closing WebSocket…")
        try:
            connector.stop()
        except Exception as e:
            logger.debug("Connector stop error: %s", e)
        logger.info("Flushing recorder…")
        recorder.close()

        # Final summary
        s = recorder.stats()
        logger.info("═" * 78)
        logger.info(" 📊 FINAL RECORDING SUMMARY")
        logger.info("═" * 78)
        logger.info(f"  Duration        : {s['elapsed_sec']/60:.1f} minutes")
        logger.info(f"  Ticks recorded  : {s['ticks_written']:,}")
        logger.info(f"  Ticks dropped   : {s['ticks_dropped']:,}")
        logger.info(f"  Data written    : {s['mb_written']:.1f} MB")
        logger.info(f"  Avg throughput  : {s['avg_tps']:.1f} ticks/sec")
        logger.info(f"  Files rotated   : {s['file_rotations']}")
        logger.info(f"  Output location : {args.output_dir}")
        logger.info("═" * 78)
        logger.info(" Next step: run backtest on recorded data")
        logger.info(f"    python3 historical_backtest.py --data-dir {args.output_dir}")
        logger.info("═" * 78)

    return 0


if __name__ == "__main__":
    sys.exit(main())
