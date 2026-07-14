#!/usr/bin/env python3
"""Collect historical OHLCV candles for all Apex DEX perpetual swap markets.

Outputs Freqtrade-compatible feather files::

    {BASE}_{QUOTE}_{SETTLE}-{timeframe}-futures.feather

Apex API quirks handled here:

- ``publicGetV3Klines`` requires both ``start`` and ``end`` in UNIX seconds
- API symbol is concatenated base+quote (e.g. ``BTCUSDT``), not the CCXT market id
- Hard limit of 200 candles per request; exceeding returns empty
- ``since`` / ``start`` alone ignored when window spans > 200 candles
- Data available from ~2024-06-15 depending on asset

Usage::

    # Full timeframe, all pairs
    python collect_ohlcv.py --timeframe 1h --until 1718409600

    # Shard 0 of 4 (for CI matrix parallelism)
    python collect_ohlcv.py --timeframe 15m --shard 0 --total-shards 4 --until 1718409600
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import ccxt
import pandas as pd

logger = logging.getLogger(__name__)

# Apex API hard cap — exceeding this returns empty response
API_MAX = 200

# Apex v3 data genesis — no data before this date
DATA_GENESIS = "2024-01-01"

# CCXT timeframe string → (apex interval str, candle duration seconds)
TF_MAP: dict[str, tuple[str, int]] = {
    "1m":  ("1",   60),
    "5m":  ("5",   300),
    "15m": ("15",  900),
    "30m": ("30",  1800),
    "1h":  ("60",  3600),
    "2h":  ("120", 7200),
    "4h":  ("240", 14400),
    "6h":  ("360", 21600),
    "12h": ("720", 43200),
    "1d":  ("D",   86400),
    "1w":  ("W",   604800),
}

FREQTRADE_COLS = ["date", "open", "high", "low", "close", "volume"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _api_symbol(market: dict) -> str:
    """Return the concatenated base+quote symbol the Apex REST API expects."""
    return f"{market['base']}{market['quote']}"


def _pair_to_filename(pair: str) -> str:
    """Convert unified CCXT pair to Freqtrade file stem, e.g. BTC/USDT:USDT → BTC_USDT_USDT."""
    return pair.replace("/", "_").replace(":", "_")


def _feather_path(out_dir: Path, pair: str, timeframe: str) -> Path:
    stem = _pair_to_filename(pair)
    return out_dir / f"{stem}-{timeframe}-futures.feather"


def select_active_swap_pairs(markets: dict[str, dict]) -> list[str]:
    """Return only currently active perpetual swap markets."""
    return sorted(
        symbol
        for symbol, market in markets.items()
        if market.get("type") == "swap" and market.get("active") is True
    )


def _rows_to_df(rows: list[dict]) -> pd.DataFrame:
    records = [
        {
            "date": int(r["t"]),
            "open": float(r["o"]),
            "high": float(r["h"]),
            "low": float(r["l"]),
            "close": float(r["c"]),
            "volume": float(r["v"]),
        }
        for r in rows
    ]
    df = pd.DataFrame(records, columns=FREQTRADE_COLS)
    df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)
    df.drop_duplicates(subset=["date"], keep="last", inplace=True)
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _save_feather(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index(drop=True).to_feather(path, compression="lz4", compression_level=9)


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------

def fetch_ohlcv(
    exchange: ccxt.Exchange,
    api_sym: str,
    apex_interval: str,
    candle_secs: int,
    since_s: int,
    until_s: int,
) -> list[dict]:
    """Fetch all candles from ``since_s`` to ``until_s`` using windowed start+end calls.

    :param exchange: CCXT apex instance.
    :param api_sym: API symbol string e.g. ``BTCUSDT``.
    :param apex_interval: Apex interval code e.g. ``60`` or ``D``.
    :param candle_secs: Candle duration in seconds.
    :param since_s: Start timestamp in UNIX seconds (inclusive).
    :param until_s: End timestamp in UNIX seconds (inclusive).
    :returns: List of raw candle dicts ``{t, o, h, l, c, v}``.
    """
    all_rows: list[dict] = []
    window_secs = API_MAX * candle_secs
    start_s = since_s

    while start_s <= until_s:
        end_s = min(start_s + window_secs, until_s)
        try:
            raw = exchange.publicGetV3Klines(
                {
                    "symbol": api_sym,
                    "interval": apex_interval,
                    "start": str(start_s),
                    "end": str(end_s),
                    "limit": str(API_MAX),
                }
            )
        except ccxt.RateLimitExceeded:
            logger.warning("Rate limited on %s %s — sleeping 10s", api_sym, apex_interval)
            time.sleep(10)
            continue
        except ccxt.BaseError as exc:
            logger.warning("API error %s %s: %s", api_sym, apex_interval, exc)
            raise

        batch = raw.get("data", {}).get(api_sym, [])
        if batch:
            all_rows.extend(batch)

        start_s = end_s + 1
        time.sleep(exchange.rateLimit / 1000.0)

    return all_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Apex DEX OHLCV collector")
    ap.add_argument("--timeframe", required=True, choices=list(TF_MAP), help="Candle timeframe")
    ap.add_argument("--until", type=int, required=True, help="UTC UNIX seconds cutoff to fetch through")
    ap.add_argument("--shard", type=int, default=0, help="Shard index (0-based)")
    ap.add_argument("--total-shards", type=int, default=1, help="Total number of shards")
    ap.add_argument("--out-dir", type=Path, default=Path("data"), help="Output directory for feathers")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    apex_interval, candle_secs = TF_MAP[args.timeframe]
    genesis_s = int(pd.Timestamp(DATA_GENESIS, tz="UTC").timestamp())
    if args.until < genesis_s:
        ap.error(f"--until must be >= DATA_GENESIS ({DATA_GENESIS})")

    exchange = ccxt.apex(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    logger.info("Loading Apex markets...")
    markets = exchange.load_markets()
    all_pairs = select_active_swap_pairs(markets)

    # Deterministic strided sharding: shard 0 takes [0, N, 2N, ...], shard 1 takes [1, N+1, ...]
    pairs = all_pairs[args.shard :: args.total_shards]
    logger.info(
        "Shard %d/%d — %d pairs for %s",
        args.shard,
        args.total_shards,
        len(pairs),
        args.timeframe,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    for i, pair in enumerate(pairs, 1):
        m = markets[pair]
        api_sym = _api_symbol(m)
        out_path = _feather_path(args.out_dir, pair, args.timeframe)

        logger.info(
            "[%d/%d] %s %s since=%s until=%s",
            i, len(pairs), pair, args.timeframe,
            pd.Timestamp(genesis_s, unit="s", tz="UTC").date(),
            pd.Timestamp(args.until, unit="s", tz="UTC").date(),
        )

        rows = fetch_ohlcv(exchange, api_sym, apex_interval, candle_secs, genesis_s, args.until)

        if not rows:
            raise RuntimeError(f"No data returned for active market {pair} {args.timeframe}")

        new_df = _rows_to_df(rows)

        _save_feather(new_df, out_path)
        logger.info(
            "  Saved %d candles (%s → %s)",
            len(new_df),
            new_df["date"].iloc[0].date(),
            new_df["date"].iloc[-1].date(),
        )
        saved += 1

    logger.info("Done. saved=%d", saved)


if __name__ == "__main__":
    main()
