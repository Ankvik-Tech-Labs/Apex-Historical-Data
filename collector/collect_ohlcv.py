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
    python collect_ohlcv.py --timeframe 1h

    # Shard 0 of 4 (for CI matrix parallelism)
    python collect_ohlcv.py --timeframe 15m --shard 0 --total-shards 4

    # Incremental from a prior data dir
    python collect_ohlcv.py --timeframe 1h --prior-data-dir /path/to/prev/feathers
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


def _load_prior_latest_ts(prior_dir: Path, pair: str, timeframe: str) -> int | None:
    """Return the latest timestamp (ms) in a prior feather file, or None."""
    path = _feather_path(prior_dir, pair, timeframe)
    if not path.exists():
        return None
    try:
        df = pd.read_feather(path)
        if df.empty:
            return None
        col = df.columns[0]  # date is always first column
        ts = df[col].iloc[-1]
        if hasattr(ts, "timestamp"):
            return int(ts.timestamp() * 1000)
        return int(ts)
    except Exception as exc:
        logger.warning("Could not read prior feather %s: %s", path, exc)
        return None


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
) -> list[dict]:
    """Fetch all candles from ``since_s`` to now using windowed start+end calls.

    :param exchange: CCXT apex instance.
    :param api_sym: API symbol string e.g. ``BTCUSDT``.
    :param apex_interval: Apex interval code e.g. ``60`` or ``D``.
    :param candle_secs: Candle duration in seconds.
    :param since_s: Start timestamp in UNIX seconds (inclusive).
    :returns: List of raw candle dicts ``{t, o, h, l, c, v}``.
    """
    all_rows: list[dict] = []
    window_secs = API_MAX * candle_secs
    now_s = int(pd.Timestamp("now", tz="UTC").timestamp())
    start_s = since_s

    while start_s < now_s:
        end_s = min(start_s + window_secs, now_s)
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
            start_s = end_s + 1
            continue

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
    ap.add_argument("--shard", type=int, default=0, help="Shard index (0-based)")
    ap.add_argument("--total-shards", type=int, default=1, help="Total number of shards")
    ap.add_argument("--out-dir", type=Path, default=Path("data"), help="Output directory for feathers")
    ap.add_argument("--prior-data-dir", type=Path, default=None, help="Dir with prior feathers for incremental update")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    apex_interval, candle_secs = TF_MAP[args.timeframe]
    genesis_s = int(pd.Timestamp(DATA_GENESIS, tz="UTC").timestamp())

    exchange = ccxt.apex(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    logger.info("Loading Apex markets...")
    markets = exchange.load_markets()
    all_pairs = sorted(sym for sym, m in markets.items() if m.get("type") == "swap")

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
    saved = skipped = 0

    for i, pair in enumerate(pairs, 1):
        m = markets[pair]
        api_sym = _api_symbol(m)
        out_path = _feather_path(args.out_dir, pair, args.timeframe)

        # Incremental: if we have prior data, start from its latest timestamp + 1 candle
        since_s = genesis_s
        if args.prior_data_dir:
            latest_ms = _load_prior_latest_ts(args.prior_data_dir, pair, args.timeframe)
            if latest_ms is not None:
                since_s = int(latest_ms / 1000) + candle_secs

        logger.info(
            "[%d/%d] %s %s since=%s",
            i, len(pairs), pair, args.timeframe,
            pd.Timestamp(since_s, unit="s", tz="UTC").date(),
        )

        rows = fetch_ohlcv(exchange, api_sym, apex_interval, candle_secs, since_s)

        if not rows:
            logger.warning("  No data — skipping")
            skipped += 1
            continue

        new_df = _rows_to_df(rows)

        # Merge with prior data if doing incremental update
        if args.prior_data_dir and out_path.exists():
            try:
                prior_df = pd.read_feather(args.prior_data_dir / out_path.name)
                prior_df.columns = FREQTRADE_COLS
                prior_df["date"] = pd.to_datetime(prior_df["date"], utc=True)
                combined = pd.concat([prior_df, new_df], ignore_index=True)
                combined.drop_duplicates(subset=["date"], keep="last", inplace=True)
                combined.sort_values("date", inplace=True)
                combined.reset_index(drop=True, inplace=True)
                new_df = combined
            except Exception as exc:
                logger.warning("Could not merge prior data for %s: %s — using new only", pair, exc)

        _save_feather(new_df, out_path)
        logger.info(
            "  Saved %d candles (%s → %s)",
            len(new_df),
            new_df["date"].iloc[0].date(),
            new_df["date"].iloc[-1].date(),
        )
        saved += 1

    logger.info("Done. saved=%d skipped=%d", saved, skipped)


if __name__ == "__main__":
    main()
