#!/usr/bin/env python3
"""Collect historical funding rates for all Apex DEX perpetual swap markets.

Outputs Freqtrade-compatible feather files::

    {BASE}_{QUOTE}_{SETTLE}-1h-funding_rate.feather

with the signed funding rate carried in the ``open`` column and the remaining
OHLCV columns zero — the schema Freqtrade's ``CandleType.FUNDING_RATE`` loader
expects. Companion to :mod:`collect_ohlcv`; shares its sharding + ``--until``
contract so it slots into the same CI matrix.

Apex API quirks handled here:

- ``fetch_funding_rate_history`` ignores a bare ``since``; it needs an explicit
  ``end`` (``paramsEndTime``) per window, so we page in ``[start, end]`` slices.
- API symbol is the concatenated base+quote (e.g. ``BTCUSDT``), like OHLCV.
- Data available from ~2024-06-15 depending on asset.

Usage::

    # All pairs, single job
    python collect_funding.py --until 1718409600

    # Shard 0 of 2 (for CI matrix parallelism)
    python collect_funding.py --shard 0 --total-shards 2 --until 1718409600
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

# Apex funding endpoint page cap (``/v3/history-funding`` rejects larger).
API_MAX = 100

# Apex v3 data genesis — no funding before this date.
DATA_GENESIS = "2024-01-01"

# Funding filename timeframe token (apex ``funding_fee_timeframe`` is 1h).
FUNDING_TF = "1h"

FREQTRADE_COLS = ["date", "open", "high", "low", "close", "volume"]


# ---------------------------------------------------------------------------
# Helpers (kept byte-identical to collect_ohlcv for cross-collector parity)
# ---------------------------------------------------------------------------

def _api_symbol(market: dict) -> str:
    """Return the concatenated base+quote symbol the Apex REST API expects."""
    return f"{market['base']}{market['quote']}"


def _pair_to_filename(pair: str) -> str:
    """Convert unified CCXT pair to Freqtrade file stem, e.g. BTC/USDT:USDT → BTC_USDT_USDT."""
    return pair.replace("/", "_").replace(":", "_")


def _feather_path(out_dir: Path, pair: str) -> Path:
    stem = _pair_to_filename(pair)
    return out_dir / f"{stem}-{FUNDING_TF}-funding_rate.feather"


def select_active_swap_pairs(markets: dict[str, dict]) -> list[str]:
    """Return only currently active perpetual swap markets."""
    return sorted(
        symbol
        for symbol, market in markets.items()
        if market.get("type") == "swap" and market.get("active") is True
    )


def _rows_to_df(rows: list[dict]) -> pd.DataFrame:
    """Build the Freqtrade FUNDING_RATE frame — rate in ``open``, other cols zero."""
    records = [
        {"date": int(r["timestamp"]), "open": float(r["fundingRate"])}
        for r in rows
        if r.get("fundingRate") is not None and r.get("timestamp") is not None
    ]
    df = pd.DataFrame(records, columns=["date", "open"])
    if df.empty:
        return pd.DataFrame(columns=FREQTRADE_COLS)
    df["date"] = pd.to_datetime(df["date"], unit="ms", utc=True)
    df.drop_duplicates(subset=["date"], keep="last", inplace=True)
    df.sort_values("date", inplace=True)
    for col in ("high", "low", "close", "volume"):
        df[col] = 0.0
    df = df[FREQTRADE_COLS]
    df.reset_index(drop=True, inplace=True)
    return df


def _save_feather(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index(drop=True).to_feather(path, compression="lz4", compression_level=9)


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------

def fetch_funding(
    exchange: ccxt.Exchange,
    symbol: str,
    since_ms: int,
    until_ms: int,
) -> list[dict]:
    """Fetch all funding events from ``since_ms`` to ``until_ms`` via windowed calls.

    :param exchange: CCXT apex instance.
    :param symbol: Unified CCXT symbol e.g. ``BTC/USDT:USDT``.
    :param since_ms: Start timestamp in UNIX milliseconds (inclusive).
    :param until_ms: End timestamp in UNIX milliseconds (inclusive).
    :returns: List of raw funding dicts ``{timestamp, fundingRate, ...}``.
    """
    all_rows: list[dict] = []
    # Apex funding cadence is hourly; page an API_MAX-hour window per call.
    window_ms = API_MAX * 3_600_000
    start_ms = since_ms

    while start_ms <= until_ms:
        end_ms = min(start_ms + window_ms, until_ms)
        try:
            batch = exchange.fetch_funding_rate_history(
                symbol,
                since=start_ms,
                limit=API_MAX,
                params={"until": end_ms},
            )
        except ccxt.RateLimitExceeded:
            logger.warning("Rate limited on %s funding — sleeping 10s", symbol)
            time.sleep(10)
            continue
        except ccxt.BaseError as exc:
            logger.warning("API error %s funding: %s", symbol, exc)
            raise

        if batch:
            all_rows.extend(batch)

        start_ms = end_ms + 1
        time.sleep(exchange.rateLimit / 1000.0)

    return all_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Apex DEX funding-rate collector")
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

    # Deterministic strided sharding: shard 0 takes [0, N, 2N, ...].
    pairs = all_pairs[args.shard :: args.total_shards]
    logger.info("Shard %d/%d — %d pairs for funding", args.shard, args.total_shards, len(pairs))

    args.out_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    empty = 0

    for i, pair in enumerate(pairs, 1):
        out_path = _feather_path(args.out_dir, pair)
        logger.info(
            "[%d/%d] %s funding since=%s until=%s",
            i, len(pairs), pair,
            pd.Timestamp(genesis_s, unit="s", tz="UTC").date(),
            pd.Timestamp(args.until, unit="s", tz="UTC").date(),
        )

        rows = fetch_funding(exchange, pair, genesis_s * 1000, args.until * 1000)
        new_df = _rows_to_df(rows)

        if new_df.empty:
            # Not fatal: some markets are too new or expose no funding history.
            logger.warning("  No funding for %s — skipping", pair)
            empty += 1
            continue

        _save_feather(new_df, out_path)
        logger.info(
            "  Saved %d funding rows (%s → %s)",
            len(new_df),
            new_df["date"].iloc[0].date(),
            new_df["date"].iloc[-1].date(),
        )
        saved += 1

    logger.info("Done. saved=%d empty=%d", saved, empty)


if __name__ == "__main__":
    main()
