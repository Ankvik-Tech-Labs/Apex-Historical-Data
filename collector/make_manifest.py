#!/usr/bin/env python3
"""Generate manifest.json and release_notes.md from collected feather files.

Usage::

    python make_manifest.py --data-dir data --out-dir .
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd


FREQTRADE_COLS = ["date", "open", "high", "low", "close", "volume"]


def read_coverage(path: Path) -> tuple[str, str, int] | None:
    """Return (first_date, last_date, n_candles) for a feather file."""
    try:
        df = pd.read_feather(path)
        if df.empty:
            return None
        df.columns = FREQTRADE_COLS
        df["date"] = pd.to_datetime(df["date"], utc=True)
        return (
            str(df["date"].iloc[0].date()),
            str(df["date"].iloc[-1].date()),
            len(df),
        )
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--out-dir", type=Path, default=Path("."))
    args = ap.parse_args()

    feathers = sorted(args.data_dir.glob("*-futures.feather"))
    if not feathers:
        print("No feather files found.", file=sys.stderr)
        sys.exit(1)

    # Group by asset
    assets: dict[str, dict] = defaultdict(lambda: {"timeframes": {}})
    for f in feathers:
        parts = f.stem.split("-")  # e.g. BTC_USDT_USDT-1h-futures
        if len(parts) < 3:
            continue
        pair_slug = parts[0]            # BTC_USDT_USDT
        timeframe = parts[1]            # 1h
        cov = read_coverage(f)
        if cov is None:
            continue
        first, last, n = cov
        assets[pair_slug]["timeframes"][timeframe] = {
            "first": first,
            "last": last,
            "candles": n,
            "file": f.name,
        }

    manifest = {
        "generated": str(pd.Timestamp("now", tz="UTC").date()),
        "exchange": "apex",
        "assets": dict(sorted(assets.items())),
    }

    out_manifest = args.out_dir / "manifest.json"
    out_manifest.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote {out_manifest}")

    # Release notes markdown table
    lines = [
        "# Apex DEX Historical Data",
        "",
        f"**Updated:** {manifest['generated']}  ",
        f"**Assets:** {len(assets)}  ",
        f"**Files:** {len(feathers)}",
        "",
        "## Coverage",
        "",
        "| Asset | Timeframe | From | To | Candles |",
        "|---|---|---|---|---:|",
    ]
    for slug, info in sorted(assets.items()):
        for tf, cov in sorted(info["timeframes"].items()):
            lines.append(
                f"| {slug} | {tf} | {cov['first']} | {cov['last']} | {cov['candles']:,} |"
            )

    out_notes = args.out_dir / "release_notes.md"
    out_notes.write_text("\n".join(lines) + "\n")
    print(f"Wrote {out_notes}")


if __name__ == "__main__":
    main()
