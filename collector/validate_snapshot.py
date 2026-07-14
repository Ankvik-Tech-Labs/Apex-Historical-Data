#!/usr/bin/env python3
"""Validate that a candidate snapshot is well-formed and covers any baseline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def read_dates(path: Path) -> pd.Series:
    """Read and validate the date column from a feather file."""
    df = pd.read_feather(path)
    if df.empty:
        raise ValueError(f"{path.name} is empty")

    date_col = df.columns[0]
    dates = pd.to_datetime(df[date_col], utc=True)
    if dates.empty:
        raise ValueError(f"{path.name} has no timestamps")
    if dates.duplicated().any():
        raise ValueError(f"{path.name} contains duplicate timestamps")
    if not dates.is_monotonic_increasing:
        raise ValueError(f"{path.name} timestamps must be in ascending order")
    return dates


def validate_snapshot(candidate_dir: Path, baseline_dir: Path | None = None) -> int:
    """Validate a candidate snapshot directory against an optional baseline."""
    candidate_files = sorted(candidate_dir.glob("*-futures.feather"))
    if not candidate_files:
        raise ValueError(f"No feather files found in {candidate_dir}")

    candidate_dates: dict[str, pd.Series] = {}
    for path in candidate_files:
        candidate_dates[path.name] = read_dates(path)

    if baseline_dir is not None:
        baseline_files = sorted(baseline_dir.glob("*-futures.feather"))
        if not baseline_files:
            raise ValueError(f"No feather files found in baseline {baseline_dir}")

        for baseline_path in baseline_files:
            candidate_path = candidate_dir / baseline_path.name
            if not candidate_path.exists():
                raise ValueError(f"Missing candidate file for baseline {baseline_path.name}")

            baseline_dates = read_dates(baseline_path)
            candidate_dates_for_file = candidate_dates[baseline_path.name]

            if candidate_dates_for_file.iloc[0] > baseline_dates.iloc[0]:
                raise ValueError(f"{baseline_path.name} starts after baseline")
            if candidate_dates_for_file.iloc[-1] < baseline_dates.iloc[-1]:
                raise ValueError(f"{baseline_path.name} ends before baseline")

    return len(candidate_files)


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate an Apex OHLCV snapshot")
    ap.add_argument("--candidate-dir", type=Path, required=True)
    ap.add_argument("--baseline-dir", type=Path, default=None)
    args = ap.parse_args()

    try:
        count = validate_snapshot(args.candidate_dir, args.baseline_dir)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    print(f"Validated {count} feather files")


if __name__ == "__main__":
    main()
