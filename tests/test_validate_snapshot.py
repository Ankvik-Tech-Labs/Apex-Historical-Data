from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector.validate_snapshot import validate_snapshot


def _write_feather(path: Path, dates: list[int]) -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(dates, unit="ms", utc=True),
            "open": [1.0] * len(dates),
            "high": [1.0] * len(dates),
            "low": [1.0] * len(dates),
            "close": [1.0] * len(dates),
            "volume": [1.0] * len(dates),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_feather(path)


def test_validate_snapshot_accepts_extended_candidate_coverage(tmp_path):
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    filename = "BTC_USDT_USDT-1h-futures.feather"
    _write_feather(baseline / filename, [100, 200])
    _write_feather(candidate / filename, [100, 200, 300])

    assert validate_snapshot(candidate, baseline) == 1


def test_validate_snapshot_rejects_short_candidate_coverage(tmp_path):
    baseline = tmp_path / "baseline"
    candidate = tmp_path / "candidate"
    filename = "BTC_USDT_USDT-1h-futures.feather"
    _write_feather(baseline / filename, [100, 200])
    _write_feather(candidate / filename, [100])

    with pytest.raises(ValueError, match="ends before baseline"):
        validate_snapshot(candidate, baseline)


def test_validate_snapshot_rejects_duplicate_dates(tmp_path):
    candidate = tmp_path / "candidate"
    _write_feather(candidate / "BTC_USDT_USDT-1h-futures.feather", [100, 100])

    with pytest.raises(ValueError, match="duplicate"):
        validate_snapshot(candidate)


def test_validate_snapshot_rejects_unsorted_dates(tmp_path):
    candidate = tmp_path / "candidate"
    _write_feather(candidate / "BTC_USDT_USDT-1h-futures.feather", [200, 100])

    with pytest.raises(ValueError, match="ascending"):
        validate_snapshot(candidate)
