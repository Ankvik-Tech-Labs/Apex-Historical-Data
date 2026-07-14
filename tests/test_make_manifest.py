from pathlib import Path
import json
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector import make_manifest


def _write_feather(path: Path) -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime([1000, 2000], unit="ms", utc=True),
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "volume": [1.0, 2.0],
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_feather(path)


def test_make_manifest_records_snapshot_provenance(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "out"
    _write_feather(data_dir / "BTC_USDT_USDT-1h-futures.feather")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "make_manifest.py",
            "--data-dir",
            str(data_dir),
            "--out-dir",
            str(out_dir),
            "--snapshot-date",
            "2026-07-19",
            "--base-snapshot",
            "2026-07-12",
        ],
    )

    make_manifest.main()

    manifest = json.loads((out_dir / "manifest.json").read_text())
    notes = (out_dir / "release_notes.md").read_text()

    assert manifest["snapshot_date"] == "2026-07-19"
    assert manifest["base_snapshot"] == "2026-07-12"
    assert "Snapshot: 2026-07-19" in notes
    assert "Base snapshot: 2026-07-12" in notes
    assert "immutable" in notes
