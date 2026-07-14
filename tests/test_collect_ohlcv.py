from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector.collect_ohlcv import _rows_to_df, fetch_ohlcv


class Exchange:
    rateLimit = 0

    def __init__(self, replies):
        self.replies, self.calls = iter(replies), []

    def publicGetV3Klines(self, params):
        self.calls.append(params)
        reply = next(self.replies)
        if isinstance(reply, Exception):
            raise reply
        return reply


def test_fetch_stops_at_explicit_cutoff(monkeypatch):
    monkeypatch.setattr("collector.collect_ohlcv.time.sleep", lambda _: None)
    exchange = Exchange([{"data": {"BTCUSDT": []}}, {"data": {"BTCUSDT": []}}])
    fetch_ohlcv(exchange, "BTCUSDT", "1", 1, 0, 400)
    assert exchange.calls[-1]["end"] == "400"


def test_fetch_propagates_api_error(monkeypatch):
    monkeypatch.setattr("collector.collect_ohlcv.time.sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="unavailable"):
        fetch_ohlcv(Exchange([RuntimeError("unavailable")]), "BTCUSDT", "1", 60, 0, 60)


def test_rows_to_df_deduplicates_and_sorts_by_date():
    df = _rows_to_df(
        [
            {"t": 2000, "o": 2, "h": 2, "l": 2, "c": 2, "v": 2},
            {"t": 1000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1},
            {"t": 2000, "o": 3, "h": 3, "l": 3, "c": 3, "v": 3},
        ]
    )

    assert list(df["date"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")) == [
        "1970-01-01 00:00:01",
        "1970-01-01 00:00:02",
    ]
    assert len(df) == 2
    assert df.iloc[-1][["open", "high", "low", "close", "volume"]].tolist() == [3.0, 3.0, 3.0, 3.0, 3.0]
