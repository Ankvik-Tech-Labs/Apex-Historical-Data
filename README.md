# Apex DEX Historical Data

Automated collector for Apex DEX (ApeX Omni) historical perpetual swap candles, published as immutable UTC date-tagged GitHub Releases every Sunday.

## Dataset

| Property | Value |
|---|---|
| Exchange | [ApeX Omni](https://omni.apex.exchange/) |
| Markets | 135 USDT-margined perpetual swaps |
| Timeframes | 15m · 1h · 4h · 1d |
| History | ~2024-06-15 to present (varies by asset) |
| Format | Freqtrade feather (`{BASE}_{QUOTE}_{SETTLE}-{tf}-futures.feather`) |
| Update cadence | Weekly (Sunday 06:00 UTC) |

## Download

```bash
# Find dated snapshots
gh release list --repo Ankvik-Tech-Labs/Apex-Historical-Data --limit 100

# Download a specific 1h snapshot for all 135 pairs
SNAPSHOT=2026-07-14
gh release download "$SNAPSHOT" \
  --pattern "apex-ohlcv-1h.zip" \
  --repo Ankvik-Tech-Labs/Apex-Historical-Data

unzip apex-ohlcv-1h.zip -d data/
```

```bash
# All timeframes from one snapshot
SNAPSHOT=2026-07-14
for TF in 15m 1h 4h 1d; do
  gh release download "$SNAPSHOT" --pattern "apex-ohlcv-${TF}.zip" \
    --repo Ankvik-Tech-Labs/Apex-Historical-Data
  unzip apex-ohlcv-${TF}.zip -d data/
done
```

```bash
# Via curl (no gh CLI needed)
curl -L https://github.com/Ankvik-Tech-Labs/Apex-Historical-Data/releases/download/2026-07-14/apex-ohlcv-1d.zip \
  -o apex-ohlcv-1d.zip
```

## Read in Python

```python
import pandas as pd

df = pd.read_feather("BTC_USDT_USDT-1h-futures.feather")
# columns: date (UTC datetime), open, high, low, close, volume
print(df.head())
```

## Use with Freqtrade

Drop the feathers into your `user_data/data/apex/futures/` directory:

```bash
unzip apex-ohlcv-1h.zip -d user_data/data/apex/futures/
```

## Architecture

```
GitHub Actions (weekly)
├── setup      — generates shard matrix, snapshot tag, and UTC cutoff
├── ohlcv      — parallel matrix jobs (8 total)
│   ├── 15m × 4 shards   (most API calls)
│   ├── 1h  × 2 shards
│   ├── 4h  × 1 shard
│   └── 1d  × 1 shard
└── publish    — validate full-history shards → zip per timeframe → immutable dated release
```

Each weekly run fetches the full live Apex API history from `DATA_GENESIS` through one workflow-wide UTC cutoff. The publish step uses the previous dated snapshot only as a validation baseline; it never supplies candle rows for the new release.

### Apex API quirks

The standard CCXT `fetchOHLCV` with `since` is unreliable for Apex — the `v3/klines` endpoint requires **both** `start` and `end` in UNIX seconds, and returns empty when `limit > 200`. The collector uses `publicGetV3Klines` directly with explicit sliding windows of `200 × candle_duration` seconds.

## Manifest

Every release includes `manifest.json` with machine-readable coverage per asset:

```json
{
  "generated": "2026-06-10",
  "snapshot_date": "2026-06-10",
  "base_snapshot": "2026-06-03",
  "exchange": "apex",
  "assets": {
    "BTC_USDT_USDT": {
      "timeframes": {
        "1h": { "first": "2024-06-15", "last": "2026-06-10", "candles": 17520 }
      }
    }
  }
}
```

## Local usage

```bash
pip install -r requirements.txt

# Full-history snapshot through "now" in UTC
python collector/collect_ohlcv.py --timeframe 1h --until "$(date -u +%s)" --out-dir data/

# Shard 0 of 4 (matches CI matrix)
python collector/collect_ohlcv.py --timeframe 15m --shard 0 --total-shards 4 --until "$(date -u +%s)" --out-dir data/
```
