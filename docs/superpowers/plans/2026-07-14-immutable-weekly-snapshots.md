# Immutable Weekly Apex OHLCV Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish immutable UTC date-tagged weekly releases with freshly fetched full Apex OHLCV history.

**Architecture:** Matrix jobs request all active swap candles from `DATA_GENESIS` through one workflow-wide UTC cutoff. The newest dated release is downloaded only in the publish job for validation; it never supplies output candle rows. A date-tagged release is created only after collection and validation succeed.

**Tech Stack:** Python 3.12, pandas, pyarrow, ccxt, pytest, GitHub Actions, GitHub CLI.

---

## File Structure

- Modify: `collector/collect_ohlcv.py` - full-history collection, fixed cutoff, and fail-closed errors.
- Create: `collector/validate_snapshot.py` - candidate and baseline validation.
- Modify: `collector/make_manifest.py` - snapshot provenance.
- Modify: `.github/workflows/collect.yml` - immutable release lifecycle.
- Modify: `README.md` and `requirements.txt`.
- Create: `tests/test_collect_ohlcv.py`, `tests/test_validate_snapshot.py`, `tests/test_make_manifest.py`, and `tests/test_workflow.py`.

### Task 1: Collect Full History

**Files:**
- Modify: `requirements.txt`
- Modify: `collector/collect_ohlcv.py:84-279`
- Create: `tests/test_collect_ohlcv.py`

- [ ] **Step 1: Write failing fixed-cutoff and error tests**

```python
import pytest
from collector.collect_ohlcv import fetch_ohlcv

class Exchange:
    rateLimit = 0
    def __init__(self, replies): self.replies, self.calls = iter(replies), []
    def publicGetV3Klines(self, params):
        self.calls.append(params)
        reply = next(self.replies)
        if isinstance(reply, Exception): raise reply
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
```

- [ ] **Step 2: Verify red state**

Run: `pytest tests/test_collect_ohlcv.py -v`

Expected: FAIL because `fetch_ohlcv` lacks an `until_s` argument and suppresses non-rate-limit errors.

- [ ] **Step 3: Implement full-range fetches**

Append `pytest>=8.0` to `requirements.txt`. Remove `_load_prior_latest_ts`, `--prior-data-dir`, and all dataframe merge branches from `collector/collect_ohlcv.py`. Change the function signature to `fetch_ohlcv(exchange, api_sym, apex_interval, candle_secs, since_s, until_s)`. Loop while `start_s <= until_s`, use `end_s = min(start_s + API_MAX * candle_secs, until_s)`, and re-raise non-rate-limit `ccxt.BaseError` exceptions after logging.

Add required `--until` as Unix UTC seconds; reject values before `DATA_GENESIS`. Always pass `DATA_GENESIS` and `args.until` to each fetch. If any active market returns no rows, raise `RuntimeError` and make the matrix job fail. Keep `_rows_to_df` as the sole deduplication and sort operation.

- [ ] **Step 4: Add sorted-unique output test and verify green state**

Add a test passing timestamps `[2000, 1000, 2000]` to `_rows_to_df`; assert it returns ascending UTC dates and two rows, with the final duplicate row winning. Run `pytest tests/test_collect_ohlcv.py -v`; expected PASS.

- [ ] **Step 5: Commit**

Run: `git add requirements.txt collector/collect_ohlcv.py tests/test_collect_ohlcv.py && git commit -m "feat: collect full Apex history per snapshot"`

### Task 2: Validate Candidates Before Publishing

**Files:**
- Create: `collector/validate_snapshot.py`
- Create: `tests/test_validate_snapshot.py`

- [ ] **Step 1: Write failing coverage tests**

Create temporary baseline and candidate directories containing `BTC_USDT_USDT-1h-futures.feather`. Test that candidate dates `[100, 200, 300]` pass against baseline `[100, 200]`, while candidate `[100]` raises `ValueError` matching `ends before baseline`.

- [ ] **Step 2: Verify red state**

Run: `pytest tests/test_validate_snapshot.py -v`

Expected: FAIL with missing `collector.validate_snapshot`.

- [ ] **Step 3: Implement validator**

Create `read_dates(path)` to read Feather, normalize the first column to UTC, and reject empty, duplicate, or non-ascending timestamps. Implement `validate_snapshot(candidate_dir, baseline_dir=None)` to first validate every candidate, then for each baseline feather require the same candidate filename, candidate first timestamp no later than baseline first timestamp, and candidate final timestamp no earlier than baseline final timestamp. Raise descriptive `ValueError` messages for each failure.

Add CLI arguments `--candidate-dir` and optional `--baseline-dir`; print the validated file count and exit non-zero for validation errors.

- [ ] **Step 4: Add shape tests and verify green state**

Add candidate-only tests for duplicate `[100, 100]` and unsorted `[200, 100]` timestamps. Run `pytest tests/test_validate_snapshot.py -v`; expected PASS.

- [ ] **Step 5: Commit**

Run: `git add collector/validate_snapshot.py tests/test_validate_snapshot.py && git commit -m "feat: validate immutable snapshot coverage"`

### Task 3: Add Snapshot Metadata

**Files:**
- Modify: `collector/make_manifest.py:40-101`
- Create: `tests/test_make_manifest.py`

- [ ] **Step 1: Write failing provenance test**

Run `make_manifest.main()` against one temporary feather with `--snapshot-date 2026-07-19 --base-snapshot 2026-07-12`. Assert JSON contains those values and release notes include `Snapshot: 2026-07-19`, `Base snapshot: 2026-07-12`, and `immutable`.

- [ ] **Step 2: Verify red state**

Run: `pytest tests/test_make_manifest.py -v`

Expected: FAIL because the CLI does not accept these arguments.

- [ ] **Step 3: Implement metadata contract**

Add required `--snapshot-date` and optional `--base-snapshot`. Require `snapshot_date` to be an exact normalized `YYYY-MM-DD` UTC date; normalize an empty baseline argument to `None`. Add `snapshot_date` and `base_snapshot` to `manifest.json`. Release notes must label the snapshot and baseline and state the assets are an immutable, full live-API history.

- [ ] **Step 4: Verify green state and commit**

Run: `pytest tests/test_make_manifest.py -v`

Expected: PASS.

Run: `git add collector/make_manifest.py tests/test_make_manifest.py && git commit -m "feat: record snapshot release provenance"`

### Task 4: Publish Immutable Date Releases

**Files:**
- Modify: `.github/workflows/collect.yml:1-170`
- Create: `tests/test_workflow.py`

- [ ] **Step 1: Write failing static policy tests**

Assert the workflow text does not contain `gh release delete`, `refs/tags/latest`, or `--prior-data-dir`; it must contain `--until ${{ needs.setup.outputs.collection_cutoff }}`, `gh release view "$SNAPSHOT_TAG"`, and `gh release create "${{ needs.setup.outputs.snapshot_tag }}"`.

- [ ] **Step 2: Verify red state**

Run: `pytest tests/test_workflow.py -v`

Expected: FAIL because current workflow downloads, deletes, and recreates `latest`.

- [ ] **Step 3: Set immutable run outputs in setup**

Remove `workflow_dispatch.inputs.full_history`. Add setup outputs `snapshot_tag`, `collection_cutoff`, and `base_snapshot`. In setup, calculate `SNAPSHOT_TAG=$(date -u +%F)` and `COLLECTION_CUTOFF=$(date -u +%s)`, then fail if `gh release view "$SNAPSHOT_TAG" --repo "$GITHUB_REPOSITORY"` succeeds. Select the newest existing release whose tag matches `^[0-9]{4}-[0-9]{2}-[0-9]{2}$` using `gh release list --json tagName,createdAt`, and write all values to `$GITHUB_OUTPUT` with the existing matrix.

- [ ] **Step 4: Collect fresh shards and validate them**

Delete the matrix prior-release step. Call the collector with `--until ${{ needs.setup.outputs.collection_cutoff }}` and no prior-data option. In `publish`, when `base_snapshot` is nonempty, download every timeframe ZIP from that tag into `baseline/`, fail if any asset is missing, and unpack it. After downloading merged artifacts, run `python collector/validate_snapshot.py --candidate-dir merged --baseline-dir baseline` when a baseline exists, otherwise run it without `--baseline-dir`.

- [ ] **Step 5: Package and release safely**

For each configured timeframe, fail if its feather glob is empty before `zip -j`; remove `|| true`. Invoke manifest generation with the setup outputs. Delete the old deletion step. Create only `"${{ needs.setup.outputs.snapshot_tag }}"` with `gh release create`, `--notes-file release_assets/release_notes.md`, all four ZIPs, manifest, and release notes.

- [ ] **Step 6: Verify and commit**

Run: `pytest tests/test_workflow.py -v && pytest -v && rg -n 'gh release delete|refs/tags/latest|prior-data-dir' .github/workflows/collect.yml`

Expected: all tests PASS and ripgrep returns no matches.

Run: `git add .github/workflows/collect.yml tests/test_workflow.py && git commit -m "feat: publish immutable dated Apex snapshots"`

### Task 5: Document One-Release Full-History Downloads

**Files:**
- Modify: `README.md:1-110`

- [ ] **Step 1: Replace mutable-release instructions**

Document a `SNAPSHOT=YYYY-MM-DD` variable and `gh release download "$SNAPSHOT" --pattern "apex-ohlcv-1h.zip"`. Add `gh release list --repo Ankvik-Tech-Labs/Apex-Historical-Data --limit 100` to discover snapshots. State explicitly that every dated release was freshly collected from the complete live Apex API history and can be downloaded independently; users never combine weekly deltas.

- [ ] **Step 2: Update local usage and architecture**

Replace the incremental local command with `python collector/collect_ohlcv.py --timeframe 1h --until "$(date -u +%s)" --out-dir data/`. Update architecture prose to describe fresh full-history matrix collection, validation-only baseline download, and immutable dated release creation.

- [ ] **Step 3: Final verification and commit**

Run: `rg -n 'rolling|latest|prior-data-dir|incremental' README.md && pytest -v && git diff --check`

Expected: only an intentional statement that no mutable `latest` release exists may match; tests PASS and whitespace check is clean.

Run: `git add README.md && git commit -m "docs: explain dated full-history snapshots"`
