# Immutable Weekly Apex OHLCV Snapshots Design

## Goal

Publish one immutable, date-tagged GitHub Release per weekly collection run. Every
release must contain a complete, cumulative Apex OHLCV history for every supported
timeframe, so a consumer can obtain the full available history from a single
release download.

## Scope

- Replace the rolling `latest` release with immutable UTC date tags in `YYYY-MM-DD`
  format.
- Build each snapshot from the newest prior dated snapshot plus newly fetched live
  Apex API candles.
- Preserve prior data when the API returns no data for a pair or a shard fails to
  fetch a pair.
- Validate that every published feather file is sorted, timestamp-unique, and is
  not shorter than its baseline file.
- Update the manifest, release notes, workflow documentation, and download
  instructions for date-tagged full snapshots.

The existing four timeframes (`15m`, `1h`, `4h`, `1d`), sharding scheme, feather
format, and Sunday 06:00 UTC schedule remain unchanged.

## Release Model

Each scheduled run calculates the snapshot tag from the current UTC date, for
example `2026-07-19`. It creates a GitHub Release and Git tag with that name.
Existing dated releases are immutable: the workflow must not delete, edit, or
replace their assets. A run whose target date tag already exists must fail before
collection or publication, requiring an operator to choose a new explicit
snapshot date for any rerun.

Each release contains:

- `apex-ohlcv-15m.zip`
- `apex-ohlcv-1h.zip`
- `apex-ohlcv-4h.zip`
- `apex-ohlcv-1d.zip`
- `manifest.json`
- `release_notes.md`

Every timeframe ZIP contains the complete accumulated set of pair feather files,
not only that week’s delta. There is no `latest` tag or mutable release.

## Data Flow

1. The setup job determines the release tag once and exposes it to all jobs.
2. Before matrix collection, the workflow identifies the newest existing release
   whose tag exactly matches `YYYY-MM-DD`, ordered by release creation date. It
   records that tag as `base_snapshot`; absence of one means bootstrap from Apex
   history genesis.
3. Each matrix job downloads and unpacks that baseline release’s ZIP for its
   assigned timeframe. The job passes the unpacked directory to the collector as
   `--prior-data-dir`.
4. For every assigned pair, the collector loads the baseline feather if it exists.
   It fetches candles beginning one candle before the baseline’s latest timestamp
   to include the last potentially revised or previously open candle. It combines
   baseline and fetched rows by UTC timestamp, with fetched data winning only for
   the overlap, then writes the complete sorted result to its shard artifact.
5. If a pair has baseline data but the fetch returns no rows or fails, the
   collector writes the unchanged baseline file to its shard artifact and records
   the condition as a warning. A pair without baseline data and without fetched
   data is skipped.
6. The publish job downloads all shard artifacts, packages each complete timeframe
   set, generates metadata, verifies invariants against the baseline, and creates
   the new date-tagged release.

## Collector Contract

`--prior-data-dir` is a read-only baseline. The collector always writes outputs to
`--out-dir`; it never relies on matching output paths already existing. For a pair
with a baseline file, the collector must load that exact file from
`prior_data_dir / out_path.name` before collecting.

The initial request timestamp is:

`max(DATA_GENESIS, latest_baseline_timestamp - candle_duration)`

This intentional one-candle overlap provides a bounded correction window without
rewriting earlier history. After concatenation, duplicates are resolved by UTC
timestamp with the fetched row taking precedence, then rows are sorted ascending.

The output must satisfy these invariants for any pair with a baseline:

- Its first timestamp equals the baseline first timestamp.
- Its final timestamp is greater than or equal to the baseline final timestamp.
- It has no duplicate timestamps and timestamps are strictly ascending.
- It includes every baseline timestamp except timestamps in the one-candle overlap
  that are intentionally replaced by fetched values.

## Publication Validation and Metadata

The publish job downloads the selected baseline into a separate directory for
validation. Before creating a release, a validation command compares every
baseline feather to its candidate feather and fails if the candidate omits a
baseline file, starts later, ends earlier, or has unsorted/duplicate timestamps.
The validator also fails when no output exists for any configured timeframe.

`manifest.json` gains `snapshot_date` and `base_snapshot` fields. Per-file
coverage continues to report first date, last date, candle count, and filename.
Release notes state the snapshot date, base snapshot (or `none` for bootstrap),
and that the release is immutable and contains full cumulative history.

## Failure Handling

- No existing dated release: bootstrap by fetching full history from `DATA_GENESIS`.
- Baseline download missing one timeframe ZIP: fail the workflow rather than
  silently constructing a partial history.
- Apex request failure: retry rate-limit failures as now; for other API failures,
  retain that pair’s baseline and emit a warning. Bootstrap pairs with no baseline
  remain absent and must be reported in the manifest/release notes.
- Existing target tag: fail without changing any release.
- Validation failure: fail before `gh release create`; all historical releases
  remain untouched.

## Testing

Unit tests will cover baseline path loading, one-candle overlap selection,
baseline-plus-delta merging, no-data fallback, and preservation invariants. Tests
will use generated feather fixtures and mocked Apex responses; no live network
requests are required.

Workflow-oriented tests will exercise tag selection from mocked GitHub CLI JSON,
reject duplicate target tags, reject a missing baseline asset, and verify the
workflow contains no release deletion or asset replacement command. The release
metadata generator will be tested for `snapshot_date` and `base_snapshot`.

## Non-Goals

- Backfilling data earlier than the Apex API exposes.
- Supporting intraday releases or a mutable `latest` alias.
- Changing data format, timeframes, or replacing GitHub Releases with object
  storage.
