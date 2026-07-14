# Immutable Weekly Apex OHLCV Snapshots Design

## Goal

Publish one immutable, date-tagged GitHub Release per weekly collection run. Every
release must contain a complete, cumulative Apex OHLCV history for every supported
timeframe, so a consumer can obtain the full available history from a single
release download.

## Scope

- Replace the rolling `latest` release with immutable UTC date tags in `YYYY-MM-DD`
  format.
- Re-fetch Apex's entire available OHLCV history from `DATA_GENESIS` on every
  weekly run for every active perpetual market and supported timeframe.
- Use the newest prior dated snapshot only as a validation baseline; never use it
  as the source of candles in a newly published snapshot.
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
2. Before collection, the workflow identifies the newest existing release whose
   tag exactly matches `YYYY-MM-DD`, ordered by release creation date. It records
   that tag as `base_snapshot`; its assets are downloaded only for validation.
3. Every matrix job invokes the collector without `--prior-data-dir`. For every
   assigned pair, the collector requests the full time range from `DATA_GENESIS`
   through the run's fixed UTC collection cutoff, using API windows no larger than
   200 candles.
4. The collector deduplicates by UTC timestamp, sorts ascending, and writes the
   complete API-returned history to its shard artifact. It does not merge or copy
   candle rows from a prior release.
5. A fetch that returns no valid candles for an active pair is a collection
   failure, not a reason to carry forward stale data. Any failed matrix job blocks
   publication.
6. The publish job downloads all shard artifacts, packages each complete timeframe
   set, generates metadata, verifies coverage against the baseline when present,
   and creates the new date-tagged release.

## Collector Contract

Each scheduled collection uses a single UTC cutoff timestamp established before
the matrix starts. The collector always writes outputs to `--out-dir` and makes no
use of `--prior-data-dir` during the scheduled full-history workflow.

The initial request timestamp is:

`DATA_GENESIS`

The final request ends at the fixed collection cutoff. The collector de-duplicates
raw API rows by UTC timestamp and sorts the complete result ascending.

The output must satisfy these invariants:

- It has no duplicate timestamps and timestamps are strictly ascending.
- It contains data only fetched from the live Apex API during the current run.
- When a baseline file exists for the same pair and timeframe, its first timestamp
  is no later than the baseline first timestamp and its final timestamp is no
  earlier than the baseline final timestamp.

## Publication Validation and Metadata

The publish job downloads the selected baseline into a separate directory for
validation. Before creating a release, a validation command compares every
baseline feather to its candidate feather and fails if the candidate omits a
baseline file for an active pair, starts later, ends earlier, or has
unsorted/duplicate timestamps.
The validator also fails when no output exists for any configured timeframe.

`manifest.json` gains `snapshot_date` and `base_snapshot` fields. Per-file
coverage continues to report first date, last date, candle count, and filename.
Release notes state the snapshot date, base snapshot (or `none` for bootstrap),
and that the release is immutable and contains full cumulative history.

## Failure Handling

- No existing dated release: collect and publish the full live API history from
  `DATA_GENESIS`.
- Baseline download missing one timeframe ZIP: fail validation rather than
  silently publishing a snapshot whose continuity cannot be checked.
- Apex request failure or an active pair with no valid fetched candles: retry
  rate-limit failures as now, then fail the matrix job. Do not copy prior data
  into a current live-API snapshot.
- Existing target tag: fail without changing any release.
- Validation failure: fail before `gh release create`; all historical releases
  remain untouched.

## Testing

Unit tests will cover full-history request boundaries, fixed collection cutoff,
window pagination, duplicate removal, empty-response failure, and output
invariants. Tests will use mocked Apex responses; no live network requests are
required.

Workflow-oriented tests will exercise tag selection from mocked GitHub CLI JSON,
reject duplicate target tags, reject a missing baseline asset, and verify the
workflow contains no release deletion or asset replacement command. The release
metadata generator will be tested for `snapshot_date` and `base_snapshot`.

## Non-Goals

- Backfilling data earlier than the Apex API exposes.
- Supporting intraday releases or a mutable `latest` alias.
- Changing data format, timeframes, or replacing GitHub Releases with object
  storage.
