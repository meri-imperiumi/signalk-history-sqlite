# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Plugin status is no longer recomputed with full-table `COUNT(*)` queries
  and a `UNION ALL` time-range scan on every batch flush. Those queries are
  O(database size) and synchronous, blocking the Signal K event loop for
  hundreds of milliseconds per second on grown databases — the cause of
  reported slowdowns at 100+ messages/second. Counts and the time range are
  now tracked in memory (seeded once at start via index seeks) and the flush
  path performs no database-wide scans.
- The allow/deny path list is precomputed into a `Set` at plugin start
  instead of rebuilding a lookup object for every ingested value.
- Deltas arriving after the plugin has stopped are ignored instead of
  triggering failed writes against the closed database.

### Changed
- The batch buffer is capped at four times the configured batch size: if
  flushing ever becomes asynchronous and cannot keep up, the oldest values
  are dropped instead of growing memory without bound.

### Added
- Perf smoketest (`test/perf-smoke.test.js`) guarding the flush path
  against O(database) work: it ingests 2000 values into a 400k-row database
  and asserts the average flush stays under 10 ms (the regressed behavior
  measured ~88 ms/flush). Overridable via `SQLITE_HISTORY_FLUSH_BUDGET_MS`
  on very slow devices.

## [0.3.1] - 2026-08-27

### Fixed
- History API `values` requests no longer fail with "Provided value cannot
  be bound to SQLite parameter 8" when requesting `first`/`last`
  aggregation for a path that has never been recorded. Paths without a
  metrics row are skipped and return no data instead of binding an
  `undefined` metric id.

## [0.3.0] - 2026-08-22

### Added
- The History API now serves textual, boolean and JSON object values, not
  just numbers. Non-numeric paths (e.g. `autopilot.mode`,
  `electrical.batteries.0.charging`, notification objects) are read from the
  `telemetry_string`, `telemetry_bool` and `telemetry_json` tables. Only
  `first`/`last` aggregation is meaningful for non-numeric values, so any
  other requested method is coerced to `first`, matching the
  signalk-to-influxdb provider.
- Object (non-position) values are now stored by the writer as JSON-encoded
  text in the `telemetry_json` table; previously they were silently dropped.
- `getContexts` and `getPaths` now report contexts and paths across all
  telemetry tables (real, bool, string and json).

### Fixed
- Time-bucket alignment now uses integer division, so values that fall in the
  same resolution bucket are no longer split into separate buckets when the
  bucket size is bound as a parameter.
- Numeric `first`/`last` aggregation is now chronological (by timestamp within
  each time bucket) rather than value-based `MIN`/`MAX`, matching the
  signalk-to-influxdb behaviour.

## [0.2.0] - 2026-08-20

### Fixed
- Resolution is now actually applied instead of relying Signal K subscription to handle it

### Added
- Screenshot of the plugin config screen

## [0.1.0] - 2026-08-20

### Added
- Initial implementation of SQLite-based Signal K history storage
