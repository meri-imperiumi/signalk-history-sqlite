# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
