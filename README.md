# Signal K SQLite History API Provider

This plugin provides lightweight, dependency-free Signal K History API implementation for systems where installing InfluxDB or other big time series systems is not feasible. The plugin uses `node:sqlite` bundled in Node.js 22+.

## Features

- **Zero external dependencies** for database operations - uses Node.js's built-in `node:sqlite`
- **Write-Ahead Logging (WAL)** for concurrent non-blocking reads and writes
- **Batch writing** to protect flash storage lifespan
- **Configurable allowlist/denylist** for filtering which paths to store
- **Multi-context support** - can store data from other vessels, ATONs, and SAR aircraft

## Database Schema

The plugin creates the following tables in a database file inside Signal K plugin data directory (usually `~/.signalk/plugin-config-data/signalk-history-sqlite/sqlite-history/`):

- `metrics`: Dictionary mapping metric names to IDs
- `telemetry_real`: Real-valued telemetry (numbers)
- `telemetry_bool`: Boolean telemetry (switches, binary indicators)
- `telemetry_string`: String telemetry (status flags, mode names, enum values)
- `telemetry_json`: Reserved for future use (not currently utilized)

## History API

This plugin implements the Signal K History API, allowing clients to query historical data:

### Get Values

```
GET /signalk/v2/api/history/values?paths=navigation.speedOverGround,navigation.position&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z
```

Supports aggregation methods: `average`, `min`, `max`, `first`, `last`, `sma`, `ema`

```
GET /signalk/v2/api/history/values?paths=navigation.speedOverGround:sma:5&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z
```

### Get Contexts

```
GET /signalk/v2/api/history/contexts?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z
```

### Get Paths

```
GET /signalk/v2/api/history/paths?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z
```

## Storage Requirements

Storage usage depends on the number of metrics, data frequency, and retention period. With the default 1-second resolution, rough estimates are:

- 1 metric = ~25MB/month
- 100 metrics = ~2.5GB/month

For higher-frequency sampling, storage scales linearly with the sample rate.

## License

EUPL-1.2
