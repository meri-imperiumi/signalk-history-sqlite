# Specification: `signalk-history-sqlite`

## 1. Overview

The `signalk-history-sqlite` plugin replaces heavy external time-series engines with a local, zero-dependency SQLite storage system using Node.js's built-in `node:sqlite` module (`DatabaseSync`). It ingests incoming Signal K deltas, buffers them in memory to protect flash storage lifespan, and implements a Signal K History API provider.

---

## 2. Architecture & File Structure

* **Runtime Dependency:** Node.js built-in `node:sqlite` (Zero external package dependencies for the database layer).
* **Database Path:** Managed persistently inside the plugin data directory (`app.getDataDirPath()`).
* **Journal Mode:** Write-Ahead Logging (`WAL`) enabled for concurrent non-blocking reads and writes.

---

## 3. Database Schema Specification

```sql
-- Metric Dictionary
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
) STRICT;

-- Real-Valued Telemetry (Numbers: speed, depth, temperature, voltage, etc.)
CREATE TABLE IF NOT EXISTS telemetry_real (
    ts_ms INTEGER NOT NULL,
    metric_id INTEGER NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (ts_ms, metric_id)
) STRICT, WITHOUT ROWID;

-- Boolean Telemetry (Switches, binary indicators)
CREATE TABLE IF NOT EXISTS telemetry_bool (
    ts_ms INTEGER NOT NULL,
    metric_id INTEGER NOT NULL,
    value INTEGER NOT NULL, -- 0 or 1
    PRIMARY KEY (ts_ms, metric_id)
) STRICT, WITHOUT ROWID;

```

---

## 4. Ingestion Pipeline

1. **Subscription:** The plugin subscribes to local vessel delta streams.
2. **Buffering:** Incoming values are mapped to their metric dictionary ID and pushed to an in-memory array buffer.
3. **Batch Flushing:** When the buffer reaches **500 entries** (or a 1-second interval timer fires), the array is committed inside a single SQLite transaction (`BEGIN TRANSACTION` ... `COMMIT`) to avoid flash storage `fsync` bottlenecks.

---

## 5. API Provider Specification

* **Integration:** Implements a Signal K History API provider, registering its capabilities with the core server history registry so dashboards and client tools can request historical ranges natively.

See `history.json` for OpenAPI spec.

---

## 6. Implementation Skeleton (`index.js`)

```javascript
const { DatabaseSync } = require('node:sqlite');
const path = require('node:path');
const fs = require('node:fs');

module.exports = function (app) {
  let plugin = {};
  let db;
  let insertStmt;
  let getMetricStmt;
  let insertMetricStmt;
  let batchBuffer = [];
  let flushInterval;

  const metricCache = new Map();

  plugin.id = 'signalk-history-sqlite';
  plugin.name = 'Signal K SQLite History Storage';
  plugin.description = 'Low-overhead, zero-dependency time-series storage using node:sqlite';

  plugin.start = function (options) {
    const dbDir = path.join(app.getDataDirPath(), 'sqlite-history');
    if (!fs.existsSync(dbDir)) {
      fs.mkdirSync(dbDir, { recursive: true });
    }
    const dbPath = path.join(dbDir, 'telemetry.db');

    // Initialize database using built-in node:sqlite
    db = new DatabaseSync(dbPath);
    db.exec('PRAGMA journal_mode = WAL;');
    db.exec('PRAGMA synchronous = NORMAL;');
    db.exec('PRAGMA cache_size = -8000;'); // ~8MB cache limit

    // Create schema
    db.exec(`
      CREATE TABLE IF NOT EXISTS metrics (
          id INTEGER PRIMARY KEY,
          name TEXT UNIQUE NOT NULL
      ) STRICT;

      CREATE TABLE IF NOT EXISTS telemetry_real (
          ts_ms INTEGER NOT NULL,
          metric_id INTEGER NOT NULL,
          value REAL NOT NULL,
          PRIMARY KEY (ts_ms, metric_id)
      ) STRICT, WITHOUT ROWID;
    `);

    // Prepare statements
    insertStmt = db.prepare('INSERT OR IGNORE INTO telemetry_real (ts_ms, metric_id, value) VALUES (?, ?, ?)');
    getMetricStmt = db.prepare('SELECT id FROM metrics WHERE name = ?');
    insertMetricStmt = db.prepare('INSERT INTO metrics (name) VALUES (?) RETURNING id');

    // Set up batch flush interval (every 1 second)
    flushInterval = setInterval(flushBuffer, 1000);

    // Register Signal K subscription stream
    const localSubscription = {
      context: 'vessels.self',
      subscribe: [{ path: '*', period: 1000 }]
    };

    app.subscriptionmanager.subscribe(
      localSubscription,
      [],
      (subscriptionError) => {
        app.error(`Error subscribing: ${subscriptionError}`);
      },
      (delta) => {
        handleDelta(delta, options);
      }
    );

    // Register as a Signal K History API provider interface
    if (app.registerHistoryProvider) {
      app.registerHistoryProvider({
        id: plugin.id,
        name: plugin.name,
        getValues: async (query) => {
          return handleHistoryQuery(query);
        }
      });
    }

    app.debug('signalk-history-sqlite plugin started successfully.');
  };

  function getMetricId(name) {
    if (metricCache.has(name)) return metricCache.get(name);
    
    let row = getMetricStmt.get(name);
    if (!row) {
      row = insertMetricStmt.get(name);
    }
    metricCache.set(name, row.id);
    return row.id;
  }

  function handleDelta(delta) {
    if (!delta.updates) return;
    const now = Date.now();

    for (const update of delta.updates) {
      if (!update.values) continue;
      for (const valObj of update.values) {
        if (typeof valObj.value === 'number') {
          const metricId = getMetricId(valObj.path);
          batchBuffer.push({ ts: now, id: metricId, val: valObj.value });
          
          if (batchBuffer.length >= 500) {
            flushBuffer();
          }
        }
      }
    }
  }

  const executeBatchTransaction = db.transaction((buffer) => {
    for (const item of buffer) {
      insertStmt.run(item.ts, item.id, item.val);
    }
  });

  function flushBuffer() {
    if (batchBuffer.length === 0) return;
    const chunk = batchBuffer;
    batchBuffer = [];
    try {
      executeBatchTransaction(chunk);
    } catch (err) {
      app.error(`Failed to flush batch to SQLite: ${err.message}`);
    }
  }

  function handleHistoryQuery(query) {
    // Standard History API query handler mapping
    const paths = query.paths || [];
    const fromMs = new Date(query.from).getTime();
    const toMs = query.to ? new Date(query.to).getTime() : Date.now();

    const results = [];
    const queryStmt = db.prepare(`
      SELECT t.ts_ms AS timestamp, t.value 
      FROM telemetry_real t
      JOIN metrics m ON t.metric_id = m.id
      WHERE m.name = ? AND t.ts_ms >= ? AND t.ts_ms <= ?
      ORDER BY t.ts_ms ASC
    `);

    for (const pathSpec of paths) {
      const pathName = typeof pathSpec === 'string' ? pathSpec : pathSpec.path;
      const rows = queryStmt.all(pathName, fromMs, toMs);
      
      results.push({
        path: pathName,
        values: rows.map(r => [new Date(r.timestamp).toISOString(), r.value])
      });
    }

    return {
      context: query.context || 'vessels.self',
      range: {
        from: new Date(fromMs).toISOString(),
        to: new Date(toMs).toISOString()
      },
      data: results
    };
  }

  plugin.stop = function () {
    if (flushInterval) clearInterval(flushInterval);
    flushBuffer();
    if (db) {
      db.close();
    }
    app.debug('signalk-history-sqlite plugin stopped.');
  };

  return plugin;
};

```
