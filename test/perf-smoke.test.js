const { test } = require("node:test");
const assert = require("node:assert");
const path = require("node:path");
const fs = require("node:fs");
const os = require("node:os");
const { DatabaseSync } = require("node:sqlite");

// Perf smoketest: the flush path must stay O(batch size), never
// O(database size). See perf-plan.md for the incident this guards against:
// flushBuffer() used to run five COUNT(*) queries and a UNION ALL
// time-range scan on every flush, blocking the Signal K event loop for
// ~190 ms per flush with 200k rows stored (~848 ms at a million rows).
//
// Budget rationale (measured on a low-power ARM SBC): the regressed code
// costs ~0.18 ms per 1000 stored rows per flush (~73 ms/flush at 400k rows
// here, still ~25-35 ms on fast CI hardware), while the fixed flush path
// costs ~1 ms/flush. A 10 ms/flush average fails for the regressed code on
// any runner while leaving the fixed code an order of magnitude of headroom.
// Override with SQLITE_HISTORY_FLUSH_BUDGET_MS on very slow devices.
const PREPOPULATED_ROWS = 400_000;
const INGESTED_DELTAS = 2000;
const BATCH_SIZE = 100; // => INGESTED_DELTAS / BATCH_SIZE flushes
const FLUSH_BUDGET_MS =
  Number(process.env.SQLITE_HISTORY_FLUSH_BUDGET_MS) || 10;

// Schema matching the plugin's writer (index.js).
const SCHEMA = `
  CREATE TABLE IF NOT EXISTS metrics (
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE NOT NULL
  ) STRICT;

  CREATE TABLE IF NOT EXISTS telemetry_real (
      ts_ms INTEGER NOT NULL,
      metric_id INTEGER NOT NULL,
      value REAL NOT NULL,
      source TEXT,
      context TEXT,
      PRIMARY KEY (ts_ms, metric_id, source)
  ) STRICT, WITHOUT ROWID;

  CREATE TABLE IF NOT EXISTS telemetry_bool (
      ts_ms INTEGER NOT NULL,
      metric_id INTEGER NOT NULL,
      value INTEGER NOT NULL,
      source TEXT,
      context TEXT,
      PRIMARY KEY (ts_ms, metric_id, source)
  ) STRICT, WITHOUT ROWID;

  CREATE TABLE IF NOT EXISTS telemetry_string (
      ts_ms INTEGER NOT NULL,
      metric_id INTEGER NOT NULL,
      value TEXT NOT NULL,
      source TEXT,
      context TEXT,
      PRIMARY KEY (ts_ms, metric_id, source)
  ) STRICT, WITHOUT ROWID;

  CREATE TABLE IF NOT EXISTS telemetry_json (
      ts_ms INTEGER NOT NULL,
      metric_id INTEGER NOT NULL,
      value TEXT NOT NULL,
      source TEXT,
      context TEXT,
      PRIMARY KEY (ts_ms, metric_id, source)
  ) STRICT, WITHOUT ROWID;
`;

function createMockApp(dataDir) {
  const app = {
    selfId: "urn:mrn:imo:mmsi:123456789",
    getDataDirPath: () => dataDir,
    debug: () => {},
    error: (msg) => console.error("[ERROR]", msg),
    setPluginStatus: () => {},
    subscriptionmanager: {
      subscribe: (_subscription, _unsubscribes, _onError, onDelta) => {
        app.onDelta = onDelta;
      },
    },
  };
  return app;
}

test("flush path stays O(batch size) with a large database", () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "sk-perf-smoke-"));
  try {
    const dbDir = path.join(dataDir, "sqlite-history");
    fs.mkdirSync(dbDir, { recursive: true });
    const dbPath = path.join(dbDir, "telemetry.db");

    // Pre-populate so the database is big enough that O(database) work per
    // flush would dominate the measured ingest time
    const seed = new DatabaseSync(dbPath);
    seed.exec(SCHEMA);
    const insert = seed.prepare(
      "INSERT INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );
    const ts0 = Date.now() - PREPOPULATED_ROWS * 4320; // ~10 days of history
    seed.exec("BEGIN TRANSACTION");
    for (let i = 0; i < PREPOPULATED_ROWS; i++) {
      insert.run(
        ts0 + i * 4320,
        (i % 40) + 1,
        Math.random(),
        "canbus.1",
        "vessels.urn:mrn:imo:mmsi:123456789",
      );
    }
    seed.exec("COMMIT");
    const insertMetric = seed.prepare("INSERT INTO metrics (name) VALUES (?)");
    for (let i = 1; i <= 40; i++) insertMetric.run(`perf.seed.${i}`);
    seed.close();

    const app = createMockApp(dataDir);
    const pluginModule = require("../index.js");
    const plugin = pluginModule(app);
    plugin.start({
      batchSize: BATCH_SIZE,
      batchWriteInterval: 60, // interval never fires; only deterministic flushes
      resolution: 1,
    });
    const handler = app.onDelta;
    assert.ok(handler, "no delta handler registered");

    const base = Date.now();
    const started = performance.now();
    for (let i = 0; i < INGESTED_DELTAS; i++) {
      handler({
        context: "vessels.self",
        updates: [
          {
            timestamp: new Date(base + i * 5).toISOString(),
            $source: "perf.test",
            values: [{ path: `perf.metric.${i % 50}`, value: Math.random() }],
          },
        ],
      });
    }
    const ingestMs = performance.now() - started;
    plugin.stop();

    const flushes = INGESTED_DELTAS / BATCH_SIZE;
    const msPerFlush = ingestMs / flushes;
    assert.ok(
      msPerFlush < FLUSH_BUDGET_MS,
      `flush path too slow: ${msPerFlush.toFixed(1)} ms/flush over ${flushes} flushes ` +
        `(budget ${FLUSH_BUDGET_MS} ms) — did O(database) work creep back into flushBuffer()? ` +
        `See perf-plan.md`,
    );

    // The speed must not come from dropping data
    const verify = new DatabaseSync(dbPath);
    const count = verify
      .prepare("SELECT COUNT(*) as count FROM telemetry_real")
      .get().count;
    verify.close();
    assert.strictEqual(
      count,
      PREPOPULATED_ROWS + INGESTED_DELTAS,
      "expected all ingested values to be stored",
    );
  } finally {
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});
