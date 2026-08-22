const { test, describe } = require("node:test");
const assert = require("node:assert");
const path = require("node:path");
const fs = require("node:fs");
const { DatabaseSync } = require("node:sqlite");
const { SQLiteHistoryProvider } = require("../HistoryAPI");

describe("SQLiteHistoryProvider", () => {
  let db;
  let dbPath;
  let provider;
  const selfId = "urn:mrn:imo:mmsi:123456789";

  test.beforeEach(() => {
    // Create an in-memory test database
    dbPath = path.join(__dirname, `test-${Date.now()}.db`);
    db = new DatabaseSync(dbPath);

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
    `);

    provider = new SQLiteHistoryProvider(
      db,
      selfId,
      () => {}, // No-op debug function
    );
  });

  test.afterEach(() => {
    if (db) {
      db.close();
    }
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test("should create a provider instance", () => {
    assert.ok(provider);
    assert.strictEqual(provider.selfId, selfId);
    assert.ok(provider.db);
  });

  test("should get contexts", async () => {
    // Insert test data
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertTelemetry = db.prepare(
      "INSERT INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const metricResult = insertMetric.run("navigation.speedOverGround");
    const metricId = metricResult.lastInsertRowid;
    insertTelemetry.run(Date.now(), metricId, 5.2, "test", `vessels.${selfId}`);

    const query = {
      from: new Date(Date.now() - 3600000).toISOString(),
      to: new Date(Date.now() + 3600000).toISOString(),
    };

    const contexts = await provider.getContexts(query);
    assert.ok(Array.isArray(contexts));
    assert.strictEqual(contexts.length, 1);
    assert.strictEqual(contexts[0], `vessels.${selfId}`);
  });

  test("should get paths", async () => {
    // Insert test data
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertTelemetry = db.prepare(
      "INSERT INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const metricResult = insertMetric.run("navigation.speedOverGround");
    const metricId = metricResult.lastInsertRowid;
    insertTelemetry.run(Date.now(), metricId, 5.2, "test", `vessels.${selfId}`);

    const query = {
      from: new Date(Date.now() - 3600000).toISOString(),
      to: new Date(Date.now() + 3600000).toISOString(),
    };

    const paths = await provider.getPaths(query);
    assert.ok(Array.isArray(paths));
    assert.strictEqual(paths.length, 1);
    assert.strictEqual(paths[0], "navigation.speedOverGround");
  });

  test("should get numeric values", async () => {
    const now = Date.now();
    const oneHourAgo = now - 3600000;

    // Insert test data
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertTelemetry = db.prepare(
      "INSERT INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const speedResult = insertMetric.run("navigation.speedOverGround");
    const speedMetricId = speedResult.lastInsertRowid;
    // Insert some values over the hour
    for (let i = 0; i < 60; i++) {
      const ts = oneHourAgo + i * 60000; // Every minute
      insertTelemetry.run(
        ts,
        speedMetricId,
        5.0 + i * 0.1,
        "test",
        `vessels.${selfId}`,
      );
    }

    const query = {
      context: "vessels.self",
      from: new Date(oneHourAgo).toISOString(),
      to: new Date(now).toISOString(),
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
        },
      ],
    };

    const result = await provider.getValues(query);
    assert.ok(result);
    assert.strictEqual(result.context, `vessels.${selfId}`);
    assert.ok(result.range);
    assert.ok(result.values);
    assert.ok(result.data);
    assert.strictEqual(result.values.length, 1);
    assert.strictEqual(result.values[0].path, "navigation.speedOverGround");
    assert.strictEqual(result.values[0].method, "average");
    assert.ok(result.data.length > 0);
  });

  test("should get position values", async () => {
    const now = Date.now();
    const oneHourAgo = now - 3600000;

    // Insert test data for position
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertTelemetry = db.prepare(
      "INSERT INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const lonResult = insertMetric.run("navigation.position.longitude");
    const latResult = insertMetric.run("navigation.position.latitude");
    const lonMetricId = lonResult.lastInsertRowid;
    const latMetricId = latResult.lastInsertRowid;

    // Insert some position values
    for (let i = 0; i < 10; i++) {
      const ts = oneHourAgo + i * 300000; // Every 5 minutes
      insertTelemetry.run(
        ts,
        lonMetricId,
        -122.4 + i * 0.01,
        "test",
        `vessels.${selfId}`,
      );
      insertTelemetry.run(
        ts,
        latMetricId,
        37.8 + i * 0.01,
        "test",
        `vessels.${selfId}`,
      );
    }

    const query = {
      context: "vessels.self",
      from: new Date(oneHourAgo).toISOString(),
      to: new Date(now).toISOString(),
      pathSpecs: [
        {
          path: "navigation.position",
          aggregate: "first",
        },
      ],
    };

    const result = await provider.getValues(query);
    assert.ok(result);
    assert.strictEqual(result.context, `vessels.${selfId}`);
    assert.ok(result.values);
    assert.strictEqual(result.values.length, 1);
    assert.strictEqual(result.values[0].path, "navigation.position");
    assert.ok(result.data.length > 0);

    // Check that position data is formatted as [longitude, latitude]
    const firstPosition = result.data[0][1];
    assert.ok(Array.isArray(firstPosition));
    assert.strictEqual(firstPosition.length, 2);
    assert.strictEqual(typeof firstPosition[0], "number"); // longitude
    assert.strictEqual(typeof firstPosition[1], "number"); // latitude
  });
});

describe("Database Schema", () => {
  test("should create all required tables", () => {
    const dbPath = path.join(__dirname, `schema-test-${Date.now()}.db`);
    const db = new DatabaseSync(dbPath);

    db.exec(`
      CREATE TABLE metrics (
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
    `);

    const tables = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table'")
      .all();
    const tableNames = tables.map((t) => t.name);

    assert.ok(tableNames.includes("metrics"));
    assert.ok(tableNames.includes("telemetry_real"));
    assert.ok(tableNames.includes("telemetry_bool"));
    assert.ok(tableNames.includes("telemetry_string"));
    assert.ok(tableNames.includes("telemetry_json"));

    db.close();
    fs.unlinkSync(dbPath);
  });

  test("should enforce STRICT table constraints", () => {
    const dbPath = path.join(__dirname, `strict-test-${Date.now()}.db`);
    const db = new DatabaseSync(dbPath);

    db.exec(`
      CREATE TABLE metrics (
          id INTEGER PRIMARY KEY,
          name TEXT UNIQUE NOT NULL
      ) STRICT;

      CREATE TABLE telemetry_real (
          ts_ms INTEGER NOT NULL,
          metric_id INTEGER NOT NULL,
          value REAL NOT NULL,
          source TEXT,
          context TEXT,
          PRIMARY KEY (ts_ms, metric_id, source)
      ) STRICT, WITHOUT ROWID;
    `);

    // Insert valid data
    db.prepare("INSERT INTO metrics (name) VALUES (?)").run("test.metric");
    db.prepare("INSERT INTO metrics (name) VALUES (?)").run("test.metric2");
    const metricId = db
      .prepare("SELECT id FROM metrics WHERE name = ?")
      .get("test.metric").id;

    // This should work
    db.prepare(
      "INSERT INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    ).run(Date.now(), metricId, 42.5, "test", "vessels.test");

    // Try to insert invalid data (string where REAL expected) - this should throw
    assert.throws(() => {
      db.prepare(
        "INSERT INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
      ).run(
        Date.now() + 1,
        metricId + 1,
        "not a number",
        "test",
        "vessels.test",
      );
    }, /cannot store TEXT value in REAL column/);

    db.close();
    fs.unlinkSync(dbPath);
  });
});

describe("Utility Functions", () => {
  test("makeArray should create 2D arrays", () => {
    const makeArray = (d1, d2) => {
      const arr = [];
      for (let i = 0; i < d1; i++) {
        arr.push(new Array(d2));
      }
      return arr;
    };

    const arr = makeArray(3, 4);
    assert.strictEqual(arr.length, 3);
    assert.strictEqual(arr[0].length, 4);
    assert.strictEqual(arr[1].length, 4);
    assert.strictEqual(arr[2].length, 4);
  });

  test("valuesForSpecs should create value descriptors", () => {
    const valuesForSpecs = (pathSpecs) => {
      return pathSpecs.map(({ path, aggregateMethod, sourceRef }) => ({
        path,
        method: aggregateMethod,
        ...(sourceRef ? { sourceRef } : {}),
      }));
    };

    const specs = [
      {
        path: "test.path",
        aggregateMethod: "average",
        sourceRef: "test.source",
      },
      { path: "test.path2", aggregateMethod: "max" },
    ];

    const result = valuesForSpecs(specs);
    assert.strictEqual(result.length, 2);
    assert.strictEqual(result[0].path, "test.path");
    assert.strictEqual(result[0].method, "average");
    assert.strictEqual(result[0].sourceRef, "test.source");
    assert.strictEqual(result[1].path, "test.path2");
    assert.strictEqual(result[1].method, "max");
    assert.strictEqual(result[1].sourceRef, undefined);
  });

  test("getTimeRange should parse ISO date strings", () => {
    const getTimeRange = (query) => {
      if (query.from !== undefined) {
        const from = new Date(query.from.toString());
        const to =
          query.to !== undefined ? new Date(query.to.toString()) : new Date();
        return { from, to };
      }
      throw new Error("Invalid time range parameters");
    };

    const query = {
      from: "2025-01-01T00:00:00Z",
      to: "2025-01-02T00:00:00Z",
    };

    const { from, to } = getTimeRange(query);
    assert.strictEqual(from.toISOString(), "2025-01-01T00:00:00.000Z");
    assert.strictEqual(to.toISOString(), "2025-01-02T00:00:00.000Z");
  });

  test("resolveEmaParams should calculate alpha from period", () => {
    const resolveEmaParams = (spec) => {
      const rawParam =
        spec.parameters.length > 0
          ? Number.parseFloat(spec.parameters[0])
          : Number.NaN;
      if (Number.isFinite(rawParam) && rawParam > 0 && rawParam < 1) {
        const alpha = rawParam;
        const period = 2 / alpha - 1;
        return { period, alpha };
      }
      const period = Number.isFinite(rawParam) && rawParam > 0 ? rawParam : 5;
      const alpha = 2 / (period + 1);
      return { period, alpha };
    };

    // Test default
    const defaultResult = resolveEmaParams({ parameters: [] });
    assert.strictEqual(defaultResult.period, 5);
    assert.strictEqual(defaultResult.alpha, 2 / 6);

    // Test explicit period
    const periodResult = resolveEmaParams({ parameters: ["10"] });
    assert.strictEqual(periodResult.period, 10);
    assert.strictEqual(periodResult.alpha, 2 / 11);

    // Test explicit alpha
    const alphaResult = resolveEmaParams({ parameters: ["0.2"] });
    assert.strictEqual(alphaResult.alpha, 0.2);
    assert.strictEqual(alphaResult.period, 2 / 0.2 - 1);
  });
});
