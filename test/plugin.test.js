const { test, describe } = require("node:test");
const assert = require("node:assert");
const path = require("node:path");
const fs = require("node:fs");
const { DatabaseSync } = require("node:sqlite");

// Mock Signal K app
function createMockApp(dataDir) {
  const subscriptions = [];
  const deltaHandlers = [];

  const app = {
    selfId: "urn:mrn:imo:mmsi:123456789",
    getDataDirPath: () => dataDir,
    debug: () => {},
    error: (msg) => console.error("[ERROR]", msg),
    setPluginStatus: (_status) => {},
    subscriptionmanager: {
      subscribe: (subscription, _unsubscribes, _onError, onDelta) => {
        subscriptions.push(subscription);
        deltaHandlers.push(onDelta);
      },
    },
    registerHistoryApiProvider: (provider) => {
      app.historyProvider = provider;
    },
    getSubscriptions: () => subscriptions,
    getDeltaHandlers: () => deltaHandlers,
  };

  return app;
}

describe("Plugin", () => {
  let pluginModule;
  let testDataDir;
  let app;
  let plugin;

  test.beforeEach(() => {
    testDataDir = path.join(__dirname, `.test-data-${Date.now()}`);
    fs.mkdirSync(testDataDir, { recursive: true });

    app = createMockApp(testDataDir);
    pluginModule = require("../index.js");
  });

  test.afterEach(() => {
    if (plugin && typeof plugin.stop === "function") {
      try {
        plugin.stop();
      } catch (_err) {
        // Ignore errors if plugin wasn't started
      }
    }
    if (fs.existsSync(testDataDir)) {
      fs.rmSync(testDataDir, { recursive: true, force: true });
    }
    // Clear the require cache so we get a fresh module each test
    delete require.cache[require.resolve("../index.js")];
  });

  test("should create plugin with correct metadata", () => {
    plugin = pluginModule(app);

    assert.strictEqual(plugin.id, "signalk-history-sqlite");
    assert.strictEqual(plugin.name, "Signal K SQLite History Storage");
    assert.strictEqual(
      plugin.description,
      "Low-overhead, zero-dependency time-series storage using node:sqlite",
    );
    assert.ok(plugin.schema);
  });

  test("should have valid schema configuration", () => {
    plugin = pluginModule(app);

    assert.strictEqual(plugin.schema.type, "object");
    assert.ok(plugin.schema.properties);
    assert.ok(plugin.schema.properties.batchSize);
    assert.ok(plugin.schema.properties.batchWriteInterval);
    assert.ok(plugin.schema.properties.resolution);
    assert.ok(plugin.schema.properties.recordTrack);
    assert.ok(plugin.schema.properties.storeOthers);
    assert.ok(plugin.schema.properties.allowOrDeny);
    assert.ok(plugin.schema.properties.allowOrDenylist);
  });

  test("should start and create database", () => {
    plugin = pluginModule(app);

    assert.doesNotThrow(() => {
      plugin.start({
        batchSize: 100,
        batchWriteInterval: 1,
        resolution: 200,
        recordTrack: true,
        storeOthers: false,
      });
    });

    // Check that database was created
    const dbPath = path.join(testDataDir, "sqlite-history", "telemetry.db");
    assert.ok(fs.existsSync(dbPath));

    // Verify schema was created
    const db = new DatabaseSync(dbPath);
    const tables = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table'")
      .all();
    const tableNames = tables.map((t) => t.name);
    assert.ok(tableNames.includes("metrics"));
    assert.ok(tableNames.includes("telemetry_real"));
    assert.ok(tableNames.includes("telemetry_bool"));
    db.close();
  });

  test("should handle delta updates", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 10,
      batchWriteInterval: 1,
      resolution: 200,
    });

    const deltaHandlers = app.getDeltaHandlers();
    assert.strictEqual(deltaHandlers.length, 1);

    const handler = deltaHandlers[0];

    // Send a delta
    assert.doesNotThrow(() => {
      handler({
        context: "vessels.self",
        updates: [
          {
            timestamp: new Date().toISOString(),
            $source: "test.source",
            values: [
              { path: "navigation.speedOverGround", value: 5.2 },
              { path: "navigation.speedThroughWater", value: 5.0 },
            ],
          },
        ],
      });
    });

    // Give time for flush
    return new Promise((resolve) => setTimeout(resolve, 1500)).then(() => {
      const dbPath = path.join(testDataDir, "sqlite-history", "telemetry.db");
      const db = new DatabaseSync(dbPath);

      // Check metrics were created
      const metrics = db.prepare("SELECT name FROM metrics").all();
      const metricNames = metrics.map((m) => m.name);
      assert.ok(metricNames.includes("navigation.speedOverGround"));
      assert.ok(metricNames.includes("navigation.speedThroughWater"));

      // Check data was stored
      const rows = db
        .prepare("SELECT COUNT(*) as count FROM telemetry_real")
        .get();
      assert.ok(rows.count >= 2);

      db.close();
    });
  });

  test("should handle navigation.position correctly", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 10,
      batchWriteInterval: 1,
      resolution: 200,
      recordTrack: true,
    });

    const deltaHandlers = app.getDeltaHandlers();
    const handler = deltaHandlers[0];

    // Send a position delta
    assert.doesNotThrow(() => {
      handler({
        context: "vessels.self",
        updates: [
          {
            timestamp: new Date().toISOString(),
            $source: "gps",
            values: [
              {
                path: "navigation.position",
                value: {
                  longitude: -122.4194,
                  latitude: 37.7749,
                },
              },
            ],
          },
        ],
      });
    });

    // Give time for flush
    return new Promise((resolve) => setTimeout(resolve, 1500)).then(() => {
      const dbPath = path.join(testDataDir, "sqlite-history", "telemetry.db");
      const db = new DatabaseSync(dbPath);

      // Check that latitude/longitude metrics were created
      const metrics = db.prepare("SELECT name FROM metrics").all();
      const metricNames = metrics.map((m) => m.name);
      assert.ok(metricNames.includes("navigation.position.longitude"));
      assert.ok(metricNames.includes("navigation.position.latitude"));

      // Check data was stored
      const lonRow = db
        .prepare(
          "SELECT value FROM telemetry_real t JOIN metrics m ON t.metric_id = m.id WHERE m.name = ? LIMIT 1",
        )
        .get("navigation.position.longitude");
      assert.ok(lonRow);
      assert.strictEqual(lonRow.value, -122.4194);

      const latRow = db
        .prepare(
          "SELECT value FROM telemetry_real t JOIN metrics m ON t.metric_id = m.id WHERE m.name = ? LIMIT 1",
        )
        .get("navigation.position.latitude");
      assert.ok(latRow);
      assert.strictEqual(latRow.value, 37.7749);

      db.close();
    });
  });

  test("should stop cleanly", () => {
    plugin = pluginModule(app);

    assert.doesNotThrow(() => {
      plugin.start({
        batchSize: 100,
        batchWriteInterval: 1,
        resolution: 200,
      });
    });

    assert.doesNotThrow(() => {
      plugin.stop();
    });
  });

  test("should respect denylist", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 10,
      batchWriteInterval: 1,
      resolution: 200,
      allowOrDeny: "Deny",
      allowOrDenylist: ["navigation.speedOverGround"],
    });

    const deltaHandlers = app.getDeltaHandlers();
    const handler = deltaHandlers[0];

    handler({
      context: "vessels.self",
      updates: [
        {
          timestamp: new Date().toISOString(),
          $source: "test.source",
          values: [
            { path: "navigation.speedOverGround", value: 5.2 }, // Should be filtered out
            { path: "navigation.speedThroughWater", value: 5.0 }, // Should be stored
          ],
        },
      ],
    });

    return new Promise((resolve) => setTimeout(resolve, 1500)).then(() => {
      const dbPath = path.join(testDataDir, "sqlite-history", "telemetry.db");
      const db = new DatabaseSync(dbPath);

      const metrics = db.prepare("SELECT name FROM metrics").all();
      const metricNames = metrics.map((m) => m.name);

      // speedOverGround should NOT be in the database (denylisted)
      assert.ok(!metricNames.includes("navigation.speedOverGround"));
      // speedThroughWater should be in the database
      assert.ok(metricNames.includes("navigation.speedThroughWater"));

      db.close();
    });
  });

  test("should respect allowlist", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 10,
      batchWriteInterval: 1,
      resolution: 200,
      allowOrDeny: "Allow",
      allowOrDenylist: ["navigation.speedOverGround"],
    });

    const deltaHandlers = app.getDeltaHandlers();
    const handler = deltaHandlers[0];

    handler({
      context: "vessels.self",
      updates: [
        {
          timestamp: new Date().toISOString(),
          $source: "test.source",
          values: [
            { path: "navigation.speedOverGround", value: 5.2 }, // Should be stored
            { path: "navigation.speedThroughWater", value: 5.0 }, // Should be filtered out
          ],
        },
      ],
    });

    return new Promise((resolve) => setTimeout(resolve, 1500)).then(() => {
      const dbPath = path.join(testDataDir, "sqlite-history", "telemetry.db");
      const db = new DatabaseSync(dbPath);

      const metrics = db.prepare("SELECT name FROM metrics").all();
      const metricNames = metrics.map((m) => m.name);

      // speedOverGround should be in the database (allowlisted)
      assert.ok(metricNames.includes("navigation.speedOverGround"));
      // speedThroughWater should NOT be in the database
      assert.ok(!metricNames.includes("navigation.speedThroughWater"));

      db.close();
    });
  });

  test("should store boolean values", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 10,
      batchWriteInterval: 1,
      resolution: 200,
    });

    const deltaHandlers = app.getDeltaHandlers();
    const handler = deltaHandlers[0];

    handler({
      context: "vessels.self",
      updates: [
        {
          timestamp: new Date().toISOString(),
          $source: "test.source",
          values: [
            { path: "electrical.switches.1", value: true },
            { path: "electrical.switches.2", value: false },
          ],
        },
      ],
    });

    return new Promise((resolve) => setTimeout(resolve, 1500)).then(() => {
      const dbPath = path.join(testDataDir, "sqlite-history", "telemetry.db");
      const db = new DatabaseSync(dbPath);

      // Check metrics were created
      const metrics = db.prepare("SELECT name FROM metrics").all();
      const metricNames = metrics.map((m) => m.name);
      assert.ok(metricNames.includes("electrical.switches.1"));
      assert.ok(metricNames.includes("electrical.switches.2"));

      // Check data was stored in telemetry_bool
      const rows = db
        .prepare("SELECT COUNT(*) as count FROM telemetry_bool")
        .get();
      assert.ok(rows.count >= 2);

      // Check the values
      const switch1 = db
        .prepare(
          "SELECT value FROM telemetry_bool t JOIN metrics m ON t.metric_id = m.id WHERE m.name = ? LIMIT 1",
        )
        .get("electrical.switches.1");
      assert.strictEqual(switch1.value, 1); // true stored as 1

      const switch2 = db
        .prepare(
          "SELECT value FROM telemetry_bool t JOIN metrics m ON t.metric_id = m.id WHERE m.name = ? LIMIT 1",
        )
        .get("electrical.switches.2");
      assert.strictEqual(switch2.value, 0); // false stored as 0

      db.close();
    });
  });

  test("should store JSON object values in telemetry_json", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 10,
      batchWriteInterval: 1,
      resolution: 200,
    });

    const deltaHandlers = app.getDeltaHandlers();
    const handler = deltaHandlers[0];

    const payload = {
      state: "alert",
      message: "low battery",
      method: ["sound"],
    };
    handler({
      context: "vessels.self",
      updates: [
        {
          timestamp: new Date().toISOString(),
          $source: "test.source",
          values: [{ path: "notifications.mobility", value: payload }],
        },
      ],
    });

    return new Promise((resolve) => setTimeout(resolve, 1500)).then(() => {
      const dbPath = path.join(testDataDir, "sqlite-history", "telemetry.db");
      const db = new DatabaseSync(dbPath);

      const metrics = db.prepare("SELECT name FROM metrics").all();
      const metricNames = metrics.map((m) => m.name);
      assert.ok(metricNames.includes("notifications.mobility"));

      const rows = db
        .prepare("SELECT COUNT(*) as count FROM telemetry_json")
        .get();
      assert.ok(rows.count >= 1);

      const row = db
        .prepare(
          "SELECT value FROM telemetry_json t JOIN metrics m ON t.metric_id = m.id WHERE m.name = ? LIMIT 1",
        )
        .get("notifications.mobility");
      assert.ok(row);
      assert.deepStrictEqual(JSON.parse(row.value), payload);

      db.close();
    });
  });

  test("should serve JSON object values through the History API", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 10,
      batchWriteInterval: 1,
      resolution: 200,
    });

    const deltaHandlers = app.getDeltaHandlers();
    const handler = deltaHandlers[0];

    const payload = { state: "normal", message: "all good" };
    handler({
      context: "vessels.self",
      updates: [
        {
          timestamp: new Date().toISOString(),
          $source: "test.source",
          values: [{ path: "notifications.mobility", value: payload }],
        },
      ],
    });

    return new Promise((resolve) => setTimeout(resolve, 1500)).then(
      async () => {
        const provider = app.historyProvider;
        assert.ok(provider);

        const result = await provider.getValues({
          from: new Date(Date.now() - 60_000).toISOString(),
          to: new Date(Date.now() + 60_000).toISOString(),
          context: "vessels.self",
          resolution: 60,
          pathSpecs: [
            {
              path: "notifications.mobility",
              aggregate: "first",
              parameter: [],
            },
          ],
        });

        assert.ok(result.data.length >= 1);
        const row = result.data.find((r) => r[1] !== null);
        assert.ok(row, "expected a non-null row");
        assert.deepStrictEqual(row[1], payload);
      },
    );
  });

  test("should register history provider", () => {
    plugin = pluginModule(app);
    plugin.start({
      batchSize: 100,
      batchWriteInterval: 1,
      resolution: 200,
    });

    assert.ok(app.historyProvider);
    assert.ok(app.historyProvider.getValues);
    assert.ok(app.historyProvider.getContexts);
    assert.ok(app.historyProvider.getPaths);
  });
});
