const { test, describe } = require("node:test");
const assert = require("node:assert");
const path = require("node:path");
const fs = require("node:fs");
const { DatabaseSync } = require("node:sqlite");
const { SQLiteHistoryProvider } = require("../HistoryAPI");

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

  CREATE INDEX IF NOT EXISTS idx_telemetry_real_ts ON telemetry_real(ts_ms);
  CREATE INDEX IF NOT EXISTS idx_telemetry_real_metric ON telemetry_real(metric_id);
  CREATE INDEX IF NOT EXISTS idx_telemetry_bool_ts ON telemetry_bool(ts_ms);
  CREATE INDEX IF NOT EXISTS idx_telemetry_bool_metric ON telemetry_bool(metric_id);
  CREATE INDEX IF NOT EXISTS idx_telemetry_string_ts ON telemetry_string(ts_ms);
  CREATE INDEX IF NOT EXISTS idx_telemetry_string_metric ON telemetry_string(metric_id);
  CREATE INDEX IF NOT EXISTS idx_telemetry_json_ts ON telemetry_json(ts_ms);
  CREATE INDEX IF NOT EXISTS idx_telemetry_json_metric ON telemetry_json(metric_id);
`;

const SELF_ID = "urn:mrn:imo:mmsi:123456789";
const SELF_CONTEXT = `vessels.${SELF_ID}`;

function setup() {
  const dbPath = path.join(__dirname, `nonnumeric-${Date.now()}.db`);
  const db = new DatabaseSync(dbPath);
  db.exec(SCHEMA);

  const insertMetric = db.prepare(
    "INSERT INTO metrics (name) VALUES (?) RETURNING id",
  );
  const insertReal = db.prepare(
    "INSERT OR REPLACE INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
  );
  const insertBool = db.prepare(
    "INSERT OR REPLACE INTO telemetry_bool (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
  );
  const insertString = db.prepare(
    "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
  );
  const insertJson = db.prepare(
    "INSERT OR REPLACE INTO telemetry_json (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
  );

  function metric(name) {
    return insertMetric.get(name).id;
  }

  return {
    db,
    dbPath,
    provider: new SQLiteHistoryProvider(db, SELF_ID, () => {}),
    metric,
    insertReal,
    insertBool,
    insertString,
    insertJson,
  };
}

function cleanup(ctx) {
  ctx.db.close();
  if (fs.existsSync(ctx.dbPath)) {
    fs.unlinkSync(ctx.dbPath);
  }
}

describe("SQLiteHistoryProvider non-numeric values", () => {
  test("getValues returns string values from telemetry_string, coercing average to first", async () => {
    const ctx = setup();
    try {
      const modeId = ctx.metric("autopilot.mode");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      ctx.insertString.run(base, modeId, "wind", "autopilot", SELF_CONTEXT);
      ctx.insertString.run(
        base + 60_000,
        modeId,
        "track",
        "autopilot",
        SELF_CONTEXT,
      );

      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 120_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          { path: "autopilot.mode", aggregate: "average", parameter: [] },
        ],
      });

      assert.deepEqual(result.values, [
        { path: "autopilot.mode", method: "average" },
      ]);
      assert.deepEqual(result.data, [
        ["2026-08-08T00:00:00.000Z", "wind"],
        ["2026-08-08T00:01:00.000Z", "track"],
      ]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues returns boolean values from telemetry_bool and decodes 0/1 to booleans", async () => {
    const ctx = setup();
    try {
      const chargingId = ctx.metric("electrical.batteries.0.charging");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      ctx.insertBool.run(base, chargingId, 0, "src", SELF_CONTEXT);
      ctx.insertBool.run(base + 60_000, chargingId, 1, "src", SELF_CONTEXT);

      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 120_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          {
            path: "electrical.batteries.0.charging",
            aggregate: "last",
            parameter: [],
          },
        ],
      });

      // `last` is chronological within each bucket; each bucket has one point.
      assert.deepEqual(result.data, [
        ["2026-08-08T00:00:00.000Z", false],
        ["2026-08-08T00:01:00.000Z", true],
      ]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues decodes telemetry_json objects back into their original value", async () => {
    const ctx = setup();
    try {
      const notifId = ctx.metric("notifications.mobility");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      const payload = {
        state: "normal",
        message: "all good",
        method: ["visual"],
      };
      ctx.insertJson.run(
        base,
        notifId,
        JSON.stringify(payload),
        "src",
        SELF_CONTEXT,
      );

      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 120_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          { path: "notifications.mobility", aggregate: "first", parameter: [] },
        ],
      });

      assert.deepEqual(result.data, [["2026-08-08T00:00:00.000Z", payload]]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues collates a numeric and a textual path in a single response", async () => {
    const ctx = setup();
    try {
      const speedId = ctx.metric("navigation.speedOverGround");
      const modeId = ctx.metric("notifications.mobility.state");
      const base = Date.parse("2026-08-08T00:00:00.000Z");

      ctx.insertReal.run(base, speedId, 5.1, "gps", SELF_CONTEXT);
      ctx.insertReal.run(base + 60_000, speedId, 5.4, "gps", SELF_CONTEXT);
      ctx.insertString.run(base, modeId, "normal", "src", SELF_CONTEXT);
      ctx.insertString.run(
        base + 120_000,
        modeId,
        "alert",
        "src",
        SELF_CONTEXT,
      );

      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 180_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          {
            path: "navigation.speedOverGround",
            aggregate: "average",
            parameter: [],
          },
          {
            path: "notifications.mobility.state",
            aggregate: "average",
            parameter: [],
          },
        ],
      });

      assert.deepEqual(
        result.values.map((v) => v.path),
        ["navigation.speedOverGround", "notifications.mobility.state"],
      );
      assert.deepEqual(result.data, [
        ["2026-08-08T00:00:00.000Z", 5.1, "normal"],
        ["2026-08-08T00:01:00.000Z", 5.4, null],
        ["2026-08-08T00:02:00.000Z", null, "alert"],
      ]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues uses chronological first/last within a bucket, not value min/max", async () => {
    const ctx = setup();
    try {
      const speedId = ctx.metric("navigation.speedOverGround");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      // Within the first 60s bucket: insert 9.0 first, then 1.0.
      ctx.insertReal.run(base, speedId, 9.0, "gps", SELF_CONTEXT);
      ctx.insertReal.run(base + 30_000, speedId, 1.0, "gps", SELF_CONTEXT);
      // Next bucket: 5.0.
      ctx.insertReal.run(base + 60_000, speedId, 5.0, "gps", SELF_CONTEXT);

      const firstResult = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 120_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          {
            path: "navigation.speedOverGround",
            aggregate: "first",
            parameter: [],
          },
        ],
      });
      assert.deepEqual(firstResult.data, [
        ["2026-08-08T00:00:00.000Z", 9.0],
        ["2026-08-08T00:01:00.000Z", 5.0],
      ]);

      const lastResult = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 120_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          {
            path: "navigation.speedOverGround",
            aggregate: "last",
            parameter: [],
          },
        ],
      });
      assert.deepEqual(lastResult.data, [
        ["2026-08-08T00:00:00.000Z", 1.0],
        ["2026-08-08T00:01:00.000Z", 5.0],
      ]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues uses chronological first/last for string values within a bucket", async () => {
    const ctx = setup();
    try {
      const modeId = ctx.metric("autopilot.mode");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      ctx.insertString.run(base, modeId, "wind", "autopilot", SELF_CONTEXT);
      ctx.insertString.run(
        base + 30_000,
        modeId,
        "track",
        "autopilot",
        SELF_CONTEXT,
      );

      const firstResult = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 60_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          { path: "autopilot.mode", aggregate: "first", parameter: [] },
        ],
      });
      assert.deepEqual(firstResult.data, [
        ["2026-08-08T00:00:00.000Z", "wind"],
      ]);

      const lastResult = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 60_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          { path: "autopilot.mode", aggregate: "last", parameter: [] },
        ],
      });
      assert.deepEqual(lastResult.data, [
        ["2026-08-08T00:00:00.000Z", "track"],
      ]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues filters non-numeric paths by sourceRef", async () => {
    const ctx = setup();
    try {
      const modeId = ctx.metric("autopilot.mode");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      ctx.insertString.run(base, modeId, "wind", "autopilot", SELF_CONTEXT);
      ctx.insertString.run(
        base + 60_000,
        modeId,
        "standby",
        "nmea2000",
        SELF_CONTEXT,
      );

      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 120_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          {
            path: "autopilot.mode",
            aggregate: "first",
            parameter: [],
            sourceRef: "nmea2000",
          },
        ],
      });

      assert.deepEqual(result.values, [
        { path: "autopilot.mode", method: "first", sourceRef: "nmea2000" },
      ]);
      assert.deepEqual(result.data, [["2026-08-08T00:01:00.000Z", "standby"]]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues discovers and caches the value type across calls", async () => {
    const ctx = setup();
    try {
      const modeId = ctx.metric("autopilot.mode");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      ctx.insertString.run(base, modeId, "wind", "autopilot", SELF_CONTEXT);

      const req = {
        from: new Date(base).toISOString(),
        to: new Date(base + 60_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          { path: "autopilot.mode", aggregate: "first", parameter: [] },
        ],
      };

      await ctx.provider.getValues(req);
      // The type should now be cached as 'string'.
      assert.strictEqual(
        ctx.provider.typeCache.get("autopilot.mode"),
        "string",
      );
      // A second call must still return the correct value.
      const result = await ctx.provider.getValues(req);
      assert.deepEqual(result.data, [["2026-08-08T00:00:00.000Z", "wind"]]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues defaults an unknown path (no data) to numeric", async () => {
    const ctx = setup();
    try {
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 60_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          { path: "never.seen", aggregate: "average", parameter: [] },
        ],
      });
      // No data, so empty data array and a values entry.
      assert.deepEqual(result.values, [
        { path: "never.seen", method: "average" },
      ]);
      assert.deepEqual(result.data, []);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues with first/last on a path that has no metric row returns empty data", async () => {
    const ctx = setup();
    try {
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 60_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          { path: "network.internet.state", aggregate: "last", parameter: [] },
        ],
      });
      assert.deepEqual(result.values, [
        { path: "network.internet.state", method: "last" },
      ]);
      assert.deepEqual(result.data, []);
    } finally {
      cleanup(ctx);
    }
  });

  test("getValues collates a first/last spec for an unknown path with a known path", async () => {
    const ctx = setup();
    try {
      const speedId = ctx.metric("navigation.speedOverGround");
      const base = Date.parse("2026-08-08T00:00:00.000Z");
      ctx.insertReal.run(base, speedId, 5.1, "gps", SELF_CONTEXT);

      const result = await ctx.provider.getValues({
        from: new Date(base).toISOString(),
        to: new Date(base + 60_000).toISOString(),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [
          {
            path: "navigation.speedOverGround",
            aggregate: "last",
            parameter: [],
          },
          { path: "never.seen", aggregate: "last", parameter: [] },
        ],
      });
      assert.deepEqual(result.data, [["2026-08-08T00:00:00.000Z", 5.1, null]]);
    } finally {
      cleanup(ctx);
    }
  });

  test("getContexts and getPaths include string/bool/json tables", async () => {
    const ctx = setup();
    try {
      const speedId = ctx.metric("navigation.speedOverGround");
      const modeId = ctx.metric("autopilot.mode");
      const chargingId = ctx.metric("electrical.batteries.0.charging");
      const notifId = ctx.metric("notifications.mobility");
      const base = Date.parse("2026-08-08T00:00:00.000Z");

      ctx.insertReal.run(base, speedId, 5.0, "gps", SELF_CONTEXT);
      ctx.insertString.run(base, modeId, "wind", "autopilot", SELF_CONTEXT);
      ctx.insertBool.run(base, chargingId, 1, "src", SELF_CONTEXT);
      ctx.insertJson.run(
        base,
        notifId,
        JSON.stringify({ state: "normal" }),
        "src",
        SELF_CONTEXT,
      );

      const query = {
        from: new Date(base - 1000).toISOString(),
        to: new Date(base + 1000).toISOString(),
      };

      const contexts = await ctx.provider.getContexts(query);
      assert.deepEqual(contexts, [SELF_CONTEXT]);

      const paths = await ctx.provider.getPaths(query);
      assert.deepEqual(paths.sort(), [
        "autopilot.mode",
        "electrical.batteries.0.charging",
        "navigation.speedOverGround",
        "notifications.mobility",
      ]);
    } finally {
      cleanup(ctx);
    }
  });
});
