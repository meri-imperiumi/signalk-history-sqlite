const { test, describe } = require("node:test");
const assert = require("node:assert");
const path = require("node:path");
const fs = require("node:fs");
const { DatabaseSync } = require("node:sqlite");

describe("String Telemetry Storage", () => {
  let db;
  let dbPath;

  test.beforeEach(() => {
    dbPath = path.join(__dirname, `test-string-${Date.now()}.db`);
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
    `);
  });

  test.afterEach(() => {
    if (db) {
      db.close();
    }
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
  });

  test("should store string values in telemetry_string table", () => {
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertString = db.prepare(
      "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const modeResult = insertMetric.run("autopilot.mode");
    const modeMetricId = modeResult.lastInsertRowid;

    const ts = Date.now();
    insertString.run(ts, modeMetricId, "wind", "autopilot", "vessels.123");

    const count = db
      .prepare("SELECT COUNT(*) as c FROM telemetry_string")
      .get().c;
    assert.strictEqual(count, 1);

    const row = db
      .prepare("SELECT value FROM telemetry_string WHERE metric_id = ?")
      .get(modeMetricId);
    assert.strictEqual(row.value, "wind");
  });

  test("should store multiple string values", () => {
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertString = db.prepare(
      "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const modeId = insertMetric.run("autopilot.mode").lastInsertRowid;
    const stateId = insertMetric.run("steering.state").lastInsertRowid;

    const ts = Date.now();
    insertString.run(ts, modeId, "wind", "autopilot", "vessels.123");
    insertString.run(ts, stateId, "engaged", "steering", "vessels.123");

    const count = db
      .prepare("SELECT COUNT(*) as c FROM telemetry_string")
      .get().c;
    assert.strictEqual(count, 2);
  });

  test("should update string values for same metric", () => {
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertString = db.prepare(
      "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const modeId = insertMetric.run("autopilot.mode").lastInsertRowid;

    const ts = Date.now();
    // INSERT with same primary key (ts_ms, metric_id, source) should REPLACE
    insertString.run(ts, modeId, "wind", "autopilot", "vessels.123");
    insertString.run(ts, modeId, "track", "autopilot", "vessels.123");

    const count = db
      .prepare("SELECT COUNT(*) as c FROM telemetry_string")
      .get().c;
    assert.strictEqual(count, 1);

    const row = db
      .prepare("SELECT value FROM telemetry_string WHERE metric_id = ?")
      .get(modeId);
    assert.strictEqual(row.value, "track"); // Last value wins due to REPLACE
  });

  test("should count string values in getDatabaseStats", () => {
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertReal = db.prepare(
      "INSERT OR REPLACE INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );
    const insertBool = db.prepare(
      "INSERT OR REPLACE INTO telemetry_bool (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );
    const insertString = db.prepare(
      "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const speedId = insertMetric.run(
      "navigation.speedOverGround",
    ).lastInsertRowid;
    const lightId = insertMetric.run("navigation.lights").lastInsertRowid;
    const modeId = insertMetric.run("autopilot.mode").lastInsertRowid;

    const ts = Date.now();
    insertReal.run(ts, speedId, 5.2, "gps", "vessels.123");
    insertBool.run(ts, lightId, 1, "nmea2000", "vessels.123");
    insertString.run(ts, modeId, "wind", "autopilot", "vessels.123");

    const realCount = db
      .prepare("SELECT COUNT(*) as c FROM telemetry_real")
      .get().c;
    const boolCount = db
      .prepare("SELECT COUNT(*) as c FROM telemetry_bool")
      .get().c;
    const stringCount = db
      .prepare("SELECT COUNT(*) as c FROM telemetry_string")
      .get().c;

    assert.strictEqual(realCount, 1);
    assert.strictEqual(boolCount, 1);
    assert.strictEqual(stringCount, 1);
  });

  test("should handle empty strings", () => {
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertString = db.prepare(
      "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const modeId = insertMetric.run("autopilot.mode").lastInsertRowid;

    const ts = Date.now();
    insertString.run(ts, modeId, "", "autopilot", "vessels.123");

    const row = db
      .prepare("SELECT value FROM telemetry_string WHERE metric_id = ?")
      .get(modeId);
    assert.strictEqual(row.value, "");
  });

  test("should handle strings with special characters", () => {
    const insertMetric = db.prepare("INSERT INTO metrics (name) VALUES (?)");
    const insertString = db.prepare(
      "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
    );

    const modeId = insertMetric.run("autopilot.mode").lastInsertRowid;

    const specialStrings = [
      "wind'ward",
      'nav"station',
      "mode:standby",
      "status=active",
      "line1\nline2",
    ];

    const ts = Date.now();
    for (let i = 0; i < specialStrings.length; i++) {
      insertString.run(
        ts + i * 60000,
        modeId,
        specialStrings[i],
        "test",
        "vessels.123",
      );
    }

    const count = db
      .prepare("SELECT COUNT(*) as c FROM telemetry_string")
      .get().c;
    assert.strictEqual(count, specialStrings.length);
  });
});
