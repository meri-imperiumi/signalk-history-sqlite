const { DatabaseSync } = require("node:sqlite");
const path = require("node:path");
const fs = require("node:fs");

const { SQLiteHistoryProvider } = require("./HistoryAPI.js");

module.exports = (app) => {
  const logError =
    app.error ||
    ((err) => {
      console.error(err);
    });
  const setStatus = app.setPluginStatus || app.setProviderStatus;
  let db;
  let insertRealStmt;
  let insertBoolStmt;
  let getMetricStmt;
  let insertMetricStmt;
  let batchBuffer = [];
  let flushInterval;
  let _started = false;
  let dbPath;

  const metricCache = new Map();
  const lastWriteTime = new Map(); // Track last write time per (path, source, context)
  const selfContext = `vessels.${app.selfId}`;

  const plugin = {
    id: "signalk-history-sqlite",
    name: "Signal K SQLite History Storage",
    description:
      "Low-overhead, zero-dependency time-series storage using node:sqlite",

    lastWriteTime: 0,
    totalPointsWritten: 0,
    throughputStartTime: 0,
    throughputPoints: 0,

    schema: {
      type: "object",
      properties: {
        batchSize: {
          type: "number",
          title: "Batch size",
          description:
            "Number of values to batch before writing to database (default: 500)",
          default: 500,
        },
        batchWriteInterval: {
          type: "number",
          title: "Batch write interval (seconds)",
          description: "Maximum time between batch writes (default: 1)",
          default: 1,
        },
        resolution: {
          type: "number",
          title: "Resolution (ms)",
          description:
            "Minimum time between storing values for the same path/source (default: 1000)",
          default: 1000,
        },
        recordTrack: {
          type: "boolean",
          title: "Record Track",
          description: "When enabled the vessel position will be stored",
          default: true,
        },
        storeOthers: {
          type: "boolean",
          title: "Record Others",
          description:
            "When enabled data from other vessels, atons and sar aircraft will be stored",
          default: false,
        },
        allowOrDeny: {
          type: "string",
          title: "Type of List",
          description:
            "With a denylist, all numeric values except the ones in the list below will be stored. With an allowlist, only the values in the list below will be stored.",
          default: "Deny",
          enum: ["Allow", "Deny"],
        },
        allowOrDenylist: {
          title: "SignalK Paths",
          description:
            "A list of Signal K paths to be excluded or included based on selection above",
          type: "array",
          items: {
            type: "string",
            title: "Path",
          },
        },
      },
    },

    start: (options) => {
      _started = true;

      const dbDir = path.join(app.getDataDirPath(), "sqlite-history");
      if (!fs.existsSync(dbDir)) {
        fs.mkdirSync(dbDir, { recursive: true });
      }
      dbPath = path.join(dbDir, "telemetry.db");

      setStatus(`Initializing SQLite database at ${dbPath}...`);

      // Initialize database using built-in node:sqlite
      db = new DatabaseSync(dbPath);
      db.exec("PRAGMA journal_mode = WAL;");
      db.exec("PRAGMA synchronous = NORMAL;");
      db.exec("PRAGMA cache_size = -8000;"); // ~8MB cache limit

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

        CREATE INDEX IF NOT EXISTS idx_telemetry_real_ts ON telemetry_real(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_telemetry_real_metric ON telemetry_real(metric_id);
        CREATE INDEX IF NOT EXISTS idx_telemetry_real_context ON telemetry_real(context);
      `);

      // Prepare statements
      insertRealStmt = db.prepare(
        "INSERT OR REPLACE INTO telemetry_real (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
      );
      insertBoolStmt = db.prepare(
        "INSERT OR REPLACE INTO telemetry_bool (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
      );
      insertStringStmt = db.prepare(
        "INSERT OR REPLACE INTO telemetry_string (ts_ms, metric_id, value, source, context) VALUES (?, ?, ?, ?, ?)",
      );
      getMetricStmt = db.prepare("SELECT id FROM metrics WHERE name = ?");
      insertMetricStmt = db.prepare(
        "INSERT INTO metrics (name) VALUES (?) RETURNING id",
      );

      // Create batch transaction function (manual for DatabaseSync)
      executeBatchTransaction = (buffer) => {
        db.exec("BEGIN TRANSACTION");
        try {
          for (const item of buffer) {
            item.stmt.run(...item.params);
          }
          db.exec("COMMIT");
        } catch (err) {
          db.exec("ROLLBACK");
          throw err;
        }
      };

      // Set up batch flush interval
      const batchWriteIntervalMs = (options.batchWriteInterval || 1) * 1000;
      flushInterval = setInterval(flushBuffer, batchWriteIntervalMs);

      // Get initial database stats
      const initialStats = getDatabaseStats();
      setStatus(
        `Ready. Database: ${formatBytes(initialStats.dbSize)}, ${initialStats.metricCount} metrics, ${initialStats.realCount} real, ${initialStats.boolCount} bool, ${initialStats.stringCount} string values${initialStats.timeRange ? ` (${initialStats.timeRange})` : ""}`,
      );

      // Register Signal K subscription stream
      const localSubscription = {
        context: "vessels.self",
        subscribe: [{ path: "*", period: options.resolution || 1000 }],
      };

      app.subscriptionmanager.subscribe(
        localSubscription,
        [],
        (subscriptionError) => {
          app.error(`Error subscribing: ${subscriptionError}`);
          setStatus(`Error subscribing: ${subscriptionError}`);
        },
        (delta) => {
          handleDelta(delta, options);
        },
      );

      // Register as a Signal K History API provider interface
      if (app.registerHistoryApiProvider) {
        app.registerHistoryApiProvider(
          new SQLiteHistoryProvider(db, app.selfId, app.debug),
        );
      }

      app.debug("signalk-history-sqlite plugin started successfully.");
    },

    stop: () => {
      _started = false;
      if (flushInterval) clearInterval(flushInterval);
      flushBuffer();

      // Get final stats
      let finalStatus = "Stopped";
      if (db) {
        const stats = getDatabaseStats();
        if (
          stats.realCount > 0 ||
          stats.boolCount > 0 ||
          stats.stringCount > 0
        ) {
          finalStatus = `Stopped. Database: ${formatBytes(stats.dbSize)}, ${stats.metricCount} metrics, ${stats.realCount + stats.boolCount + stats.stringCount} values stored${stats.timeRange ? ` (${stats.timeRange})` : ""}`;
        }
        db.close();
      }
      setStatus(finalStatus);
      app.debug("signalk-history-sqlite plugin stopped.");
    },
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

  function shouldStore(options, path) {
    if (
      typeof options.allowOrDenylist !== "undefined" &&
      typeof options.allowOrDeny !== "undefined" &&
      options.allowOrDenylist.length > 0
    ) {
      const obj = {};
      options.allowOrDenylist.forEach((element) => {
        obj[element] = true;
      });

      if (options.allowOrDeny === "Allow") {
        return typeof obj[path] !== "undefined";
      } else {
        return typeof obj[path] === "undefined";
      }
    }
    return true;
  }

  function getSourceId(update) {
    if (update.$source) {
      return update.$source;
    } else if (update.source) {
      // Try to construct a source reference from the source object
      const src = update.source;
      if (src.type && src.id) {
        return `${src.type}.${src.id}`;
      }
    }
    return "unknown";
  }

  function handleDelta(delta, options) {
    if (!delta.updates) return;

    // Resolve context
    let context = delta.context;
    if (context === "vessels.self") {
      context = selfContext;
    }

    // Skip if not storing others and context is not self
    if (!options.storeOthers && context !== selfContext) {
      return;
    }

    for (const update of delta.updates) {
      if (!update.values) continue;

      const source = getSourceId(update);
      const date = update.timestamp ? new Date(update.timestamp) : new Date();
      const ts = date.getTime();
      const resolution = options.resolution || 1000;

      for (const valObj of update.values) {
        // Check blacklist/whitelist
        if (!shouldStore(options, valObj.path)) {
          continue;
        }

        // Check resolution - skip if not enough time has passed since last write for this metric
        const metricKey = `${valObj.path}:${source}:${context}`;
        const lastTs = lastWriteTime.get(metricKey);
        if (lastTs !== undefined && ts - lastTs < resolution) {
          continue;
        }

        const value = valObj.value;

        if (typeof value === "number" && !Number.isNaN(value)) {
          const metricId = getMetricId(valObj.path);
          batchBuffer.push({
            stmt: insertRealStmt,
            params: [ts, metricId, value, source, context],
          });
          lastWriteTime.set(metricKey, ts);
        } else if (typeof value === "boolean") {
          const metricId = getMetricId(valObj.path);
          batchBuffer.push({
            stmt: insertBoolStmt,
            params: [ts, metricId, value ? 1 : 0, source, context],
          });
          lastWriteTime.set(metricKey, ts);
        } else if (typeof value === "string") {
          const metricId = getMetricId(valObj.path);
          batchBuffer.push({
            stmt: insertStringStmt,
            params: [ts, metricId, value, source, context],
          });
          lastWriteTime.set(metricKey, ts);
        } else if (value !== null && typeof value === "object") {
          // Special handling for navigation.position - store as separate latitude/longitude metrics
          if (
            valObj.path === "navigation.position" &&
            options.recordTrack !== false
          ) {
            if (
              typeof value.longitude === "number" &&
              !Number.isNaN(value.longitude)
            ) {
              const lonMetricId = getMetricId("navigation.position.longitude");
              batchBuffer.push({
                stmt: insertRealStmt,
                params: [ts, lonMetricId, value.longitude, source, context],
              });
            }
            if (
              typeof value.latitude === "number" &&
              !Number.isNaN(value.latitude)
            ) {
              const latMetricId = getMetricId("navigation.position.latitude");
              batchBuffer.push({
                stmt: insertRealStmt,
                params: [ts, latMetricId, value.latitude, source, context],
              });
            }
            // Track last write time for the parent position path
            lastWriteTime.set(metricKey, ts);
          }
        }

        // Flush if batch size reached
        if (batchBuffer.length >= (options.batchSize || 500)) {
          flushBuffer();
        }
      }
    }
  }

  let executeBatchTransaction = null;

  function flushBuffer() {
    if (batchBuffer.length === 0 || !executeBatchTransaction) return;
    const chunk = batchBuffer;
    batchBuffer = [];
    try {
      executeBatchTransaction(chunk);
      plugin.totalPointsWritten += chunk.length;
      plugin.throughputPoints += chunk.length;
      plugin.lastWriteTime = Date.now();

      // Calculate throughput
      const elapsedSeconds = (Date.now() - plugin.throughputStartTime) / 1000;
      let throughput = 0;
      if (elapsedSeconds > 0) {
        throughput = Math.round(plugin.throughputPoints / elapsedSeconds);
      }

      // Reset throughput counters every minute
      if (elapsedSeconds >= 60) {
        plugin.throughputStartTime = Date.now();
        plugin.throughputPoints = 0;
      }

      // Get database stats periodically
      const stats = getDatabaseStats();
      setStatus(
        `Writing ~${throughput} pts/s. Batch: ${chunk.length}. Total: ${plugin.totalPointsWritten} pts. DB: ${formatBytes(stats.dbSize)}, ${stats.metricCount} metrics${stats.timeRange ? ` (${stats.timeRange})` : ""}`,
      );
    } catch (err) {
      logError(`Failed to flush batch to SQLite: ${err.message}`);
      setStatus(`Error writing to SQLite: ${err.message}`);
    }
  }

  function getDatabaseStats() {
    try {
      const metricCount = db
        .prepare("SELECT COUNT(*) as count FROM metrics")
        .get().count;
      const realCount = db
        .prepare("SELECT COUNT(*) as count FROM telemetry_real")
        .get().count;
      const boolCount = db
        .prepare("SELECT COUNT(*) as count FROM telemetry_bool")
        .get().count;
      const stringCount = db
        .prepare("SELECT COUNT(*) as count FROM telemetry_string")
        .get().count;

      let dbSize = 0;
      try {
        const stats = fs.statSync(dbPath);
        dbSize = stats.size;
      } catch (_e) {
        // File stats not available
      }

      // Get time range of stored data
      let timeRange = null;
      if (realCount > 0 || stringCount > 0) {
        // Use UNION to get time range from both real and string tables
        const range = db
          .prepare(`
            SELECT MIN(ts_ms) as min_ts, MAX(ts_ms) as max_ts FROM (
              SELECT ts_ms FROM telemetry_real
              UNION ALL
              SELECT ts_ms FROM telemetry_string
            )
          `)
          .get();
        if (range.min_ts && range.max_ts) {
          const days = Math.round(
            (range.max_ts - range.min_ts) / (24 * 60 * 60 * 1000),
          );
          timeRange = `${days}d`;
        }
      }

      return {
        metricCount,
        realCount,
        boolCount,
        stringCount,
        dbSize,
        timeRange,
      };
    } catch (_err) {
      return {
        metricCount: 0,
        realCount: 0,
        boolCount: 0,
        stringCount: 0,
        dbSize: 0,
        timeRange: null,
      };
    }
  }

  function formatBytes(bytes) {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / k ** i).toFixed(1))} ${sizes[i]}`;
  }

  return plugin;
};
