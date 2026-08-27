const DEFAULT_EMA_PERIOD = 5;

// The telemetry tables the writer stores each value type in. Numeric values
// go into `telemetry_real`, booleans into `telemetry_bool`, strings into
// `telemetry_string` and other objects (notifications, rich values) into
// `telemetry_json` (stored as a JSON-encoded TEXT string). This mirrors the
// v1 influxdb writer's `value`/`boolValue`/`stringValue`/`jsonValue` fields.
const TABLE_FOR_TYPE = {
  real: "telemetry_real",
  bool: "telemetry_bool",
  string: "telemetry_string",
  json: "telemetry_json",
};

// SQLite aggregate functions that are meaningful for each value type. Only
// `first`/`last` (chronological) make sense for strings, booleans and JSON
// objects; average/min/max and the moving-average methods require numbers. Any
// other method is coerced to `first` for those types, matching how
// navigation.position and the influxdb provider are handled.

// Maps the History API aggregation methods to SQLite aggregate functions for
// numeric (`telemetry_real`) values. `first`/`last` are chronological (picked
// by timestamp within each time bucket, not by value) and handled specially in
// the query builder. `sma`/`ema` are computed in post-processing on top of
// `avg` buckets, matching the signalk-to-influxdb implementation.
const functionForAggregate = {
  average: "avg",
  min: "min",
  max: "max",
  first: "first",
  last: "last",
  sma: "avg",
  ema: "avg",
  mid: "avg",
  middle_index: "avg",
};

/**
 * Create a 2D array with specified dimensions
 * @param {number} d1 - First dimension
 * @param {number} d2 - Second dimension
 * @returns {any[][]} The created array
 */
function _makeArray(d1, d2) {
  const arr = [];
  for (let i = 0; i < d1; i++) {
    arr.push(new Array(d2));
  }
  return arr;
}

/**
 * Builds the values descriptor list for a set of path specs
 * @param {Array<Object>} pathSpecs - Array of path specifications
 * @returns {Array<Object>} Value list with path, method, and optional sourceRef
 */
function valuesForSpecs(pathSpecs) {
  return pathSpecs.map(({ path, aggregateMethod, sourceRef }) => ({
    path,
    method: aggregateMethod,
    ...(sourceRef ? { sourceRef } : {}),
  }));
}

/**
 * Resolve EMA parameters from a path specification
 * @param {Object} spec - Path specification with parameters
 * @returns {{period: number, alpha: number}} EMA period and alpha value
 */
function resolveEmaParams(spec) {
  const rawParam =
    spec.parameters.length > 0
      ? Number.parseFloat(spec.parameters[0])
      : Number.NaN;

  if (Number.isFinite(rawParam) && rawParam > 0 && rawParam < 1) {
    const alpha = rawParam;
    const period = 2 / alpha - 1;
    return { period, alpha };
  }

  const period =
    Number.isFinite(rawParam) && rawParam > 0 ? rawParam : DEFAULT_EMA_PERIOD;
  const alpha = 2 / (period + 1);
  return { period, alpha };
}

/**
 * Returns true when a path stores its values in the numeric `telemetry_real`
 * table (or has not been discovered yet, in which case we assume numeric).
 * @param {Object} spec - Path specification with a discovered `type`
 * @returns {boolean}
 */
function isNumericType(spec) {
  return spec.type === undefined || spec.type === "real";
}

/**
 * Returns the SQLite aggregate function to use for a path spec, taking its
 * discovered value type into account. Numeric specs keep the requested
 * aggregation (mapped via `functionForAggregate`); non-numeric specs only
 * support `first`/`last` and any other method is coerced to `first`.
 * @param {Object} spec - Path specification with `aggregateMethod` and `type`
 * @returns {string} SQLite aggregate function name
 */
function aggregateFunctionFor(spec) {
  if (isNumericType(spec)) {
    return functionForAggregate[spec.aggregateMethod] || "avg";
  }
  const mapped = functionForAggregate[spec.aggregateMethod];
  return mapped === "first" || mapped === "last" ? mapped : "first";
}

/**
 * Decode a raw cell value read from a telemetry table according to the value
 * type. `telemetry_json` stores JSON-encoded objects as TEXT; parse them back
 * into the original value. Booleans are stored as 0/1 INTEGERs and converted
 * back to real booleans. Everything else is returned as-is.
 * @param {any} raw - Raw value from the database
 * @param {string} [type] - Discovered value type
 * @returns {any} Decoded value
 */
function decodeValue(raw, type) {
  if (raw === null || raw === undefined) {
    return null;
  }
  if (type === "bool") {
    return raw === 1 || raw === true;
  }
  if (type === "json" && typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch (_e) {
      return raw;
    }
  }
  return raw;
}

/**
 * History API provider backed by SQLite.
 *
 * Implements the Signal K History API so that the server can serve
 * `/signalk/v2/api/history/*` from data stored in SQLite by this plugin.
 *
 * The writer stores each Signal K value in a typed telemetry table:
 * `telemetry_real` (numbers), `telemetry_bool` (booleans),
 * `telemetry_string` (strings) and `telemetry_json` (objects, JSON-encoded).
 * This provider discovers which table each requested path lives in, coerces
 * the aggregation method to one valid for that value type (only `first`/`last`
 * are meaningful for non-numeric values) and queries the appropriate table.
 *
 * @class SQLiteHistoryProvider
 */
class SQLiteHistoryProvider {
  /**
   * Create a new SQLiteHistoryProvider
   * @param {import('node:sqlite').DatabaseSync} db - SQLite database instance
   * @param {string} selfId - Vessel self ID
   * @param {function(string): void} debug - Debug logging function
   */
  constructor(db, selfId, debug) {
    this.db = db;
    this.selfId = selfId;
    this.debug = debug;
    // Cache of path -> value type ('real' | 'bool' | 'string' | 'json'),
    // populated lazily by inspecting which telemetry table holds the path's
    // metric_id. Mirrors the influxdb provider's fieldCache.
    this.typeCache = new Map();
  }

  /**
   * Discover which telemetry table each non-position path stores its values
   * in, cache the result and rewrite each path spec's `aggregateFunction` to a
   * selector valid for that type. Numeric (`telemetry_real`) specs keep the
   * requested aggregation; non-numeric specs are restricted to `first`/`last`
   * and any other method is coerced to `first`.
   * @private
   * @param {Array<Object>} pathSpecs - Path specifications to resolve
   * @returns {Promise<void>}
   */
  async resolveTypes(pathSpecs) {
    const uniquePaths = Array.from(
      new Set(
        pathSpecs
          .filter(({ path }) => path !== "navigation.position")
          .map(({ path }) => path),
      ),
    );
    if (uniquePaths.length === 0) {
      return;
    }
    await Promise.all(
      uniquePaths.map(async (path) => {
        const type = await this.discoverType(path);
        pathSpecs.forEach((spec) => {
          if (spec.path === path && spec.path !== "navigation.position") {
            spec.type = type;
            spec.aggregateFunction = aggregateFunctionFor(spec);
          }
        });
      }),
    );
  }

  /**
   * Returns the value type a path stores its values as, by looking up the
   * path's metric_id and checking which telemetry table contains rows for
   * it. The result is cached per path. When no data has been stored yet (the
   * metric does not exist or no telemetry rows are found) the numeric
   * `real` default is assumed, matching the historic behaviour.
   * @private
   * @param {string} path - Signal K path
   * @returns {Promise<string>} One of 'real', 'bool', 'string', 'json'
   */
  async discoverType(path) {
    const cached = this.typeCache.get(path);
    if (cached) {
      return cached;
    }
    let type = "real";
    try {
      const metric = this.db
        .prepare("SELECT id FROM metrics WHERE name = ?")
        .get(path);
      if (metric) {
        const metricId = metric.id;
        // Check each telemetry table for rows belonging to this metric. A
        // missing table (e.g. an older database without telemetry_json) is
        // skipped rather than aborting the whole lookup.
        for (const t of ["bool", "string", "json", "real"]) {
          const table = TABLE_FOR_TYPE[t];
          try {
            const row = this.db
              .prepare(`SELECT 1 FROM ${table} WHERE metric_id = ? LIMIT 1`)
              .get(metricId);
            if (row) {
              type = t;
              break;
            }
          } catch (_e) {
            // Table does not exist in this database; assume the metric has no
            // values of this type and continue.
          }
        }
      }
    } catch (e) {
      this.debug(`discoverType failed for ${path}: ${e.message}`);
    }
    this.typeCache.set(path, type);
    return type;
  }

  /**
   * Get historical values for specified paths and time range
   * @param {Object} query - History API values query
   * @param {string} query.context - Signal K context
   * @param {Array<Object>} query.pathSpecs - Path specifications
   * @param {number} [query.resolution] - Time resolution in milliseconds
   * @param {string|number|Object} query.from - Start of time range
   * @param {string|number|Object} [query.to] - End of time range
   * @param {string|number|Object} [query.duration] - Duration of time range
   * @returns {Promise<Object>} Values response with context, range, values, and data
   */
  async getValues(query) {
    const { from, to } = getTimeRange(query);
    const context =
      (query.context === "vessels.self"
        ? `vessels.${this.selfId}`
        : query.context) || `vessels.${this.selfId}`;
    const resolution =
      query.resolution || (to.getTime() - from.getTime()) / 1000;

    // Convert the server-parsed pathSpecs into the internal format.
    const pathSpecs = query.pathSpecs.map((spec) => {
      const sourceRef = spec.sourceRef;
      return {
        path: spec.path,
        aggregateMethod: spec.aggregate,
        aggregateFunction: functionForAggregate[spec.aggregate] || "avg",
        parameters: spec.parameter || [],
        ...(sourceRef ? { sourceRef } : {}),
      };
    });

    // Discover which telemetry table each non-position path stores its values
    // in, then adjust the aggregate function accordingly: only `first`/`last`
    // are meaningful for textual, boolean and JSON values, so any numeric-only
    // method is coerced to `first` for those types.
    await this.resolveTypes(pathSpecs);

    const positionPathSpecs = pathSpecs
      .filter(({ path }) => path === "navigation.position")
      .slice(0, 1);
    const nonPositionPathSpecs = pathSpecs.filter(
      ({ path }) => path !== "navigation.position",
    );
    const needsCollation =
      nonPositionPathSpecs.length > 0 && positionPathSpecs.length > 0;

    // Calculate extended query window for SMA and EMA. Only numeric fields
    // support moving averages; non-numeric specs were coerced to `first`.
    const maxSmaWindow = nonPositionPathSpecs.reduce((max, spec) => {
      if (spec.aggregateMethod === "sma" && isNumericType(spec)) {
        const windowSize =
          spec.parameters.length > 0 ? parseInt(spec.parameters[0], 10) : 5;
        return Math.max(max, windowSize);
      }
      return max;
    }, 0);

    const maxEmaWindow = nonPositionPathSpecs.reduce((max, spec) => {
      if (spec.aggregateMethod === "ema" && isNumericType(spec)) {
        const { period } = resolveEmaParams(spec);
        return Math.max(max, Math.ceil(period * 4));
      }
      return max;
    }, 0);

    const maxWindow = Math.max(maxSmaWindow, maxEmaWindow);
    const extendedFrom =
      maxWindow > 0
        ? new Date(from.getTime() - maxWindow * resolution * 1000)
        : from;

    const positionResult = positionPathSpecs.length
      ? this.getPositions(
          context,
          from,
          to,
          resolution * 1000,
          needsCollation,
          positionPathSpecs[0].sourceRef,
        )
      : Promise.resolve({ values: [], data: [] });

    const nonPositionResult = nonPositionPathSpecs.length
      ? this.getNumericValues(
          context,
          extendedFrom,
          to,
          resolution * 1000,
          nonPositionPathSpecs,
          needsCollation,
        )
      : Promise.resolve({ values: [], data: [] });

    const [posResult, nonPosResult] = await Promise.all([
      positionResult,
      nonPositionResult,
    ]);

    // Apply SMA and EMA post-processing if needed.
    let processedNonPosResult = nonPosResult;
    if (
      (maxSmaWindow > 0 || maxEmaWindow > 0) &&
      nonPosResult.data.length > 0
    ) {
      processedNonPosResult = applyMovingAveragePostProcessing(
        nonPosResult,
        nonPositionPathSpecs,
        from.toISOString(),
      );
    }

    // Collate by timestamp (union of timestamps from both sources), not by row
    // order. This avoids mismatches when one query returns extra rows.
    const data = [];
    let values = [];

    const positionByTs = new Map();
    posResult.data.forEach((row) => {
      const ts = row[0];
      const pos = row[1];
      if (!Array.isArray(pos)) {
        return;
      }
      const [lon, lat] = pos;
      if (
        (lon === null || lon === undefined) &&
        (lat === null || lat === undefined)
      ) {
        return;
      }
      positionByTs.set(ts, [lon, lat]);
    });

    const numericByTs = new Map();
    processedNonPosResult.data.forEach((row) => {
      const ts = row[0];
      const numericValues = row.slice(1);
      const existing = numericByTs.get(ts);
      if (!existing) {
        numericByTs.set(ts, [...numericValues]);
        return;
      }
      for (let k = 0; k < numericValues.length; k++) {
        if (
          (existing[k] === null || existing[k] === undefined) &&
          numericValues[k] !== null &&
          numericValues[k] !== undefined
        ) {
          existing[k] = numericValues[k];
        }
      }
    });

    // Values list ordering: position (if requested) first, then numeric.
    if (positionPathSpecs.length > 0 && posResult.values.length > 0) {
      values = values.concat(posResult.values);
    }
    if (
      nonPositionPathSpecs.length > 0 &&
      processedNonPosResult.values.length > 0
    ) {
      values = values.concat(processedNonPosResult.values);
    }

    // Union timestamps from both results.
    const tsSet = new Set();
    posResult.data.forEach((r) => {
      tsSet.add(r[0]);
    });
    processedNonPosResult.data.forEach((r) => {
      tsSet.add(r[0]);
    });
    const timestamps = Array.from(tsSet).sort();

    const numericWidth = nonPositionPathSpecs.length;
    timestamps.forEach((ts) => {
      const row = [ts];

      if (positionPathSpecs.length > 0) {
        row.push(positionByTs.get(ts) ?? null);
      }
      if (nonPositionPathSpecs.length > 0) {
        const nv = numericByTs.get(ts);
        if (nv) {
          row.push(...nv);
        } else {
          row.push(...new Array(numericWidth).fill(null));
        }
      }

      const hasAnyValue = row.slice(1).some((v) => {
        if (Array.isArray(v)) {
          return v.some((x) => x !== null && x !== undefined);
        }
        return v !== null && v !== undefined;
      });

      if (hasAnyValue) {
        data.push(row);
      }
    });

    return {
      context,
      range: {
        from: from.toISOString(),
        to: to.toISOString(),
      },
      values,
      data,
    };
  }

  /**
   * Get contexts that have historical data in the time range
   * @param {Object} query - Contexts query with from/to/duration
   * @returns {Promise<Array<string>>} Array of context strings
   */
  async getContexts(query) {
    const { from, to } = getTimeRange(query);
    const fromMs = from.getTime();
    const toMs = to.getTime();

    const tables = this.existingTelemetryTables();
    if (tables.length === 0) {
      return [];
    }
    const selects = tables
      .map(
        (t) =>
          `SELECT DISTINCT context FROM ${t} WHERE ts_ms >= ? AND ts_ms <= ?`,
      )
      .join(" UNION ");
    const params = [];
    for (let i = 0; i < tables.length; i++) {
      params.push(fromMs, toMs);
    }
    const result = this.db.prepare(selects).all(...params);
    return result.map((row) => row.context);
  }

  /**
   * Get paths that have historical data in the time range
   * @param {Object} query - Paths query with from/to/duration
   * @returns {Promise<Array<string>>} Array of path strings
   */
  async getPaths(query) {
    const { from, to } = getTimeRange(query);
    const fromMs = from.getTime();
    const toMs = to.getTime();

    const tables = this.existingTelemetryTables();
    if (tables.length === 0) {
      return [];
    }
    const existsClauses = tables
      .map(
        (t) =>
          `EXISTS (SELECT 1 FROM ${t} x WHERE x.metric_id = m.id AND x.ts_ms >= ? AND x.ts_ms <= ?)`,
      )
      .join(" OR ");
    const params = [];
    for (let i = 0; i < tables.length; i++) {
      params.push(fromMs, toMs);
    }
    const result = this.db
      .prepare(`
        SELECT DISTINCT m.name
        FROM metrics m
        WHERE ${existsClauses}
      `)
      .all(...params);

    return result.map((row) => row.name);
  }

  /**
   * Returns the subset of telemetry tables that exist in this database, so
   * `getContexts`/`getPaths` degrade gracefully on databases created before
   * all typed tables were introduced.
   * @private
   * @returns {string[]} Existing telemetry table names
   */
  existingTelemetryTables() {
    if (this._existingTables) {
      return this._existingTables;
    }
    const rows = this.db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'telemetry_%'",
      )
      .all();
    this._existingTables = rows.map((r) => r.name);
    return this._existingTables;
  }

  /**
   * Get position data for the specified context and time range
   * @private
   * @param {string} context - Signal K context
   * @param {Date} from - Start time
   * @param {Date} to - End time
   * @param {number} timeResolutionMillis - Time resolution in milliseconds
   * @param {boolean} needsCollation - Whether collation with other data is needed
   * @param {string} [sourceRef] - Optional source reference filter
   * @returns {Promise<Object>} Data result with values and data arrays
   */
  async getPositions(
    context,
    from,
    to,
    timeResolutionMillis,
    _needsCollation,
    sourceRef,
  ) {
    const fromMs = from.getTime();
    const toMs = to.getTime();
    const bucketSize = Math.floor(timeResolutionMillis);

    let query;
    let params;

    if (sourceRef) {
      query = `
        SELECT
          datetime((t.ts_ms / CAST(? AS INTEGER)) * CAST(? AS INTEGER) / 1000, 'unixepoch') AS time,
          MIN(CASE WHEN m.name = 'navigation.position.longitude' THEN t.value END) AS longitude,
          MIN(CASE WHEN m.name = 'navigation.position.latitude' THEN t.value END) AS latitude
        FROM telemetry_real t
        JOIN metrics m ON t.metric_id = m.id
        WHERE
          t.context = ?
          AND
          t.ts_ms >= ?
          AND
          t.ts_ms <= ?
          AND
          (m.name = 'navigation.position.longitude' OR m.name = 'navigation.position.latitude')
          AND
          t.source = ?
        GROUP BY time
        ORDER BY time ASC
      `;
      params = [bucketSize, bucketSize, context, fromMs, toMs, sourceRef];
    } else {
      query = `
        SELECT
          datetime((t.ts_ms / CAST(? AS INTEGER)) * CAST(? AS INTEGER) / 1000, 'unixepoch') AS time,
          MIN(CASE WHEN m.name = 'navigation.position.longitude' THEN t.value END) AS longitude,
          MIN(CASE WHEN m.name = 'navigation.position.latitude' THEN t.value END) AS latitude
        FROM telemetry_real t
        JOIN metrics m ON t.metric_id = m.id
        WHERE
          t.context = ?
          AND
          t.ts_ms >= ?
          AND
          t.ts_ms <= ?
          AND
          (m.name = 'navigation.position.longitude' OR m.name = 'navigation.position.latitude')
        GROUP BY time
        ORDER BY time ASC
      `;
      params = [bucketSize, bucketSize, context, fromMs, toMs];
    }

    this.debug(query);

    const rows = this.db.prepare(query).all(...params);

    return {
      values: [
        {
          path: "navigation.position",
          method: "first",
          ...(sourceRef ? { sourceRef } : {}),
        },
      ],
      data: rows
        .filter((row) => row.longitude !== null && row.latitude !== null)
        .map((row) => [
          new Date(`${row.time}Z`).toISOString(),
          [row.longitude, row.latitude],
        ]),
    };
  }

  /**
   * Get values for specified non-position paths
   * @private
   * @param {string} context - Signal K context
   * @param {Date} from - Start time
   * @param {Date} to - End time
   * @param {number} timeResolutionMillis - Time resolution in milliseconds
   * @param {Array<Object>} pathSpecs - Path specifications
   * @param {boolean} needsCollation - Whether collation with other data is needed
   * @returns {Promise<Object>} Data result with values and data arrays
   */
  async getNumericValues(
    context,
    from,
    to,
    timeResolutionMillis,
    pathSpecs,
    needsCollation,
  ) {
    const distinctSourceRefs = new Set(pathSpecs.map((ps) => ps.sourceRef));

    // Common case: all paths share a single source (or none). A single query
    // group suffices and the result layout is identical to the unfiltered
    // behaviour.
    if (distinctSourceRefs.size <= 1) {
      const sourceRef = pathSpecs[0]?.sourceRef;
      return this.querySourceGroup(
        context,
        from,
        to,
        timeResolutionMillis,
        pathSpecs,
        needsCollation,
        sourceRef,
      );
    }

    // Mixed sources: each distinct sourceRef needs its own query group.
    const groups = new Map();
    pathSpecs.forEach((ps, i) => {
      let group = groups.get(ps.sourceRef);
      if (!group) {
        group = { specs: [], indices: [] };
        groups.set(ps.sourceRef, group);
      }
      group.specs.push(ps);
      group.indices.push(i);
    });

    const groupPromises = Array.from(groups.values()).map((group) =>
      this.querySourceGroup(
        context,
        from,
        to,
        timeResolutionMillis,
        group.specs,
        needsCollation,
        group.specs[0].sourceRef,
      ).then((result) => ({ result, indices: group.indices })),
    );

    return Promise.all(groupPromises).then((groupResults) => {
      const tsSet = new Set();
      groupResults.forEach(({ result }) => {
        result.data.forEach((r) => {
          tsSet.add(r[0]);
        });
      });
      const allTs = Array.from(tsSet).sort();
      const rowByTs = new Map();
      allTs.forEach((ts) => {
        const row = new Array(pathSpecs.length + 1).fill(null);
        row[0] = ts;
        rowByTs.set(ts, row);
      });

      groupResults.forEach(({ result, indices }) => {
        result.data.forEach((groupRow) => {
          const ts = groupRow[0];
          const target = rowByTs.get(ts);
          if (!target) {
            return;
          }
          indices.forEach((originalIndex, groupColumn) => {
            target[originalIndex + 1] = groupRow[groupColumn + 1] ?? null;
          });
        });
      });

      return {
        values: valuesForSpecs(pathSpecs),
        data: allTs.map((ts) => rowByTs.get(ts)),
      };
    });
  }

  /**
   * Query a group of paths with the same source reference
   * @private
   * @param {string} context - Signal K context
   * @param {Date} from - Start time
   * @param {Date} to - End time
   * @param {number} timeResolutionMillis - Time resolution in milliseconds
   * @param {Array<Object>} pathSpecs - Path specifications
   * @param {boolean} needsCollation - Whether collation is needed
   * @param {string} [sourceRef] - Optional source reference
   * @returns {Promise<Object>} Data result with values and data arrays
   */
  async querySourceGroup(
    context,
    from,
    to,
    timeResolutionMillis,
    pathSpecs,
    _needsCollation,
    sourceRef,
  ) {
    // When every spec reads the numeric `telemetry_real` table AND none needs
    // chronological first/last (which SQLite has no aggregate for) we can use a
    // single grouped query (the original, fast path). Otherwise we run one
    // query per path and collate the results by timestamp, since each value
    // type lives in its own table and first/last need a window-function
    // subquery.
    const allNumeric = pathSpecs.every((ps) => isNumericType(ps));
    const needsChronological = pathSpecs.some(
      (ps) =>
        ps.aggregateFunction === "first" || ps.aggregateFunction === "last",
    );
    if (!allNumeric || needsChronological) {
      return this.querySourceGroupPerPath(
        context,
        from,
        to,
        timeResolutionMillis,
        pathSpecs,
        sourceRef,
      );
    }
    return this.queryNumericGroup(
      context,
      from,
      to,
      timeResolutionMillis,
      pathSpecs,
      sourceRef,
    );
  }

  /**
   * Run a single grouped query against `telemetry_real` for path specs that
   * all read numeric values. This is the original fast path.
   * @private
   */
  async queryNumericGroup(
    context,
    from,
    to,
    timeResolutionMillis,
    pathSpecs,
    sourceRef,
  ) {
    const start = Date.now();
    const fromMs = from.getTime();
    const toMs = to.getTime();

    const uniquePaths = pathSpecs.reduce((acc, ps) => {
      if (acc.indexOf(ps.path) === -1) {
        acc.push(ps.path);
      }
      return acc;
    }, []);

    const bucketSize = Math.floor(timeResolutionMillis);

    // Build CASE statements with parameterized paths. The fast path only
    // handles the plain SQLite aggregates (avg/min/max); chronological
    // first/last are routed to the per-path query path.
    const selectClauses = [];
    pathSpecs.forEach((ps, idx) => {
      selectClauses.push(
        `${ps.aggregateFunction.toUpperCase()}(CASE WHEN m.name = ? THEN t.value END) AS "col_${idx}"`,
      );
    });

    const inPlaceholders = uniquePaths.map(() => "?").join(", ");

    let query;
    let params;

    if (sourceRef) {
      query = `
        SELECT
          datetime((t.ts_ms / CAST(? AS INTEGER)) * CAST(? AS INTEGER) / 1000, 'unixepoch') AS time,
          ${selectClauses.join(",\n        ")}
        FROM telemetry_real t
        JOIN metrics m ON t.metric_id = m.id
        WHERE
          t.context = ?
          AND
          t.ts_ms >= ?
          AND
          t.ts_ms <= ?
          AND
          m.name IN (${inPlaceholders})
          AND
          t.source = ?
        GROUP BY time
        ORDER BY time ASC
      `;
      params = [
        bucketSize,
        bucketSize,
        ...pathSpecs.map((ps) => ps.path),
        context,
        fromMs,
        toMs,
        ...uniquePaths,
        sourceRef,
      ];
    } else {
      query = `
        SELECT
          datetime((t.ts_ms / CAST(? AS INTEGER)) * CAST(? AS INTEGER) / 1000, 'unixepoch') AS time,
          ${selectClauses.join(",\n        ")}
        FROM telemetry_real t
        JOIN metrics m ON t.metric_id = m.id
        WHERE
          t.context = ?
          AND
          t.ts_ms >= ?
          AND
          t.ts_ms <= ?
          AND
          m.name IN (${inPlaceholders})
        GROUP BY time
        ORDER BY time ASC
      `;
      params = [
        bucketSize,
        bucketSize,
        ...pathSpecs.map((ps) => ps.path),
        context,
        fromMs,
        toMs,
        ...uniquePaths,
      ];
    }

    this.debug(query);

    const rows = this.db.prepare(query).all(...params);
    this.debug(`got ${rows.length} rows in ${Date.now() - start}ms`);

    const resultData = rows.map((row) => {
      const dataRow = [new Date(`${row.time}Z`).toISOString()];
      for (let i = 0; i < pathSpecs.length; i++) {
        dataRow.push(row[`col_${i}`] ?? null);
      }
      return dataRow;
    });

    this.debug(`rows done ${Date.now() - start}ms`);

    return {
      values: valuesForSpecs(pathSpecs),
      data: resultData,
    };
  }

  /**
   * Run one query per path (against the path's own telemetry table) and
   * collate the results by timestamp into the original column order of
   * `pathSpecs`. Used when one or more specs read a non-numeric table, since
   * each value type lives in its own table and needs type-specific decoding.
   *
   * `first`/`last` are chronological (picked by timestamp within each time
   * bucket); the numeric aggregates `avg`/`min`/`max` map to SQLite functions
   * over the bucket. Duplicate specs (same path+type+aggregate) collapse into
   * a single query whose value fans out to every output column it feeds.
   * @private
   */
  async querySourceGroupPerPath(
    context,
    from,
    to,
    timeResolutionMillis,
    pathSpecs,
    sourceRef,
  ) {
    const fromMs = from.getTime();
    const toMs = to.getTime();
    const bucketSize = Math.floor(timeResolutionMillis);

    // Collapse duplicate specs into a single query, remembering which output
    // columns each query feeds so we can fan values out.
    const perPath = new Map();
    pathSpecs.forEach((ps, i) => {
      const key = `${ps.path}|${ps.type}|${ps.aggregateFunction}`;
      let entry = perPath.get(key);
      if (!entry) {
        entry = { spec: ps, indices: [] };
        perPath.set(key, entry);
      }
      entry.indices.push(i);
    });

    const queryPromises = Array.from(perPath.values()).map(
      ({ spec, indices }) =>
        this.querySinglePath(
          context,
          fromMs,
          toMs,
          bucketSize,
          spec,
          sourceRef,
        ).then((rows) => ({ spec, indices, rows })),
    );

    const results = await Promise.all(queryPromises);

    const tsSet = new Set();
    results.forEach(({ rows }) => {
      rows.forEach((r) => {
        tsSet.add(toIso(r.time));
      });
    });
    const allTs = Array.from(tsSet).sort();
    const rowByTs = new Map();
    allTs.forEach((ts) => {
      const row = new Array(pathSpecs.length + 1).fill(null);
      row[0] = ts;
      rowByTs.set(ts, row);
    });

    results.forEach(({ spec, indices, rows }) => {
      rows.forEach((row) => {
        const ts = toIso(row.time);
        const target = rowByTs.get(ts);
        if (!target) {
          return;
        }
        const decoded = decodeValue(row.value, spec.type);
        indices.forEach((originalIndex) => {
          target[originalIndex + 1] = decoded;
        });
      });
    });

    return {
      values: valuesForSpecs(pathSpecs),
      data: allTs.map((ts) => rowByTs.get(ts)),
    };
  }

  /**
   * Run a single bucketed query for one path against its telemetry table,
   * returning rows of `{ time, value }` where `time` is an ISO timestamp and
   * `value` is the raw (undecoded) aggregated cell value.
   * @private
   * @param {string} context - Signal K context
   * @param {number} fromMs - Start time in epoch milliseconds
   * @param {number} toMs - End time in epoch milliseconds
   * @param {number} bucketSize - Bucket size in milliseconds
   * @param {Object} spec - Resolved path spec with `path`, `type`, `aggregateFunction`
   * @param {string} [sourceRef] - Optional source reference filter
   * @returns {Promise<Array<{time: string, value: any}>>} Bucketed rows
   */
  async querySinglePath(context, fromMs, toMs, bucketSize, spec, sourceRef) {
    const table = TABLE_FOR_TYPE[spec.type] || TABLE_FOR_TYPE.real;
    const fn = spec.aggregateFunction;
    const metricId = this.metricIdFor(spec.path);

    // The path has never been recorded (no metrics row), so it cannot have
    // data in any telemetry table. Return no rows rather than binding an
    // `undefined` metric_id, which node:sqlite rejects.
    if (metricId === undefined) {
      return [];
    }

    if (fn === "first" || fn === "last") {
      // Chronological first/last: pick the value of the row with the
      // smallest/largest ts_ms within each time bucket using a window
      // function, then collapse to one row per bucket. SQLite has no
      // FIRST()/LAST() aggregate, so we rank rows within each bucket by
      // ts_ms and select the value where the rank is 1.
      const order = fn === "first" ? "ASC" : "DESC";
      const query = `
        SELECT datetime(bucket_ms / 1000, 'unixepoch') AS time,
               MAX(CASE WHEN rn = 1 THEN value END) AS value
        FROM (
          SELECT
            (ts_ms / CAST(? AS INTEGER)) * CAST(? AS INTEGER) AS bucket_ms,
            value,
            ROW_NUMBER() OVER (
              PARTITION BY (ts_ms / CAST(? AS INTEGER)) * CAST(? AS INTEGER)
              ORDER BY ts_ms ${order}
            ) AS rn
          FROM ${table}
          WHERE context = ?
            AND ts_ms >= ?
            AND ts_ms <= ?
            AND metric_id = ?
            ${sourceRef ? "AND source = ?" : ""}
        )
        GROUP BY bucket_ms
        ORDER BY time ASC
      `;
      const params = sourceRef
        ? [
            bucketSize,
            bucketSize,
            bucketSize,
            bucketSize,
            context,
            fromMs,
            toMs,
            metricId,
            sourceRef,
          ]
        : [
            bucketSize,
            bucketSize,
            bucketSize,
            bucketSize,
            context,
            fromMs,
            toMs,
            metricId,
          ];
      this.debug(query);
      return this.db.prepare(query).all(...params);
    }

    // Plain aggregate (avg/min/max) over the bucket.
    const query = `
      SELECT
        datetime((t.ts_ms / CAST(? AS INTEGER)) * CAST(? AS INTEGER) / 1000, 'unixepoch') AS time,
        ${fn.toUpperCase()}(t.value) AS value
      FROM ${table} t
      WHERE
        t.context = ?
        AND
        t.ts_ms >= ?
        AND
        t.ts_ms <= ?
        AND
        t.metric_id = ?
        ${sourceRef ? "AND t.source = ?" : ""}
      GROUP BY time
      ORDER BY time ASC
    `;
    const params = sourceRef
      ? [bucketSize, bucketSize, context, fromMs, toMs, metricId, sourceRef]
      : [bucketSize, bucketSize, context, fromMs, toMs, metricId];
    this.debug(query);
    return this.db.prepare(query).all(...params);
  }

  /**
   * Resolve a path to its metric_id, caching the result. Returns the id or
   * undefined if the path has no metric row (no data stored yet).
   * @private
   * @param {string} path - Signal K path
   * @returns {number|undefined} metric id
   */
  metricIdFor(path) {
    if (this._metricIdCache?.has(path)) {
      return this._metricIdCache.get(path);
    }
    const row = this.db
      .prepare("SELECT id FROM metrics WHERE name = ?")
      .get(path);
    const id = row ? row.id : undefined;
    if (!this._metricIdCache) {
      this._metricIdCache = new Map();
    }
    this._metricIdCache.set(path, id);
    return id;
  }
}

/**
 * Resolves the from/to Dates from a History API request's time range
 * parameters.
 *
 * @param {Object} query - Query object with from/to/duration
 * @returns {{from: Date, to: Date}} Time range
 */
function getTimeRange(query) {
  if (query.duration !== undefined) {
    const durationMs =
      typeof query.duration === "number"
        ? query.duration * 1000
        : query.duration.total("milliseconds");

    if (query.from !== undefined) {
      const from = new Date(query.from.toString());
      const to = new Date(from.getTime() + durationMs);
      return { from, to };
    } else if (query.to !== undefined) {
      const to = new Date(query.to.toString());
      const from = new Date(to.getTime() - durationMs);
      return { from, to };
    } else {
      const to = new Date();
      const from = new Date(to.getTime() - durationMs);
      return { from, to };
    }
  } else if (query.from !== undefined) {
    const from = new Date(query.from.toString());
    const to =
      query.to !== undefined ? new Date(query.to.toString()) : new Date();
    return { from, to };
  }

  throw new Error("Invalid time range parameters");
}

/**
 * Apply SMA and EMA post-processing to result data. Non-numeric specs (which
 * were coerced to `first`) are skipped; only numeric SMA/EMA columns are
 * processed.
 *
 * @param {Object} result - Data result with values and data
 * @param {Array<Object>} pathSpecs - Path specifications
 * @param {string} requestedFromTimestamp - Requested start timestamp
 * @returns {Object} Processed data result
 */
function applyMovingAveragePostProcessing(
  result,
  pathSpecs,
  requestedFromTimestamp,
) {
  const data = result.data;

  const smaIndices = pathSpecs
    .map((spec, idx) => ({ spec, idx }))
    .filter(
      ({ spec }) => spec.aggregateMethod === "sma" && isNumericType(spec),
    );
  const emaIndices = pathSpecs
    .map((spec, idx) => ({ spec, idx }))
    .filter(
      ({ spec }) => spec.aggregateMethod === "ema" && isNumericType(spec),
    );

  if (smaIndices.length === 0 && emaIndices.length === 0) {
    const requestedFromMs = new Date(requestedFromTimestamp).toISOString();
    const trimmedData = data.filter((row) => row[0] >= requestedFromMs);
    return { values: result.values, data: trimmedData };
  }

  const processedData = data.map((row) => [...row]);

  // Calculate SMA for each SMA column.
  smaIndices.forEach(({ spec, idx }) => {
    const windowSize =
      spec.parameters.length > 0 ? parseInt(spec.parameters[0], 10) : 5;
    const columnIndex = idx + 1;

    for (let i = 0; i < processedData.length; i++) {
      const startIdx = Math.max(0, i - windowSize + 1);
      const vals = [];

      for (let j = startIdx; j <= i; j++) {
        const value = data[j][columnIndex];
        if (
          value !== null &&
          value !== undefined &&
          typeof value === "number"
        ) {
          vals.push(value);
        }
      }

      if (vals.length > 0) {
        processedData[i][columnIndex] =
          vals.reduce((acc, val) => acc + val, 0) / vals.length;
      } else {
        processedData[i][columnIndex] = null;
      }
    }
  });

  // Calculate EMA for each EMA column.
  emaIndices.forEach(({ spec, idx }) => {
    const { period, alpha } = resolveEmaParams(spec);
    const columnIndex = idx + 1;

    // Use 3x period for initial SMA to seed the EMA.
    const initialSmaWindow = Math.max(1, Math.round(period * 3));
    let ema = null;

    for (let i = 0; i < processedData.length; i++) {
      const currentValue = data[i][columnIndex];

      if (
        currentValue === null ||
        currentValue === undefined ||
        typeof currentValue !== "number"
      ) {
        // Carry forward last EMA when current value is null.
        processedData[i][columnIndex] = ema;
        continue;
      }

      if (ema === null) {
        // Initialize EMA with SMA of first N values.
        if (i >= initialSmaWindow - 1) {
          const startIdx = Math.max(0, i - initialSmaWindow + 1);
          const vals = [];

          for (let j = startIdx; j <= i; j++) {
            const value = data[j][columnIndex];
            if (
              value !== null &&
              value !== undefined &&
              typeof value === "number"
            ) {
              vals.push(value);
            }
          }

          if (vals.length > 0) {
            ema = vals.reduce((acc, val) => acc + val, 0) / vals.length;
            processedData[i][columnIndex] = ema;
          } else {
            processedData[i][columnIndex] = null;
          }
        } else {
          // Not enough data yet for initial SMA.
          processedData[i][columnIndex] = null;
        }
      } else {
        // EMA_t = α * Value_t + (1 - α) * EMA_{t-1}
        ema = alpha * currentValue + (1 - alpha) * ema;
        processedData[i][columnIndex] = ema;
      }
    }
  });

  // Trim to requested time range.
  const requestedFromMs = new Date(requestedFromTimestamp).toISOString();
  const trimmedData = processedData.filter((row) => row[0] >= requestedFromMs);

  return {
    values: result.values,
    data: trimmedData,
  };
}

/**
 * Convert a SQLite `datetime(...)` text result (`YYYY-MM-DD HH:MM:SS`, in
 * UTC) into an ISO-8601 timestamp string, matching the format the fast path
 * produces via `new Date(...).toISOString()`.
 * @param {string} time - Raw SQLite datetime text
 * @returns {string} ISO timestamp
 */
function toIso(time) {
  return new Date(`${time}Z`).toISOString();
}

module.exports = { SQLiteHistoryProvider };
