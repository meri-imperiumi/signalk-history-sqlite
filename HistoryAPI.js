const DEFAULT_EMA_PERIOD = 5;

// Maps the History API aggregation methods to SQLite aggregate functions.
const functionForAggregate = {
  average: "avg",
  min: "min",
  max: "max",
  first: "min",
  last: "max",
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
 * History API provider backed by SQLite.
 *
 * Implements the Signal K History API so that the server can serve
 * `/signalk/v2/api/history/*` from data stored in SQLite by this plugin.
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

    const positionPathSpecs = pathSpecs
      .filter(({ path }) => path === "navigation.position")
      .slice(0, 1);
    const nonPositionPathSpecs = pathSpecs.filter(
      ({ path }) => path !== "navigation.position",
    );
    const needsCollation =
      nonPositionPathSpecs.length > 0 && positionPathSpecs.length > 0;

    // Calculate extended query window for SMA and EMA.
    const maxSmaWindow = nonPositionPathSpecs.reduce((max, spec) => {
      if (spec.aggregateMethod === "sma") {
        const windowSize =
          spec.parameters.length > 0 ? parseInt(spec.parameters[0], 10) : 5;
        return Math.max(max, windowSize);
      }
      return max;
    }, 0);

    const maxEmaWindow = nonPositionPathSpecs.reduce((max, spec) => {
      if (spec.aggregateMethod === "ema") {
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

    const result = this.db
      .prepare(`
      SELECT DISTINCT context
      FROM telemetry_real
      WHERE ts_ms >= ? AND ts_ms <= ?
      UNION
      SELECT DISTINCT context
      FROM telemetry_bool
      WHERE ts_ms >= ? AND ts_ms <= ?
    `)
      .all(fromMs, toMs, fromMs, toMs);

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

    const result = this.db
      .prepare(`
      SELECT DISTINCT m.name
      FROM metrics m
      WHERE EXISTS (
        SELECT 1 FROM telemetry_real t
        WHERE t.metric_id = m.id
        AND t.ts_ms >= ? AND t.ts_ms <= ?
      )
      OR EXISTS (
        SELECT 1 FROM telemetry_bool t
        WHERE t.metric_id = m.id
        AND t.ts_ms >= ? AND t.ts_ms <= ?
      )
    `)
      .all(fromMs, toMs, fromMs, toMs);

    return result.map((row) => row.name);
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
          datetime((t.ts_ms / ?) * ? / 1000, 'unixepoch') AS time,
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
          datetime((t.ts_ms / ?) * ? / 1000, 'unixepoch') AS time,
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
   * Get numeric values for specified paths
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
    const _fromMs = from.getTime();
    const _toMs = to.getTime();

    const distinctSourceRefs = new Set(pathSpecs.map((ps) => ps.sourceRef));

    // Common case: all paths share a single source (or none). A single query
    // suffices and the result layout is identical to the unfiltered behaviour.
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

    // Mixed sources: each distinct sourceRef needs its own query.
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
    const start = Date.now();
    const fromMs = from.getTime();
    const toMs = to.getTime();

    const uniquePaths = pathSpecs.reduce((acc, ps) => {
      if (acc.indexOf(ps.path) === -1) {
        acc.push(ps.path);
      }
      return acc;
    }, []);
    const _uniqueAggregates = pathSpecs.reduce((acc, ps) => {
      if (acc.indexOf(ps.aggregateFunction) === -1) {
        acc.push(ps.aggregateFunction);
      }
      return acc;
    }, []);

    const bucketSize = Math.floor(timeResolutionMillis);

    // Build CASE statements with parameterized paths
    const selectClauses = [];
    pathSpecs.forEach((ps, idx) => {
      selectClauses.push(
        `${ps.aggregateFunction}(CASE WHEN m.name = ? THEN t.value END) AS "col_${idx}"`,
      );
    });

    // Build IN clause placeholders
    const inPlaceholders = uniquePaths.map(() => '?').join(', ');

    // Build query with parameters
    let query;
    let params;

    if (sourceRef) {
      query = `
        SELECT
          datetime((t.ts_ms / ?) * ? / 1000, 'unixepoch') AS time,
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
      // Build params: bucketSize (x2), pathSpec paths (for CASE), context, fromMs, toMs, uniquePaths (for IN), sourceRef
      params = [
        bucketSize, bucketSize,
        ...pathSpecs.map(ps => ps.path),
        context,
        fromMs, toMs,
        ...uniquePaths,
        sourceRef
      ];
    } else {
      query = `
        SELECT
          datetime((t.ts_ms / ?) * ? / 1000, 'unixepoch') AS time,
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
        bucketSize, bucketSize,
        ...pathSpecs.map(ps => ps.path),
        context,
        fromMs, toMs,
        ...uniquePaths
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
 * Apply SMA and EMA post-processing to result data
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
    .filter(({ spec }) => spec.aggregateMethod === "sma");
  const emaIndices = pathSpecs
    .map((spec, idx) => ({ spec, idx }))
    .filter(({ spec }) => spec.aggregateMethod === "ema");

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

module.exports = { SQLiteHistoryProvider };
