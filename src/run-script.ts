import sql from 'mssql';

export async function runScript(
  config: sql.config,
  script: string,
  dbName: string,
): Promise<string[]> {
  // A dedicated pool, not the package's `sql.connect(config)` global
  // helper: that helper lazily creates (at most) ONE shared, module-level
  // pool and reuses it across every caller regardless of config, and
  // every caller's own `pool.close()` call closes THAT SAME shared pool.
  // Two overlapping runScript() calls sharing it means whichever finishes
  // first closes the pool out from under the other one, which is still
  // mid-batch on it -- observed as queries against an already-closing
  // connection, timeouts, and error-listener accumulation on the reused
  // pool object under real concurrent load. A dedicated pool per call
  // sidesteps all of that: each call only ever closes the one connection
  // it opened itself.
  const pool: sql.ConnectionPool = await new sql.ConnectionPool(
    config,
  ).connect();

  // Handle empty script
  if (script.trim().length === 0) {
    await pool.close();
    return [];
  }

  // Split script by "GO" batch separator
  const batches = script
    .split(/^\s*GO\s*(?:--.*)?$/gim)
    .map((batch) => batch.trim())
    .filter((batch) => batch.length > 0);

  // Set the database context
  if (!batches[0].startsWith('USE master')) {
    await pool.request().query(`USE [${dbName}];`);
  }

  // Run each batch sequentially
  const result: (sql.IRecordSet<any> | string)[] = [];
  for (const batch of batches) {
    try {
      const batchResult = await pool.request().batch(batch);
      if (batchResult && batchResult.recordset && batchResult.recordset[0]) {
        for (const row of batchResult.recordset) {
          result.push(row);
        }
      }
    } catch (error) {
      /* v8 ignore next -- @preserve */
      if (error instanceof Error) {
        result.push(error.message);
        // Check for precedingErrors property
        if (
          (error as any).precedingErrors &&
          Array.isArray((error as any).precedingErrors)
        ) {
          /* v8 ignore next -- @preserve */
          const preceding = (error as any).precedingErrors
            .map((e: Error) => e.message)
            .join('\n');
          result.push(preceding);
        }
      }
    }
  }

  await pool.close();
  return result.map((item) =>
    typeof item === 'string' ? item : JSON.stringify(item),
  );
}
