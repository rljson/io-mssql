import sql from 'mssql';

export async function runScript(
  config: sql.config,
  script: string,
  dbName: string,
): Promise<string[]> {
  // Connect to SQL Server
  const pool: sql.ConnectionPool = await sql.connect(config);
  // if (!pool.connected) {
  //   throw new Error('Failed to connect to SQL Server');
  // }

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
      console.error('Error executing SQL batch:', error);
      if (error instanceof Error) {
        result.push(error.message);
        // Check for precedingErrors property
        if (
          (error as any).precedingErrors &&
          Array.isArray((error as any).precedingErrors)
        ) {
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
