import sql from 'mssql';

export async function runScript(
  config: sql.config,
  script: string,
  dbName: string,
): Promise<string[]> {
  let pool: sql.ConnectionPool | undefined;
  // Connect to SQL Server
  try {
    pool = await sql.connect(config);
  } catch (error) {
    console.error('Error connecting to SQL Server:', error);
    if (error instanceof Error) {
      throw error.message;
    } else {
      throw new Error(String(error));
    }
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
      } else {
        throw new Error(String(error));
      }
    }
  }

  await pool.close();
  return result.map((item) =>
    typeof item === 'string' ? item : JSON.stringify(item),
  );
}
