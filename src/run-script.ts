import sql from 'mssql';

export async function runScript(
  config: sql.config,
  script: string,
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

  // Run each batch sequentially
  const result: string[] = [];
  for (const batch of batches) {
    try {
      const batchResult = await pool.request().batch(batch);
      if (
        batchResult &&
        batchResult.recordset &&
        batchResult.recordset[0] &&
        batchResult.recordset[0].Status !== undefined
      ) {
        result.push(batchResult.recordset[0].Status);
      }
    } catch (error) {
      console.error('Error executing SQL batch:', error);
      if (error instanceof Error) {
        let errorMsg = error.message;
        // Check for precedingErrors property
        if (
          (error as any).precedingErrors &&
          Array.isArray((error as any).precedingErrors)
        ) {
          const preceding = (error as any).precedingErrors
            .map((e: Error) => e.message)
            .join('\n');
          errorMsg += '\nPreceding Errors:\n' + preceding;
        }
        throw new Error(errorMsg);
      } else {
        throw new Error(String(error));
      }
    }
  }

  await pool.close();
  return result;
}
