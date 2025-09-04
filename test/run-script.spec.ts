import sql from 'mssql';
import { afterAll, beforeAll, describe, it } from 'vitest';

import { DbBasics } from '../src/db-basics';
import { runScript } from '../src/run-script';

let adminCfg: sql.config;

const testDb = 'TestDb';
// const testSchema = 'TestSchema';

beforeAll(() => {
  DbBasics.createDatabase(adminCfg, testDb);
  adminCfg = {
    user: 'sa',
    password: 'Password123!',
    server: 'localhost', // or the IP of your container host
    port: 1431,
    database: 'master', // or your specific DB name
    options: {
      encrypt: false, // set to true if using SSL
      trustServerCertificate: true, // needed for local dev
    },
  };
});

afterAll(async () => {
  await DbBasics.dropDatabase(adminCfg, testDb);
});

describe('runScript', () => {
  it('should connect to SQL Server and execute a single batch', async () => {
    const script = 'SELECT 1';
    await runScript(adminCfg, script, testDb);
  });

  it('should split script by GO and execute multiple batches', async () => {
    const script = `
      SELECT 1
      GO
      SELECT 2
      GO -- comment
      SELECT 3
    `;
    await runScript(adminCfg, script, testDb);
  });
});
