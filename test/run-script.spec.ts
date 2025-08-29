import sql from 'mssql';
import { beforeAll, describe, it } from 'vitest';

import { runScript } from '../src/run-script';

let adminCfg: sql.config;

beforeAll(() => {
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

describe('runScript', () => {
  it('should connect to SQL Server and execute a single batch', async () => {
    const script = 'SELECT 1';
    await runScript(adminCfg, script);
  });

  it('should split script by GO and execute multiple batches', async () => {
    const script = `
      SELECT 1
      GO
      SELECT 2
      GO -- comment
      SELECT 3
    `;
    await runScript(adminCfg, script);
  });
});
