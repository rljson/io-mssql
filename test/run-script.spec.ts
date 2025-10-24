import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { adminCfg } from '../src/admin-cfg';
import { DbBasics } from '../src/db-basics';
import { runScript } from '../src/run-script';


describe('runScript', () => {
const testDb = 'TestDbForRunScript';

beforeAll(async () => {
  await DbBasics.createDatabase(adminCfg, testDb);
});

afterAll(async () => {
  await DbBasics.dropDatabase(adminCfg, testDb);
});

  it('should connect to SQL Server and execute a single batch', async () => {
    const script = 'SELECT 1 AS RESULT';
    const result = await runScript(adminCfg, script, testDb);
    const json = JSON.parse(result[0] as string);
    //const value = json[0][0].RESULT;
    expect(json.RESULT).toBe(1);
  });

  it('should throw an error if connection cannot be established (invalid server)', async () => {
    const badConfig = {
      ...adminCfg,
      server: 'invalid_server_name',
      port: 9999,
    };
    const script = 'SELECT 1';
    let error: any = null;
    try {
      await runScript(badConfig, script, testDb);
    } catch (err) {
      error = err;
    }
    expect(error.name).toBe('ConnectionError');
  });

  it('should handle SQL syntax errors and return error messages', async () => {
    const script = 'SELECT * FROM NonExistentTable';
    const result = await runScript(adminCfg, script, testDb);   
    expect(result.length).toBeGreaterThan(0);
    expect(result[0]).toMatch(/invalid|could not find|does not exist|error/i);
  });

  it('should handle empty script gracefully', async () => {
    const script = '';
    const result = await runScript(adminCfg, script, testDb);
    expect(result).toEqual([]);
  });

  it('should handle script with only comments and whitespace', async () => {
    const script = `
      -- This is a comment
      /* Another comment */

      GO
    `;
    const result = await runScript(adminCfg, script, testDb);
    expect(result).toEqual([]);
  });

  it('should execute script that changes database context', async () => {
    const script = `
      USE [${testDb}]
      GO
      SELECT DB_NAME() AS DbName
    `;
    const result = await runScript(adminCfg, script, testDb);
    const dbName = JSON.parse(result[result.length - 1] as string).DbName;
    expect(dbName).toBe(testDb);
  });

  it('should return an error when given wrong adminCfg', async () => {
    const wrongCfg = {
      ...adminCfg,
      password: 'WrongPassword!',
    };
    const script = 'SELECT 1';
    let error: any = null;
    try {
      await runScript(wrongCfg, script, testDb);
    } catch (err) {
      error = err;
    }
    expect(error.message).toContain('Login failed for user');
  });

  it('should split script by GO and execute multiple batches', async () => {
    const script = `
      SELECT 1 AS RESULT
      GO
      SELECT 2 AS RESULT
      GO
      SELECT 3 AS RESULT
    `;
    const result = await runScript(adminCfg, script, testDb);
    const values = result.map((r) => JSON.parse(r as string).RESULT);
    expect(values).toEqual([1, 2, 3]);
  });
});
