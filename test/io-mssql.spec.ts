import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { adminCfg } from '../src/admin-cfg';
const { DbBasics } = await import('../src/db-basics.ts');
const { IoMssql } = await import('../src/io-mssql.ts');



// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

describe('IoMssql', () => {
  let ioSql: any;
  const testDbName = 'TestDb';
  const testSchemaName = 'PantrySchema';
  const dbBasics = new DbBasics();

  beforeAll(async () => {
    await dbBasics.dropDatabase(adminCfg, testDbName);
    await dbBasics.createDatabase(adminCfg, testDbName);
    await dbBasics.useDatabase(adminCfg, testDbName);
    await dbBasics.createSchema(adminCfg, testDbName, testSchemaName);
    await dbBasics.installProcedures(adminCfg, testDbName);
  });

  beforeEach(async () => {
    // Create general access to the server
    const masterMind = new IoMssql(adminCfg);

    // Create a new database for testing
    ioSql = await masterMind.example(testDbName);
    // Initialize connection
    await ioSql.init();
    await ioSql.isReady();
  });

  afterEach(async () => {
    await ioSql.close();
  });

  // Clean up after all tests have run
  afterAll(async () => {
    // await IoMssql.dropTestLogins(adminCfg);
    // await IoMssql.dropTestSchemas(adminCfg);
  });

  it('should connect to the database', async () => {
    expect(ioSql.isOpen).toBe(true);
  });

  it('should return an error when the _conn cannot be established', async () => {
    const badConfig = {
      ...adminCfg,
      server: 'invalid_server_name',
    };
    const ioMssql = new IoMssql(badConfig, 'badSchema');
    let error: unknown = null;
    try {
      await ioMssql.init();
    } catch (err) {
      error = err;
    }
    expect((error as { name: string }).name).toBe('ConnectionError');
    await ioMssql.close();
  });

  it('should return an error when the login is invalid', async () => {
    const badConfig = {
      ...adminCfg,
      password: 'invalid',
    };
    const ioMssql = new IoMssql(badConfig, 'badSchema');
    let error: unknown = null;
    try {
      await ioMssql.init();
    } catch (err) {
      error = err;
    }
    expect((error as { name: string; message: string }).message).toBe(
      `Login failed for user '${adminCfg.user}'.`,
    );
    await ioMssql.close();
  });

  it('should return an error when the connection is closed', async () => {
    await ioSql.close();
    let error: unknown = null;
    try {
      await ioSql.isReady();
    } catch (err) {
      error = err;
    }
    expect((error as { name: string; message: string }).message).toBe(
      'MSSQL connection is not open.',
    );
  });

  it('should execute installScripts without throwing', async () => {
    await expect(
      dbBasics.installProcedures(adminCfg, testDbName),
    ).resolves.not.toThrow();
  });
});
