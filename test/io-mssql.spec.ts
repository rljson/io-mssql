import sql from 'mssql';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';

import { DbInit } from '../src/db-init';
import { IoMssql } from '../src/io-mssql'; // Adjust the path as needed

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
  let ioSql: IoMssql;

  const adminCfg: sql.config = {
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

  const testDbName = 'TestDb';
  const testSchemaName = 'PantrySchema';

  beforeAll(async () => {
    await DbInit.dropDatabase(adminCfg, testDbName);
    await DbInit.createDatabase(adminCfg, testDbName);
    await DbInit.useDatabase(adminCfg, testDbName);
    await DbInit.createSchema(adminCfg, testDbName, testSchemaName);
    console.log('Installation scripts executed successfully.');
  });

  beforeEach(async () => {
    // Create general access to the server
    const masterMind = new IoMssql(adminCfg);

    // Create a new database for testing
    ioSql = await masterMind.example();
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
    const ioMssql = new IoMssql(adminCfg);
    await ioMssql.init();
    await ioMssql.close();
    let error: unknown = null;
    try {
      await ioMssql.isReady();
    } catch (err) {
      error = err;
    }
    expect((error as { name: string; message: string }).message).toBe(
      'MSSQL connection is not open.',
    );
  });

  it('should execute installScripts without throwing', async () => {
    await expect(IoMssql.installScripts(adminCfg)).resolves.not.toThrow();
  });
});
