import { beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { DbInit } from '../src/db-initialize';

import type { config as SqlConfig } from 'mssql';
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// Dummy adminConfig for demonstration (replace with real config for integration tests)

let adminCfg: SqlConfig;

const testDbName = 'TestDbInithh';
const testSchemaName = 'TestSchemahh';

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

beforeEach(async () => {
  await DbInit.dropDatabase(adminCfg, testDbName);
  await DbInit.createDatabase(adminCfg, testDbName);
  await DbInit.useDatabase(adminCfg, testDbName);
  await DbInit.createSchema(adminCfg, testDbName, testSchemaName);
});
describe('DbInit', () => {
  it('should create a database if it does not exist', async () => {
    const x = await DbInit.createDatabase(adminCfg, testDbName);
    expect(x[0]).toBe(`Database ${testDbName} already exists`);
  });

  it('should create a schema in the database', async () => {
    const x = await DbInit.createSchema(adminCfg, testDbName, testSchemaName);
    expect(x[0]).toBe(`Schema ${testSchemaName} already exists`);
    // Optionally, verify the schema exists using a query here
  });

  it('should execute dropLogins without error', async () => {
    await expect(
      DbInit.dropLogins(adminCfg, testSchemaName),
    ).resolves.toBeUndefined();
    // Optionally, verify constraints are dropped using a query here
  });
});
