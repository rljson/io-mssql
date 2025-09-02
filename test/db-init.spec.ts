import { beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { DbInit } from '../src/db-init';

import type { config as SqlConfig } from 'mssql';
// @license
// Copyright (c) 2025 Rljson
let adminCfg: SqlConfig;

const testDbName = 'TestDb';
const testSchemaName = 'PantrySchema';
const testPassword = 'Password123!';

beforeAll(() => {
  adminCfg = {
    user: 'sa',
    password: testPassword,
    server: 'localhost', // or the IP of your container host
    port: 1431, // default port for SQL Server
    database: 'master', // or your specific DB name
    options: {
      encrypt: false, // set to true if using SSL
      trustServerCertificate: true, // needed for local dev
    },
  };
});

beforeEach(async () => {
  // await DbInit.dropDatabase(adminCfg, testDbName);
  await DbInit.createDatabase(adminCfg, testDbName);
  await DbInit.useDatabase(adminCfg, testDbName);
  await DbInit.createSchema(adminCfg, testDbName, testSchemaName);
});
describe('DbInit', () => {
  describe('Database', () => {
    it('should not be created again', async () => {
      const dbResult = await DbInit.createDatabase(adminCfg, testDbName);
      expect(dbResult[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} already exists` }),
      );
    });

    it('should be dropped', async () => {
      const x = await DbInit.dropDatabase(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} dropped` }),
      );
    });

    it('should not add an existing schema', async () => {
      const x = await DbInit.createSchema(adminCfg, testDbName, testSchemaName);
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${testSchemaName} already exists` }),
      );

      const y = await DbInit.dropSchema(adminCfg, testDbName, testSchemaName);
      expect(y[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${testSchemaName} dropped` }),
      );
    });
  });
  it('should create procedure dropLogins without error', async () => {
    const x = await DbInit.createProcDropLogins(
      adminCfg,
      testDbName,
      testSchemaName,
    );
    expect(x[0].toString()).toBe(
      JSON.stringify({ Status: `Procedure to drop logins created` }),
    );
    const y = await DbInit.dropProcDropLogins(
      adminCfg,
      testDbName,
      testSchemaName,
    );
    expect(y[0].toString()).toBe(
      JSON.stringify({ Status: `Procedure to drop logins dropped` }),
    );
  });

  it('should create and drop the dropObjects procedure', async () => {
    const x = await DbInit.createProcDropObjects(
      adminCfg,
      testDbName,
      testSchemaName,
    );
    expect(x[0].toString()).toBe(
      JSON.stringify({
        Status: `Procedure DropObjects for ${testSchemaName} created`,
      }),
    );
  });
  it('should create dropSchema procedure', async () => {
    const x = await DbInit.createProcDropSchema(
      adminCfg,
      testDbName,
      testSchemaName,
    );
    expect(x[0].toString()).toBe(
      JSON.stringify({
        Status: `Procedure DropSchema for ${testSchemaName} created`,
      }),
    );
  });

  it('should create dropConstraints procedure', async () => {
    const x = await DbInit.createProcDropConstraints(
      adminCfg,
      testDbName,
      testSchemaName,
    );
    expect(x[0].toString()).toBe(
      JSON.stringify({
        Status: `Procedure DropConstraints for ${testSchemaName} created`,
      }),
    );
  });
  it('should create login & user', async () => {
    const testUser = 'test_user';

    //  Login (drop first, then create)
    await DbInit.dropLogin(adminCfg, testUser, testDbName);
    const createLogin = await DbInit.createLogin(
      adminCfg,
      testDbName,
      testUser,
      testPassword,
    );
    expect(createLogin[0].toString()).toBe(
      JSON.stringify({ Status: `LOGIN [${testUser}] CREATED` }),
    );

    // User (drop first, then create)
    await DbInit.dropUser(adminCfg, testUser, testDbName);

    const createUser = await DbInit.createUser(
      adminCfg,
      testDbName,
      testSchemaName,
      testUser,
      testUser,
    );
    expect(createUser[0].toString()).toBe(
      JSON.stringify({ Status: `USER [${testUser}] CREATED` }),
    );

    // Get Users - check if the user is in the list
    const getUsers = await DbInit.getUsers(adminCfg, testDbName);
    const userList = Array.isArray(getUsers) ? getUsers : [getUsers];
    expect(userList.length).toEqual(5);
    userList.forEach((user, idx) => {
      console.log(`User ${idx}:`, JSON.parse(user).name);
    });
  });
});
