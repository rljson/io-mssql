import sql from 'mssql';
import { beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { DbInit } from '../src/db-init';
import { runScript } from '../src/run-script';

// @license
// Copyright (c) 2025 Rljson
let adminCfg: sql.config;

const testDbName = 'TestDb';
const testSchemaName = 'PantrySchema';
const testLogin = 'test_login';
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
  await DbInit.initDb(
    adminCfg,
    testDbName,
    testSchemaName,
    testLogin,
    testPassword,
  );
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

    it('should drop a non-existing database gracefully', async () => {
      const dbName = 'NonExistentDb';
      const result = await DbInit.dropDatabase(adminCfg, dbName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${dbName} does not exist` }),
      );
    });
  });
  describe('Schema', () => {
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

    it('should drop a non-existing schema gracefully', async () => {
      const schemaName = 'NonExistentSchema';
      const result = await DbInit.dropSchema(adminCfg, testDbName, schemaName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${schemaName} does not exist` }),
      );
    });
  });
  describe('Procedures and Users', () => {
    it('should create procedure dropLogins without error', async () => {
      const x = await DbInit.createProcDropLogins(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Procedure to drop logins created` }),
      );
    });

    it('should create and drop the dropObjects procedure', async () => {
      const x = await DbInit.createProcDropObjects(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropObjects for main created`,
        }),
      );
    });
    it('should create dropSchema procedure', async () => {
      const x = await DbInit.createProcDropSchema(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropCurrentSchema for main created`,
        }),
      );
    });

    it('should create dropConstraints procedure', async () => {
      const x = await DbInit.createProcDropConstraints(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropCurrentConstraints for main created`,
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
      // expect(userList.length).toEqual(5);
      userList.forEach((user, idx) => {
        console.log(`User ${idx}:`, JSON.parse(user).name);
      });

      // Check if the new user can login
      const newUserCfg: sql.config = {
        user: testUser,
        password: testPassword,
        server: 'localhost',
        port: 1431,
        database: testDbName,
        options: {
          encrypt: false, // SSL deaktivieren
          trustServerCertificate: true, // selbstsigniertes Zertifikat akzeptieren
        },
      };

      console.log(newUserCfg);
      console.log(adminCfg);
      const ok = await runScript(newUserCfg, `SELECT 1 AS RESULT`, testDbName);
      expect(ok).toEqual(['{"RESULT":1}']);
    });
    it('should add user to role', async () => {
      const testUser = 'test_user_role';
      await DbInit.createLogin(adminCfg, testDbName, testUser, testPassword);
      await DbInit.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const roleName = 'db_datareader';
      const result = await DbInit.addUserToRole(
        adminCfg,
        testDbName,
        roleName,
        testUser,
      );
      expect(Array.isArray(result)).toBe(true);
    });

    it('should grant schema permission to user', async () => {
      const testUser = 'test_user_perm';
      await DbInit.createLogin(adminCfg, testDbName, testUser, testPassword);
      await DbInit.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const result = await DbInit.grantSchemaPermission(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
      );
      expect(Array.isArray(result)).toBe(true);
    });
    it('should drop a non-existing login gracefully', async () => {
      const loginName = 'NonExistentLogin';
      const result = await DbInit.dropLogin(adminCfg, loginName, testDbName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `LOGIN [${loginName}] DOES NOT EXIST` }),
      );
    });

    it('should drop a non-existing user gracefully', async () => {
      const userName = 'NonExistentUser';
      const result = await DbInit.dropUser(adminCfg, userName, testDbName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `USER [${userName}] DOES NOT EXIST` }),
      );
    });

    it('should not create an existing login', async () => {
      const testUser = 'test_user_exists';
      await DbInit.createLogin(adminCfg, testDbName, testUser, testPassword);
      const result = await DbInit.createLogin(
        adminCfg,
        testDbName,
        testUser,
        testPassword,
      );
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `LOGIN [${testUser}] ALREADY EXISTS` }),
      );
    });

    it('should not create an existing user', async () => {
      const testUser = 'test_user_exists2';
      await DbInit.createLogin(adminCfg, testDbName, testUser, testPassword);
      await DbInit.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const result = await DbInit.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `USER [${testUser}] ALREADY EXISTS` }),
      );
    });
  });

  describe('Installation', () => {
    it('should run initDb without error', async () => {
      const dbName = 'InitDbTest';
      const schemaName = 'InitSchema';
      const loginName = 'init_user';
      await DbInit.dropDatabase(adminCfg, dbName);
      await DbInit.initDb(
        adminCfg,
        dbName,
        schemaName,
        loginName,
        testPassword,
      );
      const users = await DbInit.getUsers(adminCfg, dbName);
      expect(Array.isArray(users)).toBe(true);
    });

    it('should install procedures without error', async () => {
      const result = await DbInit.installProcedures(adminCfg, testDbName);
      // No error expected, just check that it returns
      expect(result).toBeUndefined();
    });
    it('should drop procedures without error', async () => {
      await DbInit.installProcedures(adminCfg, testDbName);
      const result = await DbInit.dropProcedures(adminCfg, testDbName);
      expect(result).toBeUndefined();
    });
  });
  describe('Utility Functions', () => {
    it('should get table names (empty)', async () => {
      const tables = await DbInit.getTableNames(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(Array.isArray(tables)).toBe(true);
      expect(tables.length).toBe(0);
    });
  });

  describe('Running Scripts', () => {
    it('should drop all tables` constraints', async () => {
      const createTable = `CREATE TABLE ${testSchemaName}.TestTable (ID INT PRIMARY KEY)`;
      await runScript(adminCfg, createTable, testDbName);
      const result = await DbInit.dropConstraints(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(result).toBeUndefined();

      const leftConstraints = await runScript(
        adminCfg,
        `SELECT COUNT(*) AS LEFT_CONSTRAINTS FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA='${testSchemaName}' AND TABLE_NAME='TestTable'`,
        testDbName,
      );
      expect(leftConstraints).toEqual(['{"LEFT_CONSTRAINTS":0}']);
    });
  });
});
