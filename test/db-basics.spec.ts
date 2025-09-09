import 'dotenv/config';
import sql from 'mssql';
import { beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { DbBasics } from '../src/db-basics';
import { runScript } from '../src/run-script';

// @license
// Copyright (c) 2025 Rljson
let adminCfg: sql.config;
let testDbName: string;
let testSchemaName: string;
let testLogin: string;
const testPassword = 'Password123!';

beforeAll(() => {
  adminCfg = {
    user: 'sa',
    password: process.env.SA_PASSWORD,
    server: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 1431,
    database: process.env.DB_NAME,
    options: {
      encrypt: false, // set to true if using SSL
      trustServerCertificate: true, // needed for local dev
    },
  };
});

beforeEach(async () => {
  testDbName = 'TestDb_' + Math.random().toString(36).substring(2, 10);
  testSchemaName = 'PantrySchema';
  testLogin = 'test_login';

  await DbBasics.initDb(
    adminCfg,
    testDbName,
    testSchemaName,
    testLogin,
    testPassword,
  );
});
describe('DbBasics', () => {
  describe('Database', () => {
    it('should not be created again', async () => {
      const dropped = await DbBasics.dropDatabase(adminCfg, testDbName);
      console.log(dropped);
      const created = await DbBasics.createDatabase(adminCfg, testDbName);
      console.log(created);
      const notCreated = await DbBasics.createDatabase(adminCfg, testDbName);
      expect(notCreated[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} already exists` }),
      );
      const users = await DbBasics.getUsers(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(users.length).toBe(0);
    });

    it('should be dropped', async () => {
      const x = await DbBasics.dropDatabase(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} dropped` }),
      );
    });

    it('should drop a non-existing database gracefully', async () => {
      const dbName = 'NonExistentDb';
      const result = await DbBasics.dropDatabase(adminCfg, dbName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${dbName} does not exist` }),
      );
    });

    // This is currently only for manual testing,
    // as it would drop all DBs on the server***/
    //***********************************************/
    // it('should drop multiple databases', async () => {
    //   const result = await DbBasics.dropDatabases(adminCfg);
    //   console.log(result);
    //   expect(Array.isArray(result)).toBe(true);
    // });
    //***********************************************/
  });
  describe('Schema', () => {
    it('should not add an existing schema', async () => {
      const x = await DbBasics.createSchema(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${testSchemaName} already exists` }),
      );

      const y = await DbBasics.dropSchema(adminCfg, testDbName, testSchemaName);
      expect(y[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${testSchemaName} dropped` }),
      );
    });

    it('should drop a non-existing schema gracefully', async () => {
      const schemaName = 'NonExistentSchema';
      const result = await DbBasics.dropSchema(
        adminCfg,
        testDbName,
        schemaName,
      );
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${schemaName} does not exist` }),
      );
    });
  });
  describe('Procedures and Users', () => {
    it('should create procedure dropLogins without error', async () => {
      const x = await DbBasics.createProcDropLogins(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Procedure to drop logins created` }),
      );
    });

    it('should create and drop the dropObjects procedure', async () => {
      const x = await DbBasics.createProcDropObjects(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropObjects for main created`,
        }),
      );
    });
    it('should create dropSchema procedure', async () => {
      const x = await DbBasics.createProcDropSchema(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropSchema for main created`,
        }),
      );
    });

    it('should create dropConstraints procedure', async () => {
      const x = await DbBasics.createProcDropConstraints(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropConstraints for main created`,
        }),
      );
    });
    it('should create login & user', async () => {
      const testUser = 'test_user';

      //  Login (drop first, then create)
      await DbBasics.dropLogin(adminCfg, testDbName, testUser);
      const createLogin = await DbBasics.createLogin(
        adminCfg,
        testDbName,
        testUser,
        testPassword,
      );
      expect(createLogin[0].toString()).toBe(
        JSON.stringify({ Status: `LOGIN [${testUser}] CREATED` }),
      );

      // User (drop first, then create)
      await DbBasics.dropUser(adminCfg, testDbName, testUser);

      const createUser = await DbBasics.createUser(
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
      const getUsers = await DbBasics.getUsers(
        adminCfg,
        testDbName,
        testSchemaName,
      );
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
      await DbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      await DbBasics.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const roleName = 'db_datareader';
      const result = await DbBasics.addUserToRole(
        adminCfg,
        testDbName,
        roleName,
        testUser,
      );
      expect(Array.isArray(result)).toBe(true);
    });

    it('should grant schema permission to user', async () => {
      const testUser = 'test_user_perm';
      await DbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      await DbBasics.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const result = await DbBasics.grantSchemaPermission(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
      );
      expect(Array.isArray(result)).toBe(true);
    });
    it('should drop a non-existing login gracefully', async () => {
      const loginName = 'NonExistentLogin';
      const result = await DbBasics.dropLogin(adminCfg, testDbName, loginName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `LOGIN [${loginName}] DOES NOT EXIST` }),
      );
    });

    it('should drop a non-existing user gracefully', async () => {
      const userName = 'NonExistentUser';
      const result = await DbBasics.dropUser(adminCfg, testDbName, userName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `USER [${userName}] DOES NOT EXIST` }),
      );
    });

    it('should not create an existing login', async () => {
      const testUser = 'test_user_exists';
      await DbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      const result = await DbBasics.createLogin(
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
      await DbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      await DbBasics.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const result = await DbBasics.createUser(
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
      await DbBasics.dropDatabase(adminCfg, dbName);
      await DbBasics.initDb(
        adminCfg,
        dbName,
        schemaName,
        loginName,
        testPassword,
      );
      const users = await DbBasics.getUsers(adminCfg, dbName, testSchemaName);
      expect(Array.isArray(users)).toBe(true);
    });

    it('should install procedures without error', async () => {
      const result = await DbBasics.installProcedures(adminCfg, testDbName);
      // No error expected, just check that it returns
      expect(result).toBeUndefined();
    });
    it('should drop procedures without error', async () => {
      await DbBasics.installProcedures(adminCfg, testDbName);
      const result = await DbBasics.dropProcedures(adminCfg, testDbName);
      expect(result).toBeUndefined();
    });
  });
  describe('Utility Functions', () => {
    it('should get table names (empty)', async () => {
      const tables = await DbBasics.getTableNames(
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
      const tableName = 'TestTable';
      const createTable = `CREATE TABLE ${testSchemaName}.${tableName} (ID INT PRIMARY KEY)`;
      await runScript(adminCfg, createTable, testDbName);
      const result = await DbBasics.dropConstraints(
        adminCfg,
        testDbName,
        testSchemaName,
        tableName,
      );
      expect(result).toBeUndefined();

      const leftConstraints = await runScript(
        adminCfg,
        `SELECT COUNT(*) AS LEFT_CONSTRAINTS FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA='${testSchemaName}' AND TABLE_NAME='TestTable'`,
        testDbName,
      );
      expect(leftConstraints).toEqual(['{"LEFT_CONSTRAINTS":0}']);
    });

    it('should drop all tables', async () => {
      const tableName = 'TestTable';
      const createTable = `CREATE TABLE ${testSchemaName}.${tableName} (ID INT PRIMARY KEY)`;
      await runScript(adminCfg, createTable, testDbName);
      const result = await DbBasics.dropTables(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(result).toBeUndefined();
      const leftTables = await DbBasics.getTableNames(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(leftTables.length).toBe(0);
    });
    it('should drop all users', async () => {
      const result = await DbBasics.dropUsers(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(result).toBeUndefined();
      const leftUsers = await DbBasics.getUsers(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(leftUsers.length).toBe(0);
    });
  });
});
