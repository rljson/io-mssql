import sql from 'mssql';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { IoTools } from '@rljson/io';
import { ContentType } from '@rljson/rljson';
import { adminCfg } from '../src/admin-cfg.ts';
 import { DbStatements } from '../src/db-statements.ts';
 import { runScript } from  '../src/run-script.ts';
import { DbBasics } from '../src/db-basics.ts';

async function createMiniTableCfgsTable(schemaName: string): Promise<void> {
  const dbStatements = new DbStatements(schemaName);
  const tableCfg = IoTools.tableCfgsTableCfg;
  const script =  dbStatements.createTable(tableCfg);
  await runScript(adminCfg, script, 'master');
  const values = dbStatements.serializeRow(tableCfg, tableCfg);
  const declaredValues: string[] = [];
  values.forEach((val, idx) => {
    declaredValues.push(`DECLARE @p${idx} NVARCHAR(MAX) = '${val}';`);
  });  
  return;
}

describe('dbBasics', async () => {  
let testDbName: string;
const testSchemaName: string = 'PantrySchema';
const testLogin: string = 'test_login';
const testPassword: string = 'Password123!';
const dbBasics = new DbBasics();

beforeEach(async () => {
  testDbName = 'TestDb_' + Math.random().toString(36).substring(2, 10);  

  await dbBasics.initDb(
    adminCfg,
    testDbName,
    testSchemaName,
    testLogin,
    testPassword,
  );
});

afterEach(async () => {
  await dbBasics.dropDatabase(adminCfg, testDbName);
});

  describe('Database', () => {
    it('should not be created again', async () => {
      const dropped = await dbBasics.dropDatabase(adminCfg, testDbName);
      expect(dropped[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} dropped` }),
      );
      const created = await dbBasics.createDatabase(adminCfg, testDbName);
      expect(created[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} created` }),
      );
      const notCreated = await dbBasics.createDatabase(adminCfg, testDbName);
      expect(notCreated[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} already exists` }),
      );
      const users = await dbBasics.getUsers(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(users.length).toBe(0);
    });

    it('should be dropped', async () => {
      const x = await dbBasics.dropDatabase(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${testDbName} dropped` }),
      );
    });

    it('should drop a non-existing database gracefully', async () => {
      const dbName = 'NonExistentDb';
      const result = await dbBasics.dropDatabase(adminCfg, dbName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `Database ${dbName} does not exist` }),
      );
    });
  });
  describe('Schema', () => {
    it('should not add an existing schema', async () => {
      const x = await dbBasics.createSchema(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${testSchemaName} already exists` }),
      );

      const y = await dbBasics.dropSchema(adminCfg, testDbName, testSchemaName);
      expect(y[0].toString()).toBe(
        JSON.stringify({ Status: `Schema ${testSchemaName} dropped` }),
      );
    });

    it('should drop a non-existing schema gracefully', async () => {
      const schemaName = 'NonExistentSchema';
      const result = await dbBasics.dropSchema(
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
      const x = await dbBasics.createProcDropLogins(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({ Status: `Procedure to drop logins created` }),
      );
    });

    it('should create and drop the dropObjects procedure', async () => {
      const x = await dbBasics.createProcDropObjects(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropObjects for main created`,
        }),
      );
    });
    it('should create dropSchema procedure', async () => {
      const x = await dbBasics.createProcDropSchema(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropSchema for main created`,
        }),
      );
    });

    it('should create dropConstraints procedure', async () => {
      const x = await dbBasics.createProcDropConstraints(adminCfg, testDbName);
      expect(x[0].toString()).toBe(
        JSON.stringify({
          Status: `Procedure DropConstraints for main created`,
        }),
      );
    });
    it('should create login & user', async () => {
      const testUser = 'test_user';

      //  Login (drop first, then create)
      await dbBasics.dropLogin(adminCfg, testDbName, testUser);
      const createLogin = await dbBasics.createLogin(
        adminCfg,
        testDbName,
        testUser,
        testPassword,
      );
      expect(createLogin[0].toString()).toBe(
        JSON.stringify({ Status: `LOGIN [${testUser}] CREATED` }),
      );

      // User (drop first, then create)
      await dbBasics.dropUser(adminCfg, testDbName, testUser);

      const createUser = await dbBasics.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      expect(createUser[0].toString()).toBe(
        JSON.stringify({ Status: `USER [${testUser}] CREATED` }),
      );

      // Check if the new user can login
      const newUserCfg: sql.config = {
        ...adminCfg,
        user: testUser,
        password: testPassword,
        database: testDbName,
      };

      // Try to connect with the new user
      const ok = await runScript(newUserCfg, `SELECT 1 AS RESULT`, testDbName);
      expect(ok).toEqual(['{"RESULT":1}']);
    });
    it('should add user to role', async () => {
      const testUser = 'test_user_role';
      await dbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      await dbBasics.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const roleName = 'db_datareader';
      const result = await dbBasics.addUserToRole(
        adminCfg,
        testDbName,
        roleName,
        testUser,
      );
      expect(Array.isArray(result)).toBe(true);
    });

    it('should grant schema permission to user', async () => {
      const testUser = 'test_user_perm';
      await dbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      await dbBasics.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const result = await dbBasics.grantSchemaPermission(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
      );
      expect(Array.isArray(result)).toBe(true);
    });
    it('should drop a non-existing login gracefully', async () => {
      const loginName = 'NonExistentLogin';
      const result = await dbBasics.dropLogin(adminCfg, testDbName, loginName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `LOGIN [${loginName}] DOES NOT EXIST` }),
      );
    });

    it('should drop a non-existing user gracefully', async () => {
      const userName = 'NonExistentUser';
      const result = await dbBasics.dropUser(adminCfg, testDbName, userName);
      expect(result[0].toString()).toBe(
        JSON.stringify({ Status: `USER [${userName}] DOES NOT EXIST` }),
      );
    });

    it('should not create an existing login', async () => {
      const testUser = 'test_user_exists';
      await dbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      const result = await dbBasics.createLogin(
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
      await dbBasics.createLogin(adminCfg, testDbName, testUser, testPassword);
      await dbBasics.createUser(
        adminCfg,
        testDbName,
        testSchemaName,
        testUser,
        testUser,
      );
      const result = await dbBasics.createUser(
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
      await dbBasics.dropDatabase(adminCfg, dbName);
      await dbBasics.initDb(
        adminCfg,
        dbName,
        schemaName,
        loginName,
        testPassword,
      );
      const users = await dbBasics.getUsers(adminCfg, dbName, testSchemaName);
      expect(Array.isArray(users)).toBe(true);
    });

    it('should install procedures without error', async () => {
      const result = await dbBasics.installProcedures(adminCfg, testDbName);
      // No error expected, just check that it returns
      expect(result.length).toBe(5);
    });
    it('should drop procedures without error', async () => {
      await dbBasics.installProcedures(adminCfg, testDbName);
      const result = await dbBasics.dropProcedures(adminCfg, testDbName);
      expect(result).toBeUndefined();
    });
  });
  describe('Utility Functions', () => {
    it('should get table names (empty)', async () => {
      const tables = await dbBasics.getTableNames(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(Array.isArray(tables)).toBe(true);
      expect(tables.length).toBe(0);
    });
  });

  describe('Running Scripts', () => {
    describe('Dropping Objects', () => {
    it('should drop all tables` constraints', async () => {
      const tableName = 'TestTable';
      const createTable = `CREATE TABLE ${testSchemaName}.${tableName} (ID INT PRIMARY KEY)`;
      await runScript(adminCfg, createTable, testDbName);
      const result = await dbBasics.dropConstraints(
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
      const result = await dbBasics.dropTables(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(result).toBeUndefined();
      const remainingTables = await dbBasics.getTableNames(
        adminCfg,
        testDbName,
        testSchemaName,
      );

      expect(remainingTables.length).toBe(0);
    });

    it('should drop all users', async () => {
      const result = await dbBasics.dropUsers(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(result).toBeUndefined();
      const remainingUsers = await dbBasics.getUsers(
        adminCfg,
        testDbName,
        testSchemaName,
      );
      expect(remainingUsers.length).toBe(0);
    });
  });


    describe('Content Type', () => {
    it('should return a table content type', async () => {
      // Prepare: create tableCfgs table and insert a row
      await createMiniTableCfgsTable(testSchemaName);
      const script = `CREATE TABLE ${testSchemaName}.tableCfgs_tbl (type_col NVARCHAR(255) PRIMARY KEY, key_col NVARCHAR(50))`;
      await runScript(adminCfg, script, testDbName);  
      const insertScript = `INSERT INTO ${testSchemaName}.tableCfgs_tbl (type_col, key_col) VALUES ('tableCfgs', 'tableCfgs')`;
      await runScript(adminCfg, insertScript, testDbName);

      const returnType = await dbBasics.contentType(
        adminCfg,
        testDbName,
        testSchemaName,
        'tableCfgs'
      );
      const expectedType: ContentType = 'tableCfgs';
      const returnedType: ContentType = returnType as ContentType;
            expect(returnedType).toEqual(expectedType);    
    });

    it('should throw error if content type table row does not exist', async () => {
      // Create the content type procedure
      await dbBasics.createContentTypeProc(adminCfg, testDbName);

      // Try to get content type for a non-existent table
      await expect(
      dbBasics.contentType(
        adminCfg,
        testDbName,
        testSchemaName,
        'NonExistentTable'
      )
      ).rejects.toThrow('Table "NonExistentTable" not found');
    });


  describe('Transaction Handling', () => {
    const transactionName = 'TestTransaction';

    it('should begin a transaction', async () => {
      const result = await dbBasics.transact(
        adminCfg,
        testDbName,
        'begin',
        transactionName
      );
      expect(Array.isArray(result)).toBe(true);
    });

    it('should commit a transaction', async () => {
      // Begin transaction first
      await dbBasics.transact(adminCfg, testDbName, 'begin', transactionName);
      const result = await dbBasics.transact(
        adminCfg,
        testDbName,
        'commit',
        transactionName
      );
      expect(Array.isArray(result)).toBe(true);
    });

    it('should rollback a transaction', async () => {
      // Begin transaction first
      await dbBasics.transact(adminCfg, testDbName, 'begin', transactionName);
      const result = await dbBasics.transact(
        adminCfg,
        testDbName,
        'rollback',
        transactionName
      );
      expect(Array.isArray(result)).toBe(true);
    });
  });

  // describe('CDC handling', () => {
  //   it('should enable CDC on the database', async () => {
  //     const result = await dbBasics.enableCdcDb(adminCfg, testDbName);
  //     expect(result[0].toString()).toBe(
  //       JSON.stringify({ Status: `CDC enabled for database ${testDbName}` }),
  //     );
  //     // Calling again should have no effect
  //     const result2 = await dbBasics.enableCdcDb(adminCfg, testDbName);
  //     expect(result2[0].toString()).toBe(
  //       JSON.stringify({ Status: `CDC enabled for database ${testDbName}` }),
  //     );
  //   });

  //   it('should disable CDC on the database', async () => {
  //     // First, ensure CDC is enabled
  //     await dbBasics.enableCdcDb(adminCfg, testDbName);
  //     const result = await dbBasics.disableCdcDb(adminCfg, testDbName);
  //     expect(result[0].toString()).toBe(
  //       JSON.stringify({ Status: `CDC disabled for database ${testDbName}` }),
  //     );
  //     // Calling again  should have no effect
  //     const result2 = await dbBasics.disableCdcDb(adminCfg, testDbName);
  //     expect(result2[0].toString()).toBe(
  //       JSON.stringify({ Status: `CDC disabled for database ${testDbName}` }),
  //     );
  //   });
  //   it('should enable and disable CDC on a table', async () => {
  //     await dbBasics.enableCdcDb(adminCfg, testDbName);
  //     const tableName = 'CdcTestTable';
  //     const createTable = `CREATE TABLE ${testSchemaName}.${tableName} (ID INT PRIMARY KEY, Name NVARCHAR(100))`;
  //     await runScript(adminCfg, createTable, testDbName);
  //     const result = await dbBasics.enableCDCTable(
  //       adminCfg,
  //       testDbName,
  //       testSchemaName,
  //       tableName,
  //     );
  //     expect(result[0].toString()).toBe(
  //       JSON.stringify({
  //         Status: `CDC enabled for table ${testSchemaName}.${tableName}`,
  //       }),
  //     );

  //     await expect(
  //       dbBasics.disableCDCTable(
  //         adminCfg,
  //         testDbName,
  //         testSchemaName,
  //         tableName,
  //       ),
  //     ).resolves.not.toThrow();
  //   });
  // });
});
  });




})