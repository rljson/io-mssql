// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoTools } from '@rljson/io';
import { JsonValueType } from '@rljson/json';

import { SqlStatements } from './sql-statements.ts';

export class MsSqlStatements extends SqlStatements {
  constructor() {
    super();
  }

  // Override jsonToSqlType from SqlStatements
  jsonToSqlType(dataType: JsonValueType): string {
    // Custom implementation or call super if needed
    // Example: return this.stat.jsonToSqlType(dataType);
    switch (dataType) {
      case 'string':
        return 'NVARCHAR(MAX)';
      case 'jsonArray':
        return 'NVARCHAR(MAX)';
      case 'json':
        return 'NVARCHAR(MAX)';
      case 'number':
        return 'FLOAT';
      case 'boolean':
        return 'BIT';
      case 'jsonValue':
        return 'NVARCHAR(MAX)';
      default:
        throw new Error(`Unknown JsonValueType: ${dataType}`);
    }
  }

  get tableExists() {
    return `SELECT CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS tableExists`;
  }

  // DDL stuff********************************************

  public useDatabase = (dbName: string) => `USE [${dbName}]`;
  public createSchema = (schemaName: string) => `CREATE SCHEMA [${schemaName}]`;
  public createLogin = (
    loginName: string,
    dbName: string,
    loginPassword: string,
  ) =>
    `CREATE LOGIN [${loginName}] WITH PASSWORD='${loginPassword}', DEFAULT_DATABASE=[${dbName}], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;`;
  public createUser = (
    userName: string,
    loginName: string,
    schemaName: string,
  ) =>
    `CREATE USER [${userName}] FOR LOGIN [${loginName}] WITH DEFAULT_SCHEMA=[${schemaName}]`;
  public addUserToRole = (roleName: string, userName: string) =>
    `ALTER ROLE [${roleName}] ADD MEMBER [${userName}]`;

  public grantSchemaPermission = (schemaName: string, userName: string) =>
    `GRANT ALTER ON SCHEMA:: [${schemaName}] TO [${userName}]`;
  public dropLogin = (loginName: string) => `DROP LOGIN [${loginName}]`;
  public dropUser = (userName: string) => `DROP USER [${userName}]`;
  public dropSchema = (schemaName: string) => `DROP SCHEMA [${schemaName}]`;
  public dropDatabase = (dbName: string) => `DROP DATABASE [${dbName}]`;
  public insertTableCfg() {
    const columnKeys = IoTools.tableCfgsTableCfg.columns.map((col) => col.key);
    const columnKeysWithPostfix = columnKeys.map((col) =>
      this.addColumnSuffix(col),
    );
    const columnsSql = columnKeysWithPostfix.join(', ');
    const valuesSql = columnKeys.map((_, i) => `@p${i}`).join(', ');

    return `INSERT INTO ${this.tbl.main}${this.suffix.tbl} ( ${columnsSql} ) VALUES (${valuesSql})`;
  }
}
