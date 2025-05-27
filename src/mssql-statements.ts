// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoTools } from '@rljson/io';
import { JsonValue, JsonValueType } from '@rljson/json';
import { ColumnCfg, TableCfg, TableKey } from '@rljson/rljson';

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

  get tableKeys() {
    return `SELECT TABLE_NAME AS tableKey FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`;
  }

  get tableExists() {
    return `SELECT CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS tableExists`;
  }

  createTable(tableCfg: TableCfg): string {
    const sqltableKey = this.addTableSuffix(tableCfg.key);
    const columnsCfg = tableCfg.columns;

    const sqlCreateColumns = columnsCfg
      .map((col) => {
        const sqlType = this.jsonToSqlType(col.type);
        return `${this.addColumnSuffix(col.key)} ${sqlType}`;
      })
      .join(', ');

    // standard primary key - do not remove ;-)

    const connectingCol = columnsCfg.find(
      (col) => col.key === this.connectingColumn,
    );
    if (!connectingCol) {
      throw new Error(
        `Connecting column "${this.connectingColumn}" not found in table configuration.`,
      );
    }
    // Return the column's type if the column exists
    // connectingCol.type;
    const actualType = this.jsonToSqlType(connectingCol.type);
    // If the type is NVARCHAR(MAX), replace MAX with 256 for the connecting column
    const actualTypeLimited =
      actualType === 'NVARCHAR(MAX)' ? 'NVARCHAR(256)' : actualType;
    const originalColumnTerm = `${this.connectingColumn}${this.suffix.col} ${actualType}`;
    const columnWithPrimaryKey = `${this.connectingColumn}${this.suffix.col} ${actualTypeLimited} PRIMARY KEY`;

    const colsWithPrimaryKey = sqlCreateColumns.replace(
      originalColumnTerm,
      columnWithPrimaryKey,
    );

    // *******************************************************************
    // ******************foreign keys are not yet implemented*************
    // *******************************************************************
    // const foreignKeys = this.tableReferences(
    //   columnsCfg
    //     .map((col) => col.key)
    //     .filter((col) => col.endsWith(this.suffix.ref)),
    // );
    // const sqlForeignKeys = foreignKeys ? `, ${foreignKeys}` : '';
    // return `CREATE TABLE ${sqltableKey} (${colsWithPrimaryKey}${sqlForeignKeys})`;
    return `CREATE TABLE ${sqltableKey} (${colsWithPrimaryKey})`;
  }

  alterTable(tableKey: TableKey, addedColumns: ColumnCfg[]): string[] {
    const tableKeyWithSuffix = this.addTableSuffix(tableKey);
    const statements: string[] = [];
    for (const col of addedColumns) {
      const columnKey = this.addColumnSuffix(col.key);
      const columnType = this.jsonToSqlType(col.type);
      statements.push(
        `ALTER TABLE ${tableKeyWithSuffix} ADD ${columnKey} ${columnType};`,
      );
    }
    return statements;
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

  public whereString(whereClause: [string, JsonValue][]): string {
    let constraint: string = ' ';
    for (const [column, value] of whereClause) {
      const columnWithFix = this.addColumnSuffix(column);

      if (typeof value === 'string') {
        constraint += `${columnWithFix} = '${value}' AND `;
      } else if (typeof value === 'number') {
        constraint += `${columnWithFix} = ${value} AND `;
      } else if (typeof value === 'boolean') {
        constraint += `${columnWithFix} = ${value ? 1 : 0} AND `;
      } else if (value === null) {
        constraint += `${columnWithFix} IS NULL AND `;
      } else if (typeof value === 'object') {
        constraint += `${columnWithFix} = '${JSON.stringify(value)}' AND `;
      } else {
        throw new Error(`Unsupported value type for column ${column}`);
      }
    }

    constraint = constraint.endsWith('AND ')
      ? constraint.slice(0, -5)
      : constraint; // remove last ' AND '

    return constraint;
  }
}
