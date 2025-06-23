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
  constructor(public schemaName: string) {
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
    return `SELECT TABLE_NAME AS tableKey FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '${this.schemaName}'`;
  }

  get tableExists() {
    return `SELECT CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = '${this.schemaName}') THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS tableExists`;
  }

  createTable(tableCfg: TableCfg): string {
    const sqltableKey = this.addTableSuffix(tableCfg.key);
    const columnsCfg = tableCfg.columns;

    const sqlCreateColumns = columnsCfg
      .map((col) => {
        let sqlType = this.jsonToSqlType(col.type);
        if (
          (col.key.endsWith('Ref') || col.key === this.connectingColumn) &&
          sqlType.toLowerCase() === 'nvarchar(max)'
        ) {
          sqlType = 'NVARCHAR(256)';
        }
        return `${this.addColumnSuffix(col.key)} ${sqlType}`;
      })
      .join(', ');

    // standard primary key - do not remove ;-)
    const primaryKey = `CONSTRAINT PK_${
      tableCfg.key
    } PRIMARY KEY ([${this.addColumnSuffix(this.connectingColumn)}])`;

    const foreignKeysArr = this.foreignKeys(
      columnsCfg
        .map((col) => col.key)
        .filter((col) => col.endsWith(this.suffix.ref)),
    );
    /* v8 ignore start */
    const foreignKeys = Array.isArray(foreignKeysArr)
      ? foreignKeysArr.filter(Boolean).join(', ')
      : foreignKeysArr || '';
    /* v8 ignore end */
    const colsWithPrimaryKey = `${sqlCreateColumns}, ${primaryKey}`;
    const colsWithPrimaryKeyAndForeignKeys = foreignKeys
      ? `${colsWithPrimaryKey}, ${foreignKeys}`
      : colsWithPrimaryKey;

    const sqlIfNotExists = `IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '${sqltableKey.replace(
      /^\[|\]$/g,
      '',
    )}' AND TABLE_SCHEMA = '${this.schemaName}')
  BEGIN
    CREATE TABLE [${
      this.schemaName
    }].${sqltableKey} (${colsWithPrimaryKeyAndForeignKeys})
  END`;
    return sqlIfNotExists;
  }

  alterTable(tableKey: TableKey, addedColumns: ColumnCfg[]): string[] {
    const tableKeyWithSuffix = this.addTableSuffix(tableKey);
    const statements: string[] = [];
    for (const col of addedColumns) {
      const columnKey = this.addColumnSuffix(col.key);
      const columnType = this.jsonToSqlType(col.type);
      statements.push(
        `ALTER TABLE [${this.schemaName}].${tableKeyWithSuffix} ADD ${columnKey} ${columnType};`,
      );
    }
    return statements;
  }

  rowCount(tableKey: string) {
    return `SELECT COUNT(*) AS totalCount FROM [${
      this.schemaName
    }].${this.addTableSuffix(tableKey)}`;
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

    return `INSERT INTO [${this.schemaName}].${this.tbl.main}${this.suffix.tbl} ( ${columnsSql} ) VALUES (${valuesSql})`;
  }

  public schemas = (testSchemaSchema: string) =>
    `SELECT SCHEMA_NAME AS schemaName FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '${testSchemaSchema}%'`;

  public schemaTables = (schemaName: string) =>
    `SELECT TABLE_NAME AS tableKey FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '${schemaName}'`;

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
  get tableCfg() {
    return `SELECT * FROM [${this.schemaName}].${this.tbl.main}${this.suffix.tbl} WHERE key${this.suffix.col} = ?`;
  }

  get tableCfgs() {
    return `SELECT * FROM [${this.schemaName}].${this.tbl.main}${this.suffix.tbl}`;
  }

  allData(tableKey: string, namedColumns?: string) {
    if (!namedColumns) {
      namedColumns = `*`;
    }
    return `SELECT ${namedColumns} FROM [${
      this.schemaName
    }].${this.addTableSuffix(tableKey)}`;
  }

  foreignKeys(refColumnNames: string[]) {
    return refColumnNames
      .map(
        (col) =>
          `CONSTRAINT FK_${col}${this.suffix.col} FOREIGN KEY (${col}${
            this.suffix.col
          }) REFERENCES ${this.schemaName}.${this.addTableSuffix(
            col.slice(0, -this.suffix.ref.length),
          )}(${this.addColumnSuffix(this.connectingColumn)})`,
      )
      .join(', ');
  }
}
