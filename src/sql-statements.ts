// @license
// Copyright (c) 2025 CARAT Gesellschaft für Organisation
// und Softwareentwicklung mbH. All Rights Reserved.
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

//****************************************************** */
// These sql statements are partly based on the SQLite database;
// see https://www.sqlite.org/lang.html
// and https://www.sqlite.org/cli.html
//****************************************************** */

import { IoTools } from '@rljson/io';
import { Json, JsonValue, JsonValueType } from '@rljson/json';
import { ColumnCfg, TableCfg, TableKey } from '@rljson/rljson';

// import { refName } from './constants.ts';

export class SqlStatements {
  /// simple  keywords and statements*******************
  connectingColumn: string = '_hash';
  queryIntro: string = 'SELECT DISTINCT';

  // Names for the main tables in the database
  tbl: { [key: string]: string } = {
    main: 'tableCfgs',
    revision: 'revisions',
  };

  /// Postfix handling for the database
  suffix: { [key: string]: string } = {
    col: '_col',
    tbl: '_tbl',
    tmp: '_tmp',
    ref: 'Ref',
  };

  /**
   * Converts a JSON value type to an SQLite data type.
   * @param dataType - The JSON value type to convert.
   * @returns - The corresponding SQLite data type.
   */
  jsonToSqlType(dataType: JsonValueType): string {
    switch (dataType) {
      case 'string':
        return 'TEXT';
      case 'jsonArray':
        return 'TEXT';
      case 'json':
        return 'TEXT';
      case 'number':
        return 'REAL';
      case 'boolean':
        return 'INTEGER';
      case 'jsonValue':
        return 'TEXT';
    }
  }

  public createDatabase = (dbName: string) => `CREATE DATABASE [${dbName}]`;
  public dropDatabase = (dbName: string) => `DROP DATABASE [${dbName}]`;

  get tableKey() {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name=?`;
  }
  get tableKeys() {
    return `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
  }

  rowCount(tableKey: string) {
    return `SELECT COUNT(*) FROM ${this.addTableSuffix(tableKey)}`;
  }
  foreignKeyList(tableKey: string) {
    return `PRAGMA foreign_key_list(${tableKey})`;
  }

  allData(tableKey: string, namedColumns?: string) {
    if (!namedColumns) {
      namedColumns = `*`;
    }
    return `SELECT ${namedColumns} FROM ${this.addTableSuffix(tableKey)}`;
  }

  get tableCfg() {
    return `SELECT * FROM ${this.tbl.main}${this.suffix.tbl} WHERE key${this.suffix.col} = ?`;
  }

  get tableCfgs() {
    return `SELECT * FROM ${this.tbl.main}${this.suffix.tbl}`;
  }

  /* v8 ignore start */
  get currentTableCfg(): string {
    const sql: string[] = [
      'WITH versions AS (',
      ' SELECT _hash_col, key_col, MAX(json_each.key) AS max_val',
      ' FROM tableCfgs_tbl, json_each(columns_col)',
      ' WHERE json_each.value IS NOT NULL',
      ' AND key_col = ? GROUP BY _hash_col, key_col)',
      'SELECT * FROM tableCfgs_tbl tt',
      ' LEFT JOIN versions ON tt._hash_col = versions._hash_col',
      ' WHERE versions.max_val = (SELECT MAX(max_val) FROM versions);',
    ];
    return sql.join('\n');
  }
  /* v8 ignore end */

  get currentTableCfgs(): string {
    const sql: string[] = [
      'SELECT',
      '  *',
      'FROM',
      '  tableCfgs_tbl',
      'WHERE',
      '  _hash_col IN (',
      '    WITH',
      '      column_count AS (',
      '        SELECT',
      '          _hash_col,',
      '          key_col,',
      '          MAX(json_each.key) AS max_val',
      '        FROM',
      '          tableCfgs_tbl,',
      '          json_each (columns_col)',
      '        WHERE',
      '          json_each.value IS NOT NULL',
      '        GROUP BY',
      '          _hash_col,',
      '          key_col',
      '      ),',
      '      max_tables AS (',
      '        SELECT',
      '          key_col,',
      '          MAX(max_val) AS newest',
      '        FROM',
      '          column_count',
      '        GROUP BY',
      '          key_col',
      '      )',
      '    SELECT',
      '      cc._hash_col',
      '    FROM',
      '      column_count cc',
      '      LEFT JOIN max_tables mt ON cc.key_col = mt.key_col',
      '      AND cc.max_val = mt.newest',
      '    WHERE',
      '      mt.newest IS NOT NULL',
      '  );',
    ];
    return sql.join('\n');
  }

  addColumnSuffix(name: string): string {
    return this.addFix(name, this.suffix.col);
  }

  addTableSuffix(name: string): string {
    return this.addFix(name, this.suffix.tbl);
  }

  // add suffix to a name in order to avoid name conflicts
  addFix(name: string, fix: string): string {
    return name.endsWith(fix) ? name : name + fix;
  }

  removeTableSuffix(name: string): string {
    return this.remFix(name, this.suffix.tbl);
  }

  removeColumnSuffix(name: string): string {
    return this.remFix(name, this.suffix.col);
  }

  // remove suffix from a name in order to communicate with the outside world
  remFix(name: string, fix: string): string {
    return name.endsWith(fix) ? name.slice(0, -fix.length) : name;
  }

  joinExpression(tableKey: string, alias: string) {
    return `LEFT JOIN ${tableKey} AS ${alias} \n`;
  }

  get articleExists() {
    return (
      'SELECT cl.layer, ar.assign FROM catalogLayers cl\n' +
      'LEFT JOIN articleSets ar\n' +
      'ON cl.articleSetsRef = ar._hash\n' +
      'WHERE cl.winNumber = ? '
    );
  }
  get catalogExists() {
    return 'SELECT 1 FROM catalogLayers WHERE winNumber = @winNumber';
  }
  get catalogArticleTypes() {
    return (
      `SELECT articleType FROM currentArticles\n` +
      `WHERE winNumber = ?\n` +
      `GROUP BY articleType`
    );
  }

  foreignKeyReferences(refColumnNames: string[]) {
    return refColumnNames
      .map(
        (col: string | any[]) =>
          `FOREIGN KEY (${col}${this.suffix.col}) REFERENCES ${col.slice(
            0,
            -this.suffix.ref.length,
          )}(${this.addColumnSuffix(this.connectingColumn)})`,
      )
      .join(', ');
  }

  tableReferences(referenceArray: string[]): string {
    return referenceArray
      .map(
        (col) =>
          `FOREIGN KEY (${col}) REFERENCES ${col.slice(
            0,
            -this.suffix.ref.length,
          )} (${this.addColumnSuffix(this.connectingColumn)})`,
      )
      .join(', ');
  }

  insertTableCfg() {
    const columnKeys = IoTools.tableCfgsTableCfg.columns.map((col) => col.key);
    const columnKeysWithPostfix = columnKeys.map((col) =>
      this.addColumnSuffix(col),
    );
    const columnsSql = columnKeysWithPostfix.join(', ');
    const valuesSql = '?, '.repeat(columnKeys.length - 1) + '?';

    return `INSERT INTO ${this.tbl.main}${this.suffix.tbl} ( ${columnsSql} ) VALUES (${valuesSql})`;
  }

  get tableExists() {
    return `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`;
  }

  get tableType() {
    return `SELECT type${this.suffix.col} AS type FROM ${this.tbl.main}${this.suffix.col} WHERE key${this.suffix.col} = ? AND version${this.suffix.col} = (SELECT MAX(version${this.suffix.col}) FROM ${this.tbl.main}${this.suffix.col} WHERE key${this.suffix.col} = ?)`;
  }

  columnKeys(tableKey: string) {
    return `PRAGMA table_info(${tableKey})`;
  }

  createFullTable(
    tableKey: string,
    columnsDefinition: string,
    foreignKeys: string,
  ) {
    return `CREATE TABLE ${tableKey} (${columnsDefinition}, ${foreignKeys})`;
  }

  dropTable(tableKey: string) {
    return `DROP TABLE IF EXISTS ${tableKey}${this.suffix.tbl}`;
  }

  createTempTable(tableKey: string) {
    return `CREATE TABLE ${tableKey}${this.suffix.tmp} AS SELECT * FROM ${tableKey}${this.suffix.tbl}`;
  }

  dropTempTable(tableKey: string) {
    return `DROP TABLE IF EXISTS ${tableKey}${this.suffix.tmp}`;
  }

  fillTable(tableKey: string, commonColumns: string) {
    // select only those columns that are in both tables
    return `INSERT INTO ${tableKey}${this.suffix.tbl} (${commonColumns}) SELECT ${commonColumns} FROM ${tableKey}${this.suffix.tmp}`;
  }

  deleteFromTable(tableKey: string, winNumber: string) {
    return `DELETE FROM ${tableKey} WHERE winNumber = '${winNumber}'`;
  }

  addColumn(tableKey: string, columnName: string, columnType: string) {
    return `ALTER TABLE ${tableKey} ADD COLUMN ${columnName} ${columnType}`;
  }

  selection(tableKey: string, columns: string, whereClause: string) {
    return `SELECT ${columns} FROM ${tableKey} WHERE ${whereClause}`;
  }

  articleSetsRefs(winNumber: string) {
    return `SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '${winNumber}'`;
  }

  get insertCurrentArticles() {
    return `INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)`;
  }

  currentCount(tableKey: string) {
    return `SELECT COUNT(*) FROM ${this.addTableSuffix(tableKey)}`;
  }

  createTable(tableCfg: TableCfg): string {
    const sqltableKey = this.addTableSuffix(tableCfg.key);
    const columnsCfg = tableCfg.columns;

    const sqlCreateColumns = columnsCfg
      .map((col) => {
        const sqliteType = this.jsonToSqlType(col.type);
        return `${this.addColumnSuffix(col.key)} ${sqliteType}`;
      })
      .join(', ');

    // standard primary key - do not remove ;-)

    const conKey = `${this.connectingColumn}${this.suffix.col} TEXT`;
    const primaryKey = `${conKey} PRIMARY KEY`;

    const colsWithPrimaryKey = sqlCreateColumns.replace(conKey, primaryKey);

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
        `ALTER TABLE ${tableKeyWithSuffix} ADD COLUMN ${columnKey} ${columnType};`,
      );
    }

    return statements;
  }

  get createTableCfgsTable() {
    return this.createTable(IoTools.tableCfgsTableCfg);
  }

  get tableTypeCheck() {
    return `SELECT type${this.suffix.col} FROM tableCfgs${this.suffix.tbl} WHERE key${this.suffix.col} = ?`;
  }

  parseData(data: Json[], tableCfg: TableCfg): Json[] {
    const columnTypes = tableCfg.columns.map((col) => col.type);
    const columnKeys = tableCfg.columns.map((col) => col.key);

    const convertedResult: Json[] = [];

    for (const row of data) {
      const convertedRow: { [key: string]: any } = {};
      for (let colNum = 0; colNum < columnKeys.length; colNum++) {
        const key = columnKeys[colNum];
        const keyWithSuffix = this.addColumnSuffix(key);
        const type = columnTypes[colNum] as JsonValueType;
        const val = row[keyWithSuffix];

        // Null or undefined values are ignored
        // and not added to the converted row
        if (val === undefined) {
          continue;
        }

        if (val === null) {
          continue;
        }

        switch (type) {
          case 'boolean':
            convertedRow[key] = val !== 0;
            break;
          case 'jsonArray':
          case 'json':
            convertedRow[key] = JSON.parse(val as string);
            break;
          case 'string':
          case 'number':
            convertedRow[key] = val;
            break;
          /* v8 ignore start */
          default:
            throw new Error('Unsupported column type ' + type);
          /* v8 ignore end */
        }
      }

      convertedResult.push(convertedRow);
    }

    return convertedResult;
  }

  public serializeRow(
    rowAsJson: Json,
    tableCfg: TableCfg,
  ): (JsonValue | null)[] {
    const result: (JsonValue | null)[] = [];

    // Iterate all columns in the tableCfg
    for (const col of tableCfg.columns) {
      const key = col.key;
      let value = rowAsJson[key] ?? null;
      const valueType = typeof value;

      // Stringify objects and arrays
      if (value !== null && valueType === 'object') {
        value = JSON.stringify(value);
      }

      // Convert booleans to 1 or 0
      else if (valueType === 'boolean') {
        value = value ? 1 : 0;
      }

      result.push(value);
    }

    return result;
  }
}
