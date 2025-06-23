import { ColumnCfg, TableCfg } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { SqlStatements } from '../src/sql-statements';

describe('SqlStatements', () => {
  let sqlStatements: SqlStatements;

  beforeEach(() => {
    sqlStatements = new SqlStatements();
  });

  it('should convert JSON value types to SQL types', () => {
    expect(sqlStatements.jsonToSqlType('string')).toBe('TEXT');
    expect(sqlStatements.jsonToSqlType('jsonArray')).toBe('TEXT');
    expect(sqlStatements.jsonToSqlType('json')).toBe('TEXT');
    expect(sqlStatements.jsonToSqlType('number')).toBe('REAL');
    expect(sqlStatements.jsonToSqlType('boolean')).toBe('INTEGER');
    expect(sqlStatements.jsonToSqlType('jsonValue')).toBe('TEXT');
  });

  it('should add and remove suffixes correctly', () => {
    expect(sqlStatements.addColumnSuffix('foo')).toBe('foo_col');
    expect(sqlStatements.addTableSuffix('bar')).toBe('bar_tbl');
    expect(sqlStatements.addFix('baz', '_tmp')).toBe('baz_tmp');
    expect(sqlStatements.removeTableSuffix('bar_tbl')).toBe('bar');
    expect(sqlStatements.removeColumnSuffix('foo_col')).toBe('foo');
    expect(sqlStatements.remFix('baz_tmp', '_tmp')).toBe('baz');
  });

  it('should generate create and drop database statements', () => {
    expect(sqlStatements.createDatabase('testdb')).toBe(
      'CREATE DATABASE [testdb]',
    );
    expect(sqlStatements.dropDatabase('testdb')).toBe('DROP DATABASE [testdb]');
  });

  it('should generate row count and allData statements', () => {
    expect(sqlStatements.rowCount('myTable')).toBe(
      'SELECT COUNT(*) FROM myTable_tbl',
    );
    expect(sqlStatements.allData('myTable')).toBe('SELECT * FROM myTable_tbl');
    expect(sqlStatements.allData('myTable', 'col1, col2')).toBe(
      'SELECT col1, col2 FROM myTable_tbl',
    );
  });

  it('should generate join expressions', () => {
    expect(sqlStatements.joinExpression('myTable', 't')).toBe(
      'LEFT JOIN myTable AS t \n',
    );
  });

  it('should generate tableExists and tableTypeCheck statements', () => {
    expect(sqlStatements.tableExists).toBe(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
    );
    expect(sqlStatements.tableTypeCheck).toContain(
      'SELECT type_col FROM tableCfgs_tbl',
    );
  });

  it('should generate alterTable statements', () => {
    const addedColumns: ColumnCfg[] = [
      { key: 'foo', type: 'string' },
      { key: 'bar', type: 'number' },
    ];
    const stmts = sqlStatements.alterTable('myTable', addedColumns);
    expect(stmts[0]).toBe('ALTER TABLE myTable_tbl ADD COLUMN foo_col TEXT;');
    expect(stmts[1]).toBe('ALTER TABLE myTable_tbl ADD COLUMN bar_col REAL;');
  });

  it('should serialize row as expected', () => {
    const tableCfg: TableCfg = {
      key: 'test',
      type: 'ingredients',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [
        { key: 'a', type: 'string' },
        { key: 'b', type: 'number' },
        { key: 'c', type: 'boolean' },
        { key: 'd', type: 'json' },
      ],
    };
    const row = { a: 'foo', b: 42, c: true, d: { x: 1 } };
    const result = sqlStatements.serializeRow(row, tableCfg);
    expect(result).toEqual(['foo', 42, 1, JSON.stringify({ x: 1 })]);
  });

  it('should parse data as expected', () => {
    const tableCfg: TableCfg = {
      key: 'test',
      type: 'ingredients',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [
        { key: 'a', type: 'string' },
        { key: 'b', type: 'number' },
        { key: 'c', type: 'boolean' },
        { key: 'd', type: 'json' },
      ],
    };
    const data = [
      { a_col: 'foo', b_col: 42, c_col: 1, d_col: '{"x":1}' },
      { a_col: 'bar', b_col: 7, c_col: 0, d_col: '{"y":2}' },
    ];
    const parsed = sqlStatements.parseData(data, tableCfg);
    expect(parsed).toEqual([
      { a: 'foo', b: 42, c: true, d: { x: 1 } },
      { a: 'bar', b: 7, c: false, d: { y: 2 } },
    ]);
  });

  it('should generate createTable SQL', () => {
    const tableCfg: TableCfg = {
      key: 'myTable',
      type: 'ingredients',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [
        { key: '_hash', type: 'string' },
        { key: 'foo', type: 'number' },
      ],
    };
    const sql = sqlStatements.createTable(tableCfg);
    expect(sql).toContain('CREATE TABLE myTable_tbl');
    expect(sql).toContain('_hash_col TEXT PRIMARY KEY');
    expect(sql).toContain('foo_col REAL');
  });

  it('should generate dropTable and createTempTable SQL', () => {
    expect(sqlStatements.dropTable('myTable')).toBe(
      'DROP TABLE IF EXISTS myTable_tbl',
    );
    expect(sqlStatements.createTempTable('myTable')).toBe(
      'CREATE TABLE myTable_tmp AS SELECT * FROM myTable_tbl',
    );
    expect(sqlStatements.dropTempTable('myTable')).toBe(
      'DROP TABLE IF EXISTS myTable_tmp',
    );
  });

  it('should generate fillTable SQL', () => {
    expect(sqlStatements.fillTable('myTable', 'col1, col2')).toBe(
      'INSERT INTO myTable_tbl (col1, col2) SELECT col1, col2 FROM myTable_tmp',
    );
  });

  it('should generate deleteFromTable SQL', () => {
    expect(sqlStatements.deleteFromTable('myTable', '123')).toBe(
      "DELETE FROM myTable WHERE winNumber = '123'",
    );
  });

  it('should generate addColumn SQL', () => {
    expect(sqlStatements.addColumn('myTable', 'foo', 'TEXT')).toBe(
      'ALTER TABLE myTable ADD COLUMN foo TEXT',
    );
  });

  it('should generate selection SQL', () => {
    expect(sqlStatements.selection('myTable', 'col1, col2', 'col1 = 1')).toBe(
      'SELECT col1, col2 FROM myTable WHERE col1 = 1',
    );
  });

  it('should generate articleSetsRefs SQL', () => {
    expect(sqlStatements.articleSetsRefs('123')).toBe(
      "SELECT layer, articleSetsRef FROM catalogLayers WHERE winNumber = '123'",
    );
  });

  it('should generate insertCurrentArticles SQL', () => {
    expect(sqlStatements.insertCurrentArticles).toBe(
      'INSERT OR IGNORE INTO currentArticles (winNumber, articleType, layer, articleHash) VALUES (?, ?, ?, ?)',
    );
  });

  it('should generate currentCount SQL', () => {
    expect(sqlStatements.currentCount('myTable')).toBe(
      'SELECT COUNT(*) FROM myTable_tbl',
    );
  });

  it('should generate correct SQL for tableKey', () => {
    expect(sqlStatements.tableKey).toBe(
      "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
    );
  });

  it('should generate correct SQL for tableKeys', () => {
    expect(sqlStatements.tableKeys).toBe(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    );
  });
  it('should generate correct PRAGMA foreign_key_list statement', () => {
    expect(sqlStatements.foreignKeyList('myTable')).toBe(
      'PRAGMA foreign_key_list(myTable)',
    );
    expect(sqlStatements.foreignKeyList('anotherTable')).toBe(
      'PRAGMA foreign_key_list(anotherTable)',
    );
    // Test with table name containing special characters
    expect(sqlStatements.foreignKeyList('table_123')).toBe(
      'PRAGMA foreign_key_list(table_123)',
    );
  });
  it('should generate correct SQL for tableCfg getter', () => {
    const sql = sqlStatements.tableCfg;
    expect(sql).toBe('SELECT * FROM tableCfgs_tbl WHERE key_col = ?');
  });

  it('should generate correct SQL for tableCfgs getter', () => {
    const sql = sqlStatements.tableCfgs;
    expect(sql).toBe('SELECT * FROM tableCfgs_tbl');
  });
});
