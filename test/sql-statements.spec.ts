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
        { key: 'e', type: 'jsonArray' },
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
  it('should generate correct SQL for currentTableCfg getter', () => {
    const sql = sqlStatements.currentTableCfg;
    expect(sql).toContain('WITH versions AS (');
    expect(sql).toContain(
      'SELECT _hash_col, key_col, MAX(json_each.key) AS max_val',
    );
    expect(sql).toContain('FROM tableCfgs_tbl, json_each(columns_col)');
    expect(sql).toContain('WHERE json_each.value IS NOT NULL');
    expect(sql).toContain('AND key_col = ? GROUP BY _hash_col, key_col)');
    expect(sql).toContain('SELECT * FROM tableCfgs_tbl tt');
    expect(sql).toContain(
      'LEFT JOIN versions ON tt._hash_col = versions._hash_col',
    );
    expect(sql).toContain(
      'WHERE versions.max_val = (SELECT MAX(max_val) FROM versions);',
    );
    expect(typeof sql).toBe('string');
    expect(sql).toMatch(/WITH versions AS \(/);
    expect(sql).toMatch(
      /LEFT JOIN versions ON tt\._hash_col = versions\._hash_col/,
    );
  });
  it('should generate correct SQL for currentTableCfgs getter', () => {
    const sql = sqlStatements.currentTableCfgs;
    expect(typeof sql).toBe('string');
    expect(sql).toContain('SELECT');
    expect(sql).toContain('FROM');
    expect(sql).toContain('tableCfgs_tbl');
    expect(sql).toContain('WITH');
    expect(sql).toContain('column_count AS (');
    expect(sql).toContain('max_tables AS (');
    expect(sql).toContain('LEFT JOIN max_tables');
    expect(sql).toMatch(/_hash_col IN \(\s*WITH/);
    expect(sql).toMatch(/SELECT\s+\*\s+FROM\s+tableCfgs_tbl/);
    expect(sql).toMatch(/GROUP BY\s+key_col/);
    expect(sql).toMatch(/WHERE\s+mt\.newest IS NOT NULL/);
  });
  it('should add and remove suffixes for custom suffixes', () => {
    expect(sqlStatements.addFix('foo', '_bar')).toBe('foo_bar');
    expect(sqlStatements.addFix('foo_bar', '_bar')).toBe('foo_bar');
    expect(sqlStatements.remFix('foo_bar', '_bar')).toBe('foo');
    expect(sqlStatements.remFix('foo', '_bar')).toBe('foo');
  });

  it('should generate correct SQL for joinExpression', () => {
    expect(sqlStatements.joinExpression('table1', 't1')).toBe(
      'LEFT JOIN table1 AS t1 \n',
    );
  });

  it('should generate correct SQL for articleExists', () => {
    expect(sqlStatements.articleExists).toContain(
      'SELECT cl.layer, ar.assign FROM catalogLayers cl',
    );
    expect(sqlStatements.articleExists).toContain('LEFT JOIN articleSets ar');
    expect(sqlStatements.articleExists).toContain('WHERE cl.winNumber = ?');
  });

  it('should generate correct SQL for catalogExists', () => {
    expect(sqlStatements.catalogExists).toBe(
      'SELECT 1 FROM catalogLayers WHERE winNumber = @winNumber',
    );
  });

  it('should generate correct SQL for catalogArticleTypes', () => {
    expect(sqlStatements.catalogArticleTypes).toContain(
      'SELECT articleType FROM currentArticles',
    );
    expect(sqlStatements.catalogArticleTypes).toContain('WHERE winNumber = ?');
    expect(sqlStatements.catalogArticleTypes).toContain('GROUP BY articleType');
  });

  it('should generate correct SQL for foreignKeys', () => {
    const result = sqlStatements.foreignKeys(['fooRef', 'barRef']);
    expect(result).toContain(
      'CONSTRAINT FK_fooRef_col FOREIGN KEY (fooRef_col) REFERENCES foo(',
    );
    expect(result).toContain(
      'CONSTRAINT FK_barRef_col FOREIGN KEY (barRef_col) REFERENCES bar(',
    );
  });

  it('should generate correct SQL for tableReferences', () => {
    const result = sqlStatements.tableReferences(['fooRef', 'barRef']);
    expect(result).toContain('FOREIGN KEY (fooRef) REFERENCES foo (_hash_col)');
    expect(result).toContain('FOREIGN KEY (barRef) REFERENCES bar (_hash_col)');
  });

  it('should generate correct SQL for insertTableCfg', () => {
    const sql = sqlStatements.insertTableCfg();
    expect(sql).toContain('INSERT INTO tableCfgs_tbl');
    expect(sql).toContain('VALUES');
    expect(sql).toMatch(/VALUES\s*\((\?,\s*)+\?\)/);
  });

  it('should generate correct SQL for tableType', () => {
    expect(sqlStatements.tableType).toContain(
      'SELECT type_col AS type FROM tableCfgs_col',
    );
    expect(sqlStatements.tableType).toContain('WHERE key_col = ?');
    expect(sqlStatements.tableType).toContain('SELECT MAX(version_col)');
  });

  it('should generate correct SQL for columnKeys', () => {
    expect(sqlStatements.columnKeys('myTable')).toBe(
      'PRAGMA table_info(myTable)',
    );
  });

  it('should generate correct SQL for createFullTable', () => {
    const sql = sqlStatements.createFullTable(
      'foo',
      'a_col TEXT',
      'FOREIGN KEY (b) REFERENCES bar(_hash_col)',
    );
    expect(sql).toBe(
      'CREATE TABLE foo (a_col TEXT, FOREIGN KEY (b) REFERENCES bar(_hash_col))',
    );
  });

  it('should generate correct SQL for createTableCfgsTable', () => {
    const sql = sqlStatements.createTableCfgsTable;
    expect(sql).toContain('CREATE TABLE tableCfgs_tbl');
    expect(sql).toContain('_hash_col TEXT PRIMARY KEY');
  });

  it('should throw error for unsupported column type in parseData', () => {
    const tableCfg: TableCfg = {
      key: 'test',
      type: 'ingredients',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [{ key: 'a', type: 'unsupported' as any }],
    };
    const data = [{ a_col: 'foo' }];
    expect(() => sqlStatements.parseData(data, tableCfg)).toThrow(
      'Unsupported column type unsupported',
    );
  });

  it('should handle null and undefined values in parseData', () => {
    const tableCfg: TableCfg = {
      key: 'test',
      type: 'ingredients',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [
        { key: 'a', type: 'string' },
        { key: 'b', type: 'number' },
      ],
    };
    const data = [{ a_col: null, b_col: undefined }];
    const parsed = sqlStatements.parseData(data, tableCfg);
    expect(parsed).toEqual([{}]);
  });

  it('should serializeRow with missing keys as null', () => {
    const tableCfg: TableCfg = {
      key: 'test',
      type: 'ingredients',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [
        { key: 'a', type: 'string' },
        { key: 'b', type: 'number' },
      ],
    };
    const row = { a: 'foo' };
    const result = sqlStatements.serializeRow(row, tableCfg);
    expect(result).toEqual(['foo', null]);
  });

  it('should serializeRow with boolean false as 0', () => {
    const tableCfg: TableCfg = {
      key: 'test',
      type: 'ingredients',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [{ key: 'a', type: 'boolean' }],
    };
    const row = { a: false };
    const result = sqlStatements.serializeRow(row, tableCfg);
    expect(result).toEqual([0]);
  });
});
