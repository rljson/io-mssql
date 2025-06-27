// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be

// found in the LICENSE file in the root of this package.
import { describe, expect, it } from 'vitest';

import { MsSqlStatements } from '../src/mssql-statements';

describe('MsSqlStatements.jsonToSqlType', () => {
  const msSqlStatements = new MsSqlStatements('dbo');

  it('should return NVARCHAR(MAX) for "string"', () => {
    expect(msSqlStatements.jsonToSqlType('string')).toBe('NVARCHAR(MAX)');
  });

  it('should return NVARCHAR(MAX) for "jsonArray"', () => {
    expect(msSqlStatements.jsonToSqlType('jsonArray')).toBe('NVARCHAR(MAX)');
  });

  it('should return NVARCHAR(MAX) for "json"', () => {
    expect(msSqlStatements.jsonToSqlType('json')).toBe('NVARCHAR(MAX)');
  });

  it('should return FLOAT for "number"', () => {
    expect(msSqlStatements.jsonToSqlType('number')).toBe('FLOAT');
  });

  it('should return BIT for "boolean"', () => {
    expect(msSqlStatements.jsonToSqlType('boolean')).toBe('BIT');
  });

  it('should return NVARCHAR(MAX) for "jsonValue"', () => {
    expect(msSqlStatements.jsonToSqlType('jsonValue')).toBe('NVARCHAR(MAX)');
  });

  it('should throw an error for unknown JsonValueType', () => {
    // @ts-expect-error Testing invalid input
    expect(() => msSqlStatements.jsonToSqlType('unknown')).toThrowError(
      'Unknown JsonValueType: unknown',
    );
  });
});
describe('MsSqlStatements.schemas', () => {
  const msSqlStatements = new MsSqlStatements('dbo');

  it('should generate correct SQL for schemas with given prefix', () => {
    const testSchemaSchema = 'testPrefix';
    const expectedSql =
      "SELECT SCHEMA_NAME AS schemaName FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE 'testPrefix%'";
    expect(msSqlStatements.schemas(testSchemaSchema)).toBe(expectedSql);
  });

  it('should handle empty prefix', () => {
    const testSchemaSchema = '';
    const expectedSql =
      "SELECT SCHEMA_NAME AS schemaName FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%'";
    expect(msSqlStatements.schemas(testSchemaSchema)).toBe(expectedSql);
  });
});

describe('MsSqlStatements.schemaTables', () => {
  const msSqlStatements = new MsSqlStatements('dbo');

  it('should generate correct SQL for schemaTables with given schemaName', () => {
    const schemaName = 'customSchema';
    const expectedSql =
      "SELECT TABLE_NAME AS tableKey FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'customSchema'";
    expect(msSqlStatements.schemaTables(schemaName)).toBe(expectedSql);
  });

  it('should handle empty schemaName', () => {
    const schemaName = '';
    const expectedSql =
      "SELECT TABLE_NAME AS tableKey FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ''";
    expect(msSqlStatements.schemaTables(schemaName)).toBe(expectedSql);
  });
});
describe('MsSqlStatements.allData', () => {
  const msSqlStatements = new MsSqlStatements('dbo');
  const tableKey = msSqlStatements.addTableSuffix('myTable');
  const testSchema = msSqlStatements.schemaName;
  it('should generate correct SQL for all columns when namedColumns is not provided', () => {
    const expectedSql = `SELECT * FROM [${testSchema}].[${tableKey}]`;
    expect(msSqlStatements.allData(tableKey)).toBe(expectedSql);
  });

  it('should generate correct SQL for specified namedColumns', () => {
    const namedColumns = 'col1, col2';
    const expectedSql = `SELECT col1, col2 FROM [${testSchema}].[${tableKey}]`;
    expect(msSqlStatements.allData(tableKey, namedColumns)).toBe(expectedSql);
  });
});
describe('MsSqlStatements.tableCfg', () => {
  const msSqlStatements = new MsSqlStatements('dbo');

  it('should generate correct SQL for tableCfg getter', () => {
    const expectedSql = `SELECT * FROM [${msSqlStatements.schemaName}].${msSqlStatements.tbl.main}${msSqlStatements.suffix.tbl} WHERE key${msSqlStatements.suffix.col} = ?`;
    expect(msSqlStatements.tableCfg).toBe(expectedSql);
  });
  describe('MsSqlStatements.whereString', () => {
    const msSqlStatements = new MsSqlStatements('dbo');

    it('should generate correct SQL for string value', () => {
      const whereClause: [string, string][] = [['name', 'John']];
      expect(msSqlStatements.whereString(whereClause)).toBe(
        " name_col = 'John'",
      );
    });

    it('should generate correct SQL for number value', () => {
      const whereClause: [string, number][] = [['age', 30]];
      expect(msSqlStatements.whereString(whereClause)).toBe(' age_col = 30');
    });

    it('should generate correct SQL for boolean true value', () => {
      const whereClause: [string, boolean][] = [['isActive', true]];
      expect(msSqlStatements.whereString(whereClause)).toBe(
        ' isActive_col = 1',
      );
    });

    it('should generate correct SQL for boolean false value', () => {
      const whereClause: [string, boolean][] = [['isActive', false]];
      expect(msSqlStatements.whereString(whereClause)).toBe(
        ' isActive_col = 0',
      );
    });

    it('should generate correct SQL for boolean false value', () => {
      const whereClause: [string, boolean][] = [['isActive', true]];
      expect(msSqlStatements.whereString(whereClause)).toBe(
        ' isActive_col = 1',
      );
    });
  });
});
