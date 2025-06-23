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
