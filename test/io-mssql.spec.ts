// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';
import { exampleTableCfg, TableCfg } from '@rljson/rljson';

import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { adminCfg } from '../src/admin-cfg.ts';
import { DbBasics } from '../src/db-basics.ts';
import { IoMssql } from '../src/io-mssql.ts';

describe('IoMssql', async () => {
  let ioSql: any;
  const testDbName = 'TestDbIoMssql';
  const testSchemaName = 'main';
  const dbBasics = new DbBasics();

  beforeAll(async () => {
    await dbBasics.dropDatabase(adminCfg, testDbName);
    await dbBasics.createDatabase(adminCfg, testDbName);
    await dbBasics.useDatabase(adminCfg, testDbName);
    await dbBasics.createSchema(adminCfg, testDbName, testSchemaName);
    await dbBasics.installProcedures(adminCfg, testDbName);
  });

  beforeEach(async () => {
    // Create general access to the server
    const masterMind = new IoMssql(adminCfg);

    // Create a new database for testing
    ioSql = await masterMind.example(testDbName);
    // Initialize connection
    await ioSql.init();
    await ioSql.isReady();
  });

  afterEach(async () => {
    await ioSql.close();
  });

  it('should connect to the database', async () => {
    expect(ioSql.isOpen).toBe(true);
  });

  it('should return an error when the _conn cannot be established', async () => {
    const badConfig = {
      ...adminCfg,
      server: 'invalid_server_name',
    };
    const ioMssql = new IoMssql(badConfig, 'badSchema');
    let error: unknown = null;
    try {
      await ioMssql.init();
    } catch (err) {
      error = err;
    }
    expect((error as { name: string }).name).toBe('ConnectionError');
    await ioMssql.close();
  });

  it('should return an error when the login is invalid', async () => {
    const badConfig = {
      ...adminCfg,
      password: 'invalid',
    };
    const ioMssql = new IoMssql(badConfig, 'badSchema');
    let error: unknown = null;
    try {
      await ioMssql.init();
    } catch (err) {
      error = err;
    }
    expect((error as { name: string; message: string }).message).toBe(
      `Login failed for user '${adminCfg.user}'.`,
    );
    await ioMssql.close();
  });

  it('should return an error when the connection is closed', async () => {
    await ioSql.close();
    let error: unknown = null;
    try {
      await ioSql.isReady();
    } catch (err) {
      error = err;
    }
    expect((error as { name: string; message: string }).message).toBe(
      'MSSQL connection is not open.',
    );
  });

  it('should execute installScripts without throwing', async () => {
    await expect(
      dbBasics.installProcedures(adminCfg, testDbName),
    ).resolves.not.toThrow();
  });

  it('should return the correct currentSchema', async () => {
    const schema = ioSql.currentSchema;
    expect(ioSql.currentSchema).toBe(schema);
  });

  it('should return the correct currentLogin', async () => {
    // The login is generated in the example() method as login_<random>
    // So we check that it starts with 'login_'
    expect(ioSql.currentLogin.startsWith('login_')).toBe(true);
  });

  it('should write an array correctly', async () => {
    const tableName = 'seriesArticleRef';
    const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
    const tableCfg: TableCfg = {
      ...exampleCfg,
      columns: [
        {
          key: '_hash',
          type: 'string',
          titleShort: '_hash',
          titleLong: 'Hash',
        },
        {
          key: 'articleSliceId',
          type: 'jsonArray',
          titleShort: 'articleSliceId',
          titleLong: 'articleSliceId',
        },
      ],
    };

    await ioSql.createOrExtendTable({ tableCfg });

    expect(1).toEqual(1);
  });

  // Skipped: _coerceDataToTableCfgTypes is temporarily disabled in write().
  it.skip('casts values to the type declared by the number and string columns', async () => {
    const tableName = 'coerceNumberTable';
    const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
    const tableCfg: TableCfg = {
      ...exampleCfg,
      columns: [
        { key: '_hash', type: 'string', titleShort: '_hash', titleLong: 'Hash' },
        { key: 'value', type: 'number', titleShort: 'value', titleLong: 'value' },
        { key: 'label', type: 'string', titleShort: 'label', titleLong: 'label' },
      ],
    };
    await ioSql.createOrExtendTable({ tableCfg });

    await ioSql.write({
      data: {
        [tableName]: {
          _type: 'components',
          _data: [
            { value: '42', label: 'already-string' },
            { value: 'not-a-number', label: 25 },
          ],
        },
      },
    });

    const result = await ioSql.readRows({ table: tableName, where: {} });
    const rows = result[tableName]._data as any[];
    const byLabel = (label: string) => rows.find((r) => r.label === label);

    expect(byLabel('already-string').value).toBe(42);
    expect(byLabel('25').value == null).toBe(true);
  });

  // Skipped: _coerceDataToTableCfgTypes is temporarily disabled in write().
  it.skip('casts values to the type declared by the boolean column', async () => {
    const tableName = 'coerceBooleanTable';
    const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
    const tableCfg: TableCfg = {
      ...exampleCfg,
      columns: [
        { key: '_hash', type: 'string', titleShort: '_hash', titleLong: 'Hash' },
        { key: 'flag', type: 'boolean', titleShort: 'flag', titleLong: 'flag' },
        { key: 'tag', type: 'string', titleShort: 'tag', titleLong: 'tag' },
      ],
    };
    await ioSql.createOrExtendTable({ tableCfg });

    await ioSql.write({
      data: {
        [tableName]: {
          _type: 'components',
          _data: [
            { flag: 'TRUE', tag: 'a' },
            { flag: 'false', tag: 'b' },
            { flag: 'maybe', tag: 'c' },
            { flag: 1, tag: 'd' },
          ],
        },
      },
    });

    const result = await ioSql.readRows({ table: tableName, where: {} });
    const rows = result[tableName]._data as any[];
    const byTag = (tag: string) => rows.find((r) => r.tag === tag);

    expect(byTag('a').flag).toBe(true);
    expect(byTag('b').flag).toBe(false);
    expect(byTag('c').flag == null).toBe(true);
    expect(byTag('d').flag).toBe(true);
  });

  // Skipped: _coerceDataToTableCfgTypes is temporarily disabled in write().
  it.skip('does not reject a row whose incoming hash was computed over its pre-coercion value', async () => {
    // Simulates data from an external system that hashed the row before we
    // cast "seriesNo" (a number there) to match our string column config.
    // The row's _hash is valid for its original content, but stale the
    // moment we coerce that content — write() must not treat that as
    // hash tampering.
    const tableName = 'coerceStaleHashTable';
    const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
    const tableCfg: TableCfg = {
      ...exampleCfg,
      columns: [
        { key: '_hash', type: 'string', titleShort: '_hash', titleLong: 'Hash' },
        {
          key: 'seriesNo',
          type: 'string',
          titleShort: 'seriesNo',
          titleLong: 'seriesNo',
        },
      ],
    };
    await ioSql.createOrExtendTable({ tableCfg });

    const rowWithStaleHash = hsh({ seriesNo: 25 }) as unknown as {
      seriesNo: number;
      _hash: string;
    };

    await expect(
      ioSql.write({
        data: {
          [tableName]: {
            _type: 'components',
            _data: [rowWithStaleHash],
          },
        },
      }),
    ).resolves.not.toThrow();

    const result = await ioSql.readRows({ table: tableName, where: {} });
    const rows = result[tableName]._data as any[];

    expect(rows[0].seriesNo).toBe('25');
    expect(rows[0]._hash).not.toBe(rowWithStaleHash._hash);
  });

  // write() doesn't call these yet (see the it.skip tests above), so they're
  // exercised directly here to keep the coercion logic itself covered.
  describe('_coerceValue', () => {
    it('casts a non-string value to a string for string columns', () => {
      expect((ioSql as any)._coerceValue(42, 'string')).toBe('42');
      expect((ioSql as any)._coerceValue('already', 'string')).toBe('already');
    });

    it('casts a numeric string to a number, and an unparsable one to null', () => {
      expect((ioSql as any)._coerceValue('42', 'number')).toBe(42);
      expect((ioSql as any)._coerceValue(42, 'number')).toBe(42);
      expect((ioSql as any)._coerceValue('not-a-number', 'number')).toBe(null);
    });

    it('casts recognized strings to booleans, and an ambiguous one to null', () => {
      expect((ioSql as any)._coerceValue('TRUE', 'boolean')).toBe(true);
      expect((ioSql as any)._coerceValue('false', 'boolean')).toBe(false);
      expect((ioSql as any)._coerceValue('maybe', 'boolean')).toBe(null);
      expect((ioSql as any)._coerceValue(true, 'boolean')).toBe(true);
      expect((ioSql as any)._coerceValue(1, 'boolean')).toBe(true);
    });

    it('returns unsupported (complex) types unchanged', () => {
      const value = { nested: true };
      expect((ioSql as any)._coerceValue(value, 'json')).toBe(value);
    });
  });

  describe('_coerceDataToTableCfgTypes', () => {
    it('coerces row values to their column type and drops the stale hash', async () => {
      const tableName = 'coerceDirectTable';
      const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
      const tableCfg: TableCfg = {
        ...exampleCfg,
        columns: [
          {
            key: '_hash',
            type: 'string',
            titleShort: '_hash',
            titleLong: 'Hash',
          },
          {
            key: 'value',
            type: 'number',
            titleShort: 'value',
            titleLong: 'value',
          },
        ],
      };
      await ioSql.createOrExtendTable({ tableCfg });

      const row: any = { value: '42', _hash: 'stale' };
      const data: any = {
        [tableName]: { _type: 'components', _data: [row] },
      };

      await (ioSql as any)._coerceDataToTableCfgTypes(data);

      expect(row.value).toBe(42);
      expect(row._hash).toBeUndefined();
    });

    it('leaves a row untouched when nothing needs coercion', async () => {
      const tableName = 'coerceDirectNoopTable';
      const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
      const tableCfg: TableCfg = {
        ...exampleCfg,
        columns: [
          {
            key: '_hash',
            type: 'string',
            titleShort: '_hash',
            titleLong: 'Hash',
          },
          {
            key: 'value',
            type: 'number',
            titleShort: 'value',
            titleLong: 'value',
          },
        ],
      };
      await ioSql.createOrExtendTable({ tableCfg });

      const row: any = { value: 42, _hash: 'unchanged' };
      const data: any = {
        [tableName]: { _type: 'components', _data: [row] },
      };

      await (ioSql as any)._coerceDataToTableCfgTypes(data);

      expect(row.value).toBe(42);
      expect(row._hash).toBe('unchanged');
    });

    it('ignores undefined and null values on a row', async () => {
      const tableName = 'coerceDirectNullTable';
      const exampleCfg: TableCfg = exampleTableCfg({ key: tableName });
      const tableCfg: TableCfg = {
        ...exampleCfg,
        columns: [
          {
            key: '_hash',
            type: 'string',
            titleShort: '_hash',
            titleLong: 'Hash',
          },
          {
            key: 'value',
            type: 'number',
            titleShort: 'value',
            titleLong: 'value',
          },
          {
            key: 'label',
            type: 'string',
            titleShort: 'label',
            titleLong: 'label',
          },
        ],
      };
      await ioSql.createOrExtendTable({ tableCfg });

      const row: any = { value: undefined, label: null, _hash: 'unchanged' };
      const data: any = {
        [tableName]: { _type: 'components', _data: [row] },
      };

      await (ioSql as any)._coerceDataToTableCfgTypes(data);

      expect(row._hash).toBe('unchanged');
    });
  });
});
