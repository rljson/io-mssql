// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { resolve } from 'node:path';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';

import { adminCfg } from '../src/admin-cfg.ts';
import { main, readStdin } from '../src/cli.ts';
import { DbBasics } from '../src/db-basics.ts';
import { IoMssql } from '../src/io-mssql.ts';

const ROOT = resolve(fileURLToPath(import.meta.url), '..', '..');

const CONN_ARGS = [
  '--server',
  adminCfg.server!,
  '--port',
  String(adminCfg.port),
  '--user',
  adminCfg.user!,
  '--password',
  adminCfg.password!,
];

const TEST_DB = 'TestDbCli';
const CATALOG_DB = 'TestDbCliCatalog';
const SCHEMA = 'main';
const DB_ARGS = [...CONN_ARGS, '--database', TEST_DB, '--schema', SCHEMA];
const CATALOG_FILE = resolve(ROOT, 'data', 'minimal-catalog.json');
const WRITE_FILE = resolve(ROOT, 'test', 'fixtures', 'write-sample.json');

describe('CLI', () => {
  const dbBasics = new DbBasics();

  beforeAll(async () => {
    await dbBasics.dropDatabase(adminCfg, TEST_DB);
    await dbBasics.createDatabase(adminCfg, TEST_DB);
    await dbBasics.createSchema(adminCfg, TEST_DB, SCHEMA);
    await dbBasics.installProcedures(adminCfg, TEST_DB);

    const io = new IoMssql({ ...adminCfg, database: TEST_DB }, SCHEMA);
    try {
      await io.init();
      await io.createOrExtendTable({
        tableCfg: {
          key: 'smallTable',
          type: 'components',
          isHead: false,
          isRoot: false,
          isShared: false,
          columns: [
            {
              key: '_hash',
              type: 'string',
              titleLong: 'Hash',
              titleShort: 'H',
            },
            { key: 'name', type: 'string', titleLong: 'Name', titleShort: 'N' },
          ],
        },
      });
      await io.write({
        data: {
          smallTable: {
            _type: 'components',
            _data: [{ _hash: 'l2112PPVjy1p1stUN59jZr', name: 'seedRow' }],
          },
        },
      });
    } finally {
      await io.close();
    }
  });

  afterAll(async () => {
    await dbBasics.dropDatabase(adminCfg, TEST_DB);
    await dbBasics.dropDatabase(adminCfg, CATALOG_DB);
  });

  beforeEach(() => {
    vi.spyOn(process, 'exit').mockImplementation(
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      (_code?: string | number | null | undefined) => {
        throw new Error('process.exit called');
      },
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // ── help ──────────────────────────────────────────────────────────────────

  it('prints help when called with no args', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    await main([]);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('io-mssql'));
  });

  it('prints help with --help', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    await main(['--help']);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('Commands:'));
  });

  it('prints help with -h', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    await main(['-h']);
    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('Commands:'));
  });

  // ── missing --database ───────────────────────────────────────────────────

  it('exits when --database is missing for non-catalog commands', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    await expect(main([...CONN_ARGS, 'dump'])).rejects.toThrow(
      'process.exit called',
    );
    expect(errSpy).toHaveBeenCalledWith(
      expect.stringContaining('--database is required'),
    );
  });

  // ── catalog ───────────────────────────────────────────────────────────────

  describe('catalog', () => {
    it('throws when file arg is missing', async () => {
      await expect(main([...CONN_ARGS, 'catalog'])).rejects.toThrow(
        'catalog requires <file>',
      );
    });

    it('throws when file does not exist', async () => {
      await expect(
        main([...CONN_ARGS, 'catalog', 'no-such-file.json']),
      ).rejects.toThrow('File not found');
    });

    it('imports catalog and reports OK', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([
        ...CONN_ARGS,
        '--database',
        CATALOG_DB,
        '--schema',
        SCHEMA,
        'catalog',
        CATALOG_FILE,
      ]);
      const output = logSpy.mock.calls.map((c) => c[0] as string).join('');
      expect(output).toContain('"status": "OK"');
    });

    it('uses default database name when not provided', async () => {
      // Tests coverage of line 70: dbName = baseConfig.database ?? 'db_' + basename...
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...CONN_ARGS, '--schema', SCHEMA, 'catalog', CATALOG_FILE]);
      const output = logSpy.mock.calls.map((c) => c[0] as string).join('');
      expect(output).toContain('"status": "OK"');
      expect(output).toContain('db_minimal-catalog');
    });

    it('uses default schema name when not provided', async () => {
      // Tests coverage of line 71: schemaName = schema ?? 'main'
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([
        ...CONN_ARGS,
        '--database',
        CATALOG_DB,
        'catalog',
        CATALOG_FILE,
      ]);
      const output = logSpy.mock.calls.map((c) => c[0] as string).join('');
      expect(output).toContain('"status": "OK"');
      expect(output).toContain('"schema": "main"');
    });

    it('handles catalog with no tableCfgs gracefully', async () => {
      // Tests coverage of line 85: optional chaining on catalogData['tableCfgs']?.['_data']
      const noCfgFile = resolve(ROOT, 'data', 'catalog-no-data.json');
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([
        ...CONN_ARGS,
        '--database',
        'TestDbCliNoData',
        '--schema',
        SCHEMA,
        'catalog',
        noCfgFile,
      ]);
      const output = logSpy.mock.calls.map((c) => c[0] as string).join('');
      expect(output).toContain('"status": "OK"');
    });
  });

  // ── dump ─────────────────────────────────────────────────────────────────

  it('dump outputs JSON', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    await main([...DB_ARGS, 'dump']);
    expect(logSpy).toHaveBeenCalled();
  });

  // ── dump-table ────────────────────────────────────────────────────────────

  describe('dump-table', () => {
    it('throws when table arg is missing', async () => {
      await expect(main([...DB_ARGS, 'dump-table'])).rejects.toThrow(
        'dump-table requires <table>',
      );
    });

    it('outputs JSON for a known table', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'dump-table', 'smallTable']);
      expect(logSpy).toHaveBeenCalled();
    });
  });

  // ── table-exists ──────────────────────────────────────────────────────────

  describe('table-exists', () => {
    it('throws when tableKey arg is missing', async () => {
      await expect(main([...DB_ARGS, 'table-exists'])).rejects.toThrow(
        'table-exists requires <tableKey>',
      );
    });

    it('returns true for an existing table', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'table-exists', 'smallTable']);
      expect(logSpy).toHaveBeenCalledWith('true');
    });

    it('returns false for a missing table', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'table-exists', 'noSuchTable']);
      expect(logSpy).toHaveBeenCalledWith('false');
    });
  });

  // ── create-table ──────────────────────────────────────────────────────────

  describe('create-table', () => {
    it('throws when JSON arg is missing', async () => {
      await expect(main([...DB_ARGS, 'create-table'])).rejects.toThrow(
        'create-table requires <tableCfgJson>',
      );
    });

    it('creates a new table and returns OK', async () => {
      const cfg = JSON.stringify({
        key: 'cliCreatedTable',
        columns: [
          { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'H' },
          { key: 'val', type: 'string', titleLong: 'Val', titleShort: 'V' },
        ],
      });
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'create-table', cfg]);
      expect(logSpy).toHaveBeenCalledWith('"OK"');
    });
  });

  // ── raw-table-cfgs ────────────────────────────────────────────────────────

  it('raw-table-cfgs returns JSON', async () => {
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    await main([...DB_ARGS, 'raw-table-cfgs']);
    expect(logSpy).toHaveBeenCalled();
  });

  // ── default port value ────────────────────────────────────────────────────

  it('uses default port when not specified', async () => {
    // Tests coverage of line 134: port: Number(opts.port ?? '1433')
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    // Build args without --port to test the default
    const argsWithoutPort = [
      '--server',
      adminCfg.server!,
      '--user',
      adminCfg.user!,
      '--password',
      adminCfg.password!,
      '--database',
      TEST_DB,
      '--schema',
      SCHEMA,
      'dump',
    ];
    await main(argsWithoutPort);
    expect(logSpy).toHaveBeenCalled();
  });

  // ── write ─────────────────────────────────────────────────────────────────

  describe('write', () => {
    const writeData = JSON.stringify({
      smallTable: {
        _type: 'components',
        _data: [{ _hash: 'l2112PPVjy1p1stUN59jZr', name: 'seedRow' }],
      },
    });

    function withFakeStdin(
      data: string,
      fn: () => Promise<void>,
    ): Promise<void> {
      const original = process.stdin;
      const fake = Readable.from([Buffer.from(data)]);
      Object.defineProperty(process, 'stdin', {
        value: fake,
        configurable: true,
        writable: true,
      });
      return fn().finally(() => {
        Object.defineProperty(process, 'stdin', {
          value: original,
          configurable: true,
          writable: true,
        });
      });
    }

    it('writes from a file path', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'write', WRITE_FILE]);
      expect(logSpy).toHaveBeenCalledWith('"OK"');
    });

    it('writes from stdin when arg is -', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await withFakeStdin(writeData, () => main([...DB_ARGS, 'write', '-']));
      expect(logSpy).toHaveBeenCalledWith('"OK"');
    });

    it('writes from stdin when no arg is given', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await withFakeStdin(writeData, () => main([...DB_ARGS, 'write']));
      expect(logSpy).toHaveBeenCalledWith('"OK"');
    });
  });

  // ── read-rows ─────────────────────────────────────────────────────────────

  describe('read-rows', () => {
    it('throws when whereJson arg is missing', async () => {
      await expect(
        main([...DB_ARGS, 'read-rows', 'smallTable']),
      ).rejects.toThrow('read-rows requires <table> <whereJson>');
    });

    it('returns rows matching the where clause', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'read-rows', 'smallTable', '{}']);
      expect(logSpy).toHaveBeenCalled();
    });
  });

  // ── row-count ─────────────────────────────────────────────────────────────

  describe('row-count', () => {
    it('throws when table arg is missing', async () => {
      await expect(main([...DB_ARGS, 'row-count'])).rejects.toThrow(
        'row-count requires <table>',
      );
    });

    it('returns the row count', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'row-count', 'smallTable']);
      expect(logSpy).toHaveBeenCalled();
    });
  });

  // ── content-type ──────────────────────────────────────────────────────────

  describe('content-type', () => {
    it('throws when table arg is missing', async () => {
      await expect(main([...DB_ARGS, 'content-type'])).rejects.toThrow(
        'content-type requires <table>',
      );
    });

    it('returns the content type', async () => {
      const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
      await main([...DB_ARGS, 'content-type', 'smallTable']);
      expect(logSpy).toHaveBeenCalled();
    });
  });

  // ── unknown command ───────────────────────────────────────────────────────

  it('exits with error for an unknown command', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    await expect(main([...DB_ARGS, 'no-such-command'])).rejects.toThrow(
      'process.exit called',
    );
    expect(errSpy).toHaveBeenCalledWith(
      expect.stringContaining('Unknown command: no-such-command'),
    );
  });

  // ── readStdin ─────────────────────────────────────────────────────────────

  describe('readStdin', () => {
    it('concatenates all chunks from stdin', async () => {
      const original = process.stdin;
      const fake = Readable.from([Buffer.from('hello '), Buffer.from('world')]);
      Object.defineProperty(process, 'stdin', {
        value: fake,
        configurable: true,
        writable: true,
      });
      try {
        const result = await readStdin();
        expect(result).toBe('hello world');
      } finally {
        Object.defineProperty(process, 'stdin', {
          value: original,
          configurable: true,
          writable: true,
        });
      }
    });
  });
});
