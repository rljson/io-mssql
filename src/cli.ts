// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { TableCfg } from '@rljson/rljson';

import sql from 'mssql';
import { createReadStream, existsSync } from 'node:fs';
import { basename, resolve } from 'node:path';
import type { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { inspect, parseArgs } from 'node:util';
import streamValues from 'stream-json/streamers/stream-values.js';

import { DbBasics } from './db-basics.ts';
import { IoMssql } from './io-mssql.ts';

const HELP = `
io-mssql — CLI for SQL Server via Rljson IoMssql

Usage: io-mssql [connection options] <command> [args...]

Connection options (also readable from env vars):
  --server <host>         SQL Server hostname / IP      [MSSQL_SERVER]
  --database <name>       Database name                 [MSSQL_DATABASE]
  --user <user>           Login username                [MSSQL_USER]
  --password <pass>       Login password                [MSSQL_PASSWORD]
  --port <port>           Server port (default 1433)    [MSSQL_PORT]
  --schema <name>         Schema name                   [MSSQL_SCHEMA]
  --encrypt               Enable TLS encryption         (default: false)
  -h, --help              Show this help and exit

Commands:
  catalog <file>                    Import a catalog JSON file into SQL Server.
                                    Creates the database automatically
                                    (named db_<filename> unless --database is set).
                                    Schema defaults to "main" unless --schema is set.
  dump                              Dump all tables as Rljson JSON
  dump-table <table>                Dump a single table as Rljson JSON
  table-exists <tableKey>           Print true/false whether table exists
  create-table <tableCfgJson>       Create or extend a table (JSON arg)
  raw-table-cfgs                    List raw table config records
  write <file | ->                  Write Rljson data from file path or stdin (-)
  read-rows <table> <whereJson>     Read rows matching a where clause (JSON)
  row-count <table>                 Print row count for a table
  content-type <table>              Print content type of a table

Output is always JSON on stdout.  Errors go to stderr; exit code 1 on error.
`.trim();

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString('utf-8');
}

// Parses JSON from a stream incrementally instead of first materializing the
// whole input as one JS string. Node/V8 caps string length at ~512 MiB
// (0x1fffffe8 chars) — reading a large catalog/data file via
// readFileSync(..., 'utf-8') + JSON.parse hits that ceiling long before disk
// space or RAM would, even though the resulting parsed object would fit
// comfortably in memory.
async function readJsonStream<T = unknown>(stream: Readable): Promise<T> {
  const parseStream = streamValues.withParserAsStream();
  return new Promise<T>((resolvePromise, reject) => {
    let parsed: T | undefined;
    parseStream.on('data', (data: { value: T }) => {
      parsed = data.value;
    });
    parseStream.on('end', () => {
      if (parsed === undefined) {
        reject(new Error('Empty or invalid JSON input'));
        return;
      }
      resolvePromise(parsed);
    });
    parseStream.on('error', reject);
    stream.on('error', reject);
    stream.pipe(parseStream);
  });
}

function readJsonFile<T = unknown>(filePath: string): Promise<T> {
  return readJsonStream<T>(createReadStream(filePath));
}

async function runCatalog(
  baseConfig: sql.config,
  schema: string | undefined,
  args: string[],
): Promise<void> {
  if (!args[0]) throw new Error('catalog requires <file>');

  const filePath = resolve(process.cwd(), args[0]);
  if (!existsSync(filePath)) throw new Error(`File not found: ${filePath}`);

  const catalogData = await readJsonFile<Record<string, any>>(filePath);
  const tableCfgs: TableCfg[] = catalogData['tableCfgs']?.['_data'] ?? [];
  if (tableCfgs.length === 0) {
    throw new Error(
      `Invalid catalog file: ${filePath} has no tableCfgs. ` +
        `Expected a top-level "tableCfgs" object with a non-empty "_data" array ` +
        `(rljson format) — the file may need to be converted first.`,
    );
  }

  const dbName =
    baseConfig.database ??
    'db_' + basename(args[0]).replace(/\.(rljson|json)$/i, '');
  const schemaName = schema ?? 'main';

  // Create database + schema via a master connection
  const masterCfg: sql.config = { ...baseConfig, database: 'master' };
  const dbBasics = new DbBasics();
  await dbBasics.createDatabase(masterCfg, dbName);
  await dbBasics.createSchema(masterCfg, dbName, schemaName);

  // Connect to the newly created database
  const dbCfg: sql.config = { ...baseConfig, database: dbName };
  const io = new IoMssql(dbCfg, schemaName);
  try {
    await io.init();

    // Create / extend all tables
    for (const tableCfg of tableCfgs) {
      await io.createOrExtendTable({ tableCfg });
    }

    // Insert data for each table
    for (const tableCfg of tableCfgs) {
      const tableKey = tableCfg.key;
      const tableData = catalogData[tableKey]?.['_data'] ?? [];
      if (tableData.length === 0) continue;
      await io.write({
        data: { [tableKey]: { _type: 'components', _data: tableData } },
      });
    }
  } finally {
    await io.close();
  }

  console.log(
    JSON.stringify(
      { database: dbName, schema: schemaName, status: 'OK' },
      null,
      2,
    ),
  );
}

async function main(argv: string[] = process.argv.slice(2)): Promise<void> {
  const { values: opts, positionals } = parseArgs({
    args: argv,
    options: {
      server: {
        type: 'string',
        default: process.env['MSSQL_SERVER'] ?? 'localhost',
      },
      database: { type: 'string', default: process.env['MSSQL_DATABASE'] },
      user: { type: 'string', default: process.env['MSSQL_USER'] ?? 'sa' },
      password: {
        type: 'string',
        default: process.env['MSSQL_PASSWORD'] ?? 'Password123!',
      },
      port: { type: 'string', default: process.env['MSSQL_PORT'] ?? '1431' },
      schema: { type: 'string', default: process.env['MSSQL_SCHEMA'] },
      encrypt: { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false },
    },
    allowPositionals: true,
  });

  if (opts.help || positionals.length === 0) {
    console.log(HELP);
    return;
  }

  const baseConfig: sql.config = {
    server: opts.server!,
    database: opts.database,
    user: opts.user,
    password: opts.password,
    /* v8 ignore next -- parseArgs always sets port default */
    port: Number(opts.port ?? '1433'),
    options: {
      encrypt: opts.encrypt,
      trustServerCertificate: true,
    },
  };

  const [command, ...args] = positionals;

  // ── catalog has its own flow: creates the DB itself ──────────────────────
  if (command === 'catalog') {
    await runCatalog(baseConfig, opts.schema, args);
    return;
  }

  // ── all other commands need an existing database ──────────────────────────
  if (!opts.database) {
    console.error('Error: --database is required (or set MSSQL_DATABASE)');
    process.exit(1);
  }

  const io = new IoMssql(baseConfig, opts.schema);
  try {
    await io.init();

    let result: unknown;

    switch (command) {
      case 'dump':
        result = await io.dump();
        break;

      case 'dump-table':
        if (!args[0]) throw new Error('dump-table requires <table>');
        result = await io.dumpTable({ table: args[0] });
        break;

      case 'table-exists':
        if (!args[0]) throw new Error('table-exists requires <tableKey>');
        result = await io.tableExists(args[0]);
        break;

      case 'create-table': {
        if (!args[0]) throw new Error('create-table requires <tableCfgJson>');
        await io.createOrExtendTable({ tableCfg: JSON.parse(args[0]) });
        result = 'OK';
        break;
      }

      case 'raw-table-cfgs':
        result = await io.rawTableCfgs();
        break;

      case 'write': {
        const writeData =
          !args[0] || args[0] === '-'
            ? await readJsonStream(process.stdin)
            : await readJsonFile(args[0]);
        await io.write({ data: writeData as any });
        result = 'OK';
        break;
      }

      case 'read-rows': {
        if (!args[0] || !args[1]) {
          throw new Error('read-rows requires <table> <whereJson>');
        }
        result = await io.readRows({
          table: args[0],
          where: JSON.parse(args[1]),
        });
        break;
      }

      case 'row-count':
        if (!args[0]) throw new Error('row-count requires <table>');
        result = await io.rowCount(args[0]);
        break;

      case 'content-type':
        if (!args[0]) throw new Error('content-type requires <table>');
        result = await io.contentType({ table: args[0] });
        break;

      default:
        console.error(`Unknown command: ${command}\n\n${HELP}`);
        process.exit(1);
    }

    console.log(JSON.stringify(result, null, 2));
  } finally {
    await io.close();
  }
}

export { main, readStdin };

/* v8 ignore next 6 */
let isMain = true;
try {
  isMain = process.argv[1] === fileURLToPath(import.meta.url);
} catch {
  // import.meta.url is unavailable inside a Node.js SEA snapshot (the .exe
  // build) — in that context this module is always the entry point.
}
/* v8 ignore next 8 */
if (isMain) {
  main().catch((e) => {
    console.error(
      'Error:',
      e instanceof Error ? e.message : inspect(e, { depth: 5 }),
    );
    process.exit(1);
  });
}
