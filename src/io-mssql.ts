// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io, IoDbNameMapping, IoTools } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { Json, JsonValue, JsonValueType } from '@rljson/json';
import {
  ColumnCfg,
  ComponentsTable,
  ContentType,
  iterateTables,
  Rljson,
  TableCfg,
  TableKey,
  TableType,
} from '@rljson/rljson';

import sql from 'mssql';

import { DbBasics } from '../src/db-basics.ts';
import { DbStatements } from '../src/db-statements.ts';

export class IoMssql implements Io {
  private _conn: sql.ConnectionPool;
  private _ioTools!: IoTools;
  private _isReady = new IsReady();
  private _schemaName: string = 'PantrySchema';
  private stm = new DbStatements(this._schemaName);
  private _dbBasics = new DbBasics();
  private _map = new IoDbNameMapping();

  constructor(
    private readonly userCfg: sql.config,
    private readonly schemaName?: string,
  ) {
    // Create a new connection pool this.userCfg
    this._conn = new sql.ConnectionPool(this.userCfg!);

    /* v8 ignore next -- @preserve */
    this._conn.on('error', (err) => {
      console.error('SQL Server error:', err);
    });
    /* v8 ignore next -- @preserve */
    if (this.schemaName !== undefined) {
      this._schemaName = this.schemaName;
    }
    this.stm = new DbStatements(this._schemaName);
    this._dbBasics = new DbBasics();

    // Connection will be established in the async init() method
  }
  async contentType(request: { table: string }): Promise<ContentType> {
    const result = await this._dbBasics.contentType(
      this.userCfg,
      this.userCfg.database!,
      this._schemaName,
      request.table,
    );

    return result as ContentType;
  }

  async init(): Promise<void> {
    await this._conn.connect();
    this._ioTools = new IoTools(this);
    await this._initTableCfgs();
    await this._ioTools.initRevisionsTable();
    this._isReady.resolve();
  }

  async close(): Promise<void> {
    if (this._conn.connected) {
      await this._conn.close();
    }
  }

  async isReady(): Promise<void> {
    if (!this._conn.connected) {
      throw new Error('MSSQL connection is not open.');
    }
  }

  async dump(): Promise<Rljson> {
    // Dumps all tables and their contents
    const dbRequest = new sql.Request(this._conn);
    const returnData = await dbRequest.query(this.stm.tableKeys);
    const tables = returnData.recordset.map((row: any) => row.tableKey);
    const returnFile: Rljson = {};
    for (const table of tables) {
      const tableDump: Rljson = await this.dumpTable({
        table: this._map.removeTableSuffix(table),
      });
      returnFile[this._map.removeTableSuffix(table)] =
        tableDump[this._map.removeTableSuffix(table)];
    }
    this._addMissingHashes(returnFile);
    return returnFile;
  }

  async dumpTable(request: { table: string }): Promise<Rljson> {
    const dbRequest = new sql.Request(this._conn);
    const tableKey = this._map.addTableSuffix(request.table);
    await this._ioTools.throwWhenTableDoesNotExist(request.table);

    // get table's column structure
    const tableCfg = await this._ioTools.tableCfg(request.table);
    const columnKeys = tableCfg.columns.map((col) => col.key);
    const columnKeysWithSuffix = columnKeys.map((col) =>
      this._map.addColumnSuffix(col),
    );

    const returnData = await dbRequest.query(
      this.stm.allData(tableKey, columnKeysWithSuffix.join(', ')),
    );

    const dataAsJson = returnData.recordset as Json[];
    const parsedReturnData = this._parseData(dataAsJson, tableCfg);
    const tableCfgHash = tableCfg._hash as string;
    const table: TableType = {
      _type: tableCfg.type,
      _data: parsedReturnData as any,
      _tableCfg: tableCfgHash,
      _hash: '',
    };

    this._ioTools.sortTableDataAndUpdateHash(table);
    const returnFile: Rljson = {};
    returnFile[request.table] = table;
    return returnFile;
  }

  async tableExists(tableKey: TableKey): Promise<boolean> {
    const dbRequest = new sql.Request(this._conn);
    const tableKeyWithSuffix = this._map.addTableSuffix(tableKey);
    dbRequest.input('tableName', sql.NVarChar, tableKeyWithSuffix);
    const returnData = await dbRequest.query(this.stm.tableExists);
    return returnData.recordset[0]['tableExists'];
  }

  async createOrExtendTable(request: { tableCfg: TableCfg }): Promise<void> {
    // Make sure that the table config is compatible
    await this._ioTools.throwWhenTableIsNotCompatible(request.tableCfg);

    const tableKey = request.tableCfg.key;
    const tableCfgHashed = hsh(request.tableCfg);

    if (await this.tableExists(tableKey)) {
      await this._extendTable(tableCfgHashed);
    } else {
      await this._createTable(tableCfgHashed, request);
    }
  }

  async rawTableCfgs(): Promise<TableCfg[]> {
    const dbRequest = new sql.Request(this._conn);
    const tableCfg = IoTools.tableCfgsTableCfg;
    const returnData = await dbRequest.query(this.stm.tableCfgs);
    const dataAsJson = returnData.recordset as Json[];
    const parsedReturnData = this._parseData(dataAsJson, tableCfg);
    return parsedReturnData as TableCfg[];
  }

  async write(request: { data: Rljson }): Promise<void> {
    await this._ioTools.throwWhenTablesDoNotExist(request.data);
    // Disabled for now — see _coerceDataToTableCfgTypes/_coerceValue.
    // Referenced (not called) below purely to keep noUnusedLocals happy.
    // await this._coerceDataToTableCfgTypes(request.data);
    void this._coerceDataToTableCfgTypes;

    const hashedData = hsh(request.data);
    try {
      await this._ioTools.throwWhenTableDataDoesNotMatchCfg(request.data);
    } catch (error) {
      throw new Error(
        this._dedupeTableCfgMismatchErrors((error as Error).message),
      );
    }

    await iterateTables(hashedData, async (tableName, tableData) => {
      const tableCfg = await this._ioTools.tableCfg(tableName);
      const tableKeyWithSuffix = this._map.addTableSuffix(tableName);
      const columnKeys = this._prepareColumnNames(tableCfg.columns);
      const mainQuery = `INSERT INTO ${this._schemaName}.${tableKeyWithSuffix} (${columnKeys}) VALUES `;
      const columnCount = tableCfg.columns.length;

      // SQL Server rejects requests with more than 2100 parameters. Batch
      // rows so that each INSERT stays comfortably under that limit.
      const maxParamsPerBatch = 2000;
      const rowsPerBatch = Math.max(
        1,
        Math.floor(maxParamsPerBatch / columnCount),
      );

      const errorStore = new Map<number, string>();
      let errorCount = 0;
      const rows = tableData._data;

      for (
        let batchStart = 0;
        batchStart < rows.length;
        batchStart += rowsPerBatch
      ) {
        const batchRows = rows.slice(batchStart, batchStart + rowsPerBatch);
        const sqlRequest = new sql.Request(this._conn);
        const placeHolderLine: string[] = [];
        const placeHolderLines: string[] = [];
        let position = 0;
        for (const row of batchRows) {
          const serializedRow = this._serializeRow(row, tableCfg);
          const stringArray = serializedRow.map((val) =>
            val === null ? null : String(val),
          );

          for (let i = position; i < position + stringArray.length; i++) {
            sqlRequest.input(`p${i}`, stringArray[i - position]);
            placeHolderLine.push(`@p${i}`);
          }
          placeHolderLines.push(`(${placeHolderLine.join(', ')})`);
          placeHolderLine.length = 0;

          position += columnCount;
        }
        try {
          await sqlRequest.query(mainQuery + placeHolderLines.join(', '));
        } catch (error) {
          /* v8 ignore next -- @preserve */
          if ((error as any).number === 2627) {
            return;
          }
          /* v8 ignore next -- @preserve */
          const errorMessage =
            error instanceof Error ? error.message : 'Unknown error';
          /* v8 ignore next -- @preserve */

          errorCount++;
          errorStore.set(
            errorCount,
            `Error inserting into table ${tableName}: ${errorMessage}`,
          );
          /* v8 ignore end */
        }
      }

      /* v8 ignore next -- @preserve */
      if (errorCount > 0) {
        /* v8 ignore next -- @preserve */
        const errorMessages = Array.from(errorStore.values()).join('\n');
        /* v8 ignore next -- @preserve */
        throw new Error(
          `Failed to write data to MSSQL database. Errors:\n${errorMessages}`,
        );
      }
    });
  }

  // @rljson/rljson reports one "in row N" error per offending row, so a
  // column with the wrong type across an entire table produces one line per
  // row — often thousands of near-identical messages. Collapse those down to
  // one line per distinct (column, actual type, expected type) combination
  // so the thrown error stays readable.
  private _dedupeTableCfgMismatchErrors(message: string): string {
    const lines = message.split('\n');
    const firstErrorIndex = lines.findIndex((line) => line.startsWith('- '));

    const header = lines.slice(0, firstErrorIndex);
    const seen = new Set<string>();
    const dedupedErrors: string[] = [];
    for (const line of lines.slice(firstErrorIndex)) {
      const key = line.replace(/ in row \d+ /, ' in row N ');
      if (seen.has(key)) continue;
      seen.add(key);
      dedupedErrors.push(line);
    }

    return [...header, ...dedupedErrors].join('\n');
  }

  // Casts row values to the type declared for their column, so that e.g. a
  // number arriving for a string column doesn't fail validation. Values that
  // cannot be cast to the expected type (e.g. "abc" for a number column) are
  // set to null rather than rejected.
  //
  // A row's incoming _hash was computed over its original, un-coerced
  // values, so it necessarily goes stale the moment we change one of those
  // values. Dropping it here lets hsh() (called right after this) compute a
  // fresh hash instead of rejecting the row for "not matching" a hash that
  // described different content. Rows that end up unchanged keep their
  // original hash, so genuinely corrupted input is still caught.
  private async _coerceDataToTableCfgTypes(data: Rljson): Promise<void> {
    await iterateTables(data, async (tableName, tableData) => {
      const tableCfg = await this._ioTools.tableCfg(tableName);
      for (const row of tableData._data) {
        let mutated = false;
        for (const column of tableCfg.columns) {
          const value = (row as Json)[column.key];
          if (value === undefined || value === null) continue;
          const coerced = this._coerceValue(value, column.type);
          if (coerced !== value) {
            (row as Json)[column.key] = coerced;
            mutated = true;
          }
        }
        if (mutated) {
          delete (row as Json)['_hash'];
        }
      }
    });
  }

  private _coerceValue(
    value: JsonValue,
    expectedType: JsonValueType,
  ): JsonValue | null {
    switch (expectedType) {
      case 'string':
        return typeof value === 'string' ? value : String(value);

      case 'number': {
        if (typeof value === 'number') return value;
        const parsed = Number(value);
        return Number.isNaN(parsed) ? null : parsed;
      }

      case 'boolean': {
        if (typeof value === 'boolean') return value;
        if (typeof value === 'string') {
          const lowered = value.trim().toLowerCase();
          if (lowered === 'true') return true;
          if (lowered === 'false') return false;
          return null;
        }
        return Boolean(value);
      }

      default:
        return value;
    }
  }

  async readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    await this._ioTools.throwWhenTableDoesNotExist(request.table);
    await this._ioTools.throwWhenColumnDoesNotExist(request.table, [
      ...Object.keys(request.where),
    ]);
    const tableKey = this._map.addTableSuffix(request.table);
    const whereClause = this.stm.whereString(Object.entries(request.where));

    const sqlSt = this.stm.selection(tableKey, '*', whereClause);

    const dbRequest = new sql.Request(this._conn);
    const dbResult = await dbRequest.query(sqlSt);

    const tableCfg = await this._ioTools.tableCfg(request.table);
    const dataAsJson = dbResult.recordset as Json[];
    const parsedReturnData = this._parseData(dataAsJson, tableCfg);

    const table: ComponentsTable<any> = {
      _type: 'components',
      _data: parsedReturnData,
    };

    this._ioTools.sortTableDataAndUpdateHash(table);

    const result: Rljson = {
      [request.table]: table,
    } as any;

    return result;
  }

  async rowCount(table: string): Promise<number> {
    await this._ioTools.throwWhenTableDoesNotExist(table);
    table = this._map.addTableSuffix(table);
    const sqlReq = new sql.Request(this._conn);

    const result = await sqlReq.query(this.stm.rowCount(table));

    // Return the array of counts
    /* v8 ignore next -- @preserve */
    return result.recordset[0].totalCount ?? 0; // Return the second count if available, otherwise the first, or 0 if both are null
  }

  public example = async (dbName: string) => {
    await this._conn.connect();
    // Create random names
    const randomString = Math.random().toString(36).substring(2, 12);
    const testSchemaName = `testschema_${randomString}`;
    const loginName = `login_${randomString}`;
    const loginPassword = `P@ssw0rd!${randomString}`;
    // Create database and schema
    await this._dbBasics.createDatabase(this.userCfg, dbName);
    await this._dbBasics.createSchema(this.userCfg, dbName, testSchemaName);

    // Create login and user
    await this._dbBasics.createLogin(
      this.userCfg,
      dbName,
      loginName,
      loginPassword,
    );
    await this._dbBasics.createUser(
      this.userCfg,
      dbName,
      testSchemaName,
      loginName,
      loginName,
    );

    await this._dbBasics.grantSchemaPermission(
      this.userCfg,
      dbName,
      testSchemaName,
      loginName,
    );

    const loginUser: sql.config = {
      server: this.userCfg.server,
      database: dbName,
      options: {
        encrypt: false,
        trustServerCertificate: true,
      },
      user: loginName,
      password: loginPassword,
      port: 1431,
    };

    // Wait until user is actually created
    const waitForUser = async (retries = 10, delay = 1000) => {
      for (let i = 0; i < retries; i++) {
        const testReq = new sql.Request(this._conn);
        const result = await testReq.query(
          `SELECT name AS loginName FROM sys.sql_logins WHERE name = '${loginName}'`,
        );
        /* v8 ignore next -- @preserve */
        if (result.recordset.length > 0) {
          return true;
        }
        /* v8 ignore next -- @preserve */
        await new Promise((resolve) => setTimeout(resolve, delay));
        /* v8 ignore end */
      }
    };
    await waitForUser();
    /* v8 ignore next -- @preserve */
    if (!waitForUser) {
      throw new Error(`Login ${loginName} not found after retries.`);
    }

    return new IoMssql(loginUser, testSchemaName);
  };

  public get isOpen(): boolean {
    return this._conn.connected;
  }

  // structure-related methods
  public get currentSchema(): string {
    return this._schemaName;
  }
  public get currentLogin(): string {
    /* v8 ignore next -- @preserve */
    return this.userCfg.user || 'unknown';
  }

  // PRIVATE METHODS
  private _initTableCfgs = async () => {
    //create main table if it does not exist yet
    const tableCfg = IoTools.tableCfgsTableCfg;
    const dbRequest = new sql.Request(this._conn);
    await dbRequest.query(this.stm.createTable(tableCfg));

    // Write tableCfg as first row into tableCfgs tables
    // As this is the first row to be entered, it is entered manually
    const values = this.stm.serializeRow(tableCfg, tableCfg);
    const insertQuery = this.stm.insertTableCfg();
    const sqlReq = new sql.Request(this._conn);
    // Add each value as an input parameter
    values.forEach((val, idx) => {
      sqlReq.input(`p${idx}`, val);
    });

    /* v8 ignore next -- @preserve */
    try {
      await sqlReq.query(insertQuery);
    } catch (error) {
      if ((error as any).number === 2627) {
        // Duplicate entry, tableCfgs already exists
        return;
      }
      throw error;
    }
  };

  private async _extendTable(newTableCfg: TableCfg): Promise<void> {
    // Estimate added columns
    const tableKey = newTableCfg.key;
    const dbRequest = new sql.Request(this._conn);
    const oldTableCfg = await this._ioTools.tableCfg(tableKey);

    const addedColumns: ColumnCfg[] = [];
    for (
      let i = oldTableCfg.columns.length;
      i < newTableCfg.columns.length;
      i++
    ) {
      const newColumn = newTableCfg.columns[i];
      addedColumns.push(newColumn);
    }

    // No columns added? Do nothing.
    if (addedColumns.length === 0) {
      return;
    }

    // Write new tableCfg into tableCfgs table
    this._insertTableCfg(newTableCfg);

    // Add new columns to the table
    const alter = this.stm.alterTable(tableKey, addedColumns);
    for (const statement of alter) {
      await dbRequest.query(statement);
    }
  }

  private async _insertTableCfg(tableCfgHashed: TableCfg) {
    const req = new sql.Request(this._conn);
    hip(tableCfgHashed);
    const values = this.stm.serializeRow(
      tableCfgHashed,
      IoTools.tableCfgsTableCfg,
    );
    // Add each value as an input parameter
    values.forEach((val, idx) => {
      req.input(`p${idx}`, val);
    });
    await req.query(this.stm.insertTableCfg());
  }

  private async _createTable(
    tableCfgHashed: TableCfg,
    request: { tableCfg: TableCfg },
  ) {
    const req = new sql.Request(this._conn);
    this._insertTableCfg(tableCfgHashed);
    await req.query(this.stm.createTable(request.tableCfg));
  }

  // ...........................................................................
  _addMissingHashes(rljson: Json): void {
    hip(rljson, { updateExistingHashes: false, throwOnWrongHashes: false });
  }

  private _parseData(data: Json[], tableCfg: TableCfg): Json[] {
    const columnTypes = tableCfg.columns.map((col) => col.type);
    const columnKeys = tableCfg.columns.map((col) => col.key);

    const convertedResult: Json[] = [];

    for (const row of data) {
      const convertedRow: { [key: string]: any } = {};
      for (let colNum = 0; colNum < columnKeys.length; colNum++) {
        const key = columnKeys[colNum];
        const keyWithSuffix = this._map.addColumnSuffix(key);
        const type = columnTypes[colNum] as JsonValueType;
        const val = row[keyWithSuffix];

        // Null or undefined values are ignored
        // and not added to the converted row
        if (val === undefined || val === null) {
          continue;
        }

        switch (type) {
          case 'boolean':
            convertedRow[key] = val; // !== 0;
            break;
          case 'jsonArray':
            convertedRow[key] = JSON.parse(val as string);
            break;
          case 'json':
            convertedRow[key] = JSON.parse(val as string);
            break;
          case 'string':
          case 'number':
            convertedRow[key] = val;
            break;
          /* v8 ignore next -- @preserve */
          default:
            throw new Error('Unsupported column type ' + type);
          /* v8 ignore end */
        }
      }

      convertedResult.push(convertedRow);
    }

    return convertedResult;
  }

  private _serializeRow(
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

  private _prepareColumnNames(columnKeys: ColumnCfg[]): string {
    return columnKeys
      .map((col) => this._map.addColumnSuffix(col.key))
      .join(', ');
  }
}
