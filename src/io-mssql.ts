// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io, IoTools } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { Json, JsonValue, JsonValueType } from '@rljson/json';
import { ColumnCfg, iterateTables, Rljson, TableCfg, TableKey, TableType } from '@rljson/rljson';

import { promises as fs } from 'fs';
import sql from 'mssql';
import * as path from 'path';

import { DbBasics } from './db-basics.ts';
import { MsSqlStatements } from './mssql-statements.ts';


export class IoMssql implements Io {
  private _conn: sql.ConnectionPool;
  private _ioTools!: IoTools;
  private _isReady = new IsReady();
  private stm: MsSqlStatements;
  private _schemaName: string = 'PantrySchema';

  constructor(
    private readonly userCfg: sql.config,
    private readonly schemaName?: string,
  ) {
    // Create a new connection pool this.userCfg
    this._conn = new sql.ConnectionPool(this.userCfg!);

    this._conn.on('error', (err) => {
      /* v8 ignore start */
      console.error('SQL Server error:', err);
      /* v8 ignore end */
    });

    if (this.schemaName !== undefined) {
      this._schemaName = this.schemaName;
    }
    this.stm = new MsSqlStatements(this._schemaName);

    // Connection will be established in the async init() method
  }

  async init(): Promise<void> {
    await this._conn.connect();
    this._ioTools = new IoTools(this);
    await this._initTableCfgs();
    // await this._ioTools.initRevisionsTable();
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
        table: this.stm.removeTableSuffix(table),
      });
      returnFile[this.stm.removeTableSuffix(table)] =
        tableDump[this.stm.removeTableSuffix(table)];
    }
    this._addMissingHashes(returnFile);
    return returnFile;
  }

  async dumpTable(request: { table: string }): Promise<Rljson> {
    const dbRequest = new sql.Request(this._conn);
    const tableKey = this.stm.addTableSuffix(request.table);
    await this._ioTools.throwWhenTableDoesNotExist(request.table);

    // get table's column structure
    const tableCfg = await this._ioTools.tableCfg(request.table);
    const columnKeys = tableCfg.columns.map((col) => col.key);
    const columnKeysWithSuffix = columnKeys.map((col) =>
      this.stm.addColumnSuffix(col),
    );

    const returnData = await dbRequest.query(
      this.stm.allData(tableKey, columnKeysWithSuffix.join(', ')),
    );

    const dataAsJson = returnData.recordset as Json[];
    const parsedReturnData = this._parseData(dataAsJson, tableCfg);
    const tableCfgHash = tableCfg._hash as string;
    const table: TableType = {
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
    const tableKeyWithSuffix = this.stm.addTableSuffix(tableKey);
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
    const hashedData = hsh(request.data);
    const errorStore = new Map<number, string>();
    let errorCount = 0;

    await this._ioTools.throwWhenTablesDoNotExist(request.data);
    await this._ioTools.throwWhenTableDataDoesNotMatchCfg(request.data);

    await iterateTables(hashedData, async (tableName, tableData) => {
      const tableCfg = await this._ioTools.tableCfg(tableName);
      const tableKeyWithSuffix = this.stm.addTableSuffix(tableName);

      for (const row of tableData._data) {
        const sqlRequest = new sql.Request(this._conn);
        const columnKeys = tableCfg.columns.map((col) => col.key);
        const columnKeysWithPostfix = columnKeys.map((column) =>
          this.stm.addColumnSuffix(column),
        );
        const placeholders = columnKeys.map((_, i) => `@p${i}`).join(', ');
        const query = `INSERT INTO ${tableKeyWithSuffix} (${columnKeysWithPostfix.join(
          ', ',
        )}) VALUES (${placeholders})`;

        const serializedRow = this._serializeRow(row, tableCfg);
        const stringArray = serializedRow.map((val) =>
          val === null ? null : String(val),
        );

        for (let i = 0; i < stringArray.length; i++) {
          sqlRequest.input(`p${i}`, stringArray[i]);
        }
        try {
          await sqlRequest.query(query);
        } catch (error) {
          if ((error as any).number === 2627) {
            return;
          }
          /* v8 ignore start */
          const errorMessage =
            error instanceof Error ? error.message : 'Unknown error';
          const fixedErrorMessage = errorMessage
            .replace(this.stm.suffix.col, '')
            .replace(this.stm.suffix.tbl, '');

          errorCount++;
          errorStore.set(
            errorCount,
            `Error inserting into table ${tableName}: ${fixedErrorMessage}`,
          );
          /* v8 ignore end */
        }
      }

      if (errorCount > 0) {
        /* v8 ignore start */
        const errorMessages = Array.from(errorStore.values()).join('\n');
        throw new Error(
          `Failed to write data to MSSQL database. Errors:\n${errorMessages}`,
        );
        /* v8 ignore start */
      }
    });
  }

  async readRows(request: {
    table: string;
    where: { [column: string]: JsonValue };
  }): Promise<Rljson> {
    await this._ioTools.throwWhenTableDoesNotExist(request.table);
    await this._ioTools.throwWhenColumnDoesNotExist(request.table, [
      ...Object.keys(request.where),
    ]);
    const tableKey = this.stm.addTableSuffix(request.table);
    const whereClause = this.stm.whereString(Object.entries(request.where));

    const sqlSt = this.stm.selection(tableKey, '*', whereClause);

    const dbRequest = new sql.Request(this._conn);
    const dbResult = await dbRequest.query(sqlSt);

    const tableCfg = await this._ioTools.tableCfg(request.table);
    const dataAsJson = dbResult.recordset as Json[];
    const parsedReturnData = this._parseData(dataAsJson, tableCfg);

    const table = {
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
    table = this.stm.addTableSuffix(table);
    const sqlReq = new sql.Request(this._conn);

    const result = await sqlReq.query(this.stm.rowCount(table));

    // Return the array of counts
    return result.recordset[0].totalCount ?? 0; // Return the second count if available, otherwise the first, or 0 if both are null
  }

  public example = async (dbName: string) => {
    await this._conn.connect();
    // Create random names
    const randomString = Math.random().toString(36).substring(2, 12);
    //const dbName = this.userCfg.database ?? 'Test-DB'; // `CDM-Test-${randomString}`;
    const testSchemaName = `testschema_${randomString}`;
    const loginName = `login_${randomString}`;
    const loginPassword = `P@ssw0rd!${randomString}`;
    // Create database and schema
    console.log(await DbBasics.createDatabase(this.userCfg, dbName));
    console.log(
      await DbBasics.createSchema(this.userCfg, dbName, testSchemaName),
    );

    // Create login and user
    console.log(
      await DbBasics.createLogin(
        this.userCfg,
        dbName,
        loginName,
        loginPassword,
      ),
    );
    console.log(
      await DbBasics.createUser(
        this.userCfg,
        dbName,
        testSchemaName,
        loginName,
        loginName,
      ),
    );

    await DbBasics.grantSchemaPermission(
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

    // const loginUser: sql.config = this.userCfg;

    // Wait until user is actually created

    const waitForUser = async (retries = 10, delay = 1000) => {
      for (let i = 0; i < retries; i++) {
        const testReq = new sql.Request(this._conn);
        const result = await testReq.query(
          `SELECT name AS loginName FROM sys.sql_logins WHERE name = '${loginName}'`,
        );
        if (result.recordset.length > 0) {
          return true;
        }
        /* v8 ignore start */
        await new Promise((resolve) => setTimeout(resolve, delay));
        /* v8 ignore end */
      }
    };
    await waitForUser();
    if (!waitForUser) {
      /* v8 ignore start */
      throw new Error(`Login ${loginName} not found after retries.`);
      /* v8 ignore end */
    }

    return new IoMssql(loginUser, testSchemaName);
  };

  public get isOpen(): boolean {
    return this._conn.connected;
  }

  // structure-related methods

  // Static connection method
  static async makeConnection(userCfg: sql.config): Promise<sql.Request> {
    const serverPool = new sql.ConnectionPool(userCfg);
    serverPool.on('error', (err) => {
      /* v8 ignore start */
      console.error('SQL Server error:', err);

      /* v8 ignore end */
    });
    await serverPool.connect();
    return new sql.Request(serverPool);
  }

  static async installScripts(userCfg: sql.config): Promise<void> {
    const dbRequest = await this.makeConnection(userCfg);

    // Read the install-script.sql file from the same directory as this file
    const scriptPath = path.resolve(__dirname, 'install-script.sql');
    const scriptContent = await fs.readFile(scriptPath, 'utf-8');
    const separator = 'GO --REM'; // 'GO' ist not recognized by mssql, so we use a custom separator'
    // Split the script content by the separator
    const scriptParts = scriptContent.split(separator);
    for (const part of scriptParts) {
      const cleanedPart = part.replace(/[\r]/g, ' ').trim();
      if (cleanedPart) {
        await dbRequest.query(cleanedPart);
      }
    }
  }

  static async dropTestLogins(
    userCfg: sql.config,
    schemaName: string,
  ): Promise<void> {
    const dbRequest = await this.makeConnection(userCfg);
    await DbBasics.dropUsers(userCfg, userCfg.database!, schemaName);
    await dbRequest.query(`EXEC PantrySchema.DropAllPantryLogins`);
  }

  static async dropTestSchemas(userCfg: sql.config): Promise<void> {
    const dbRequest = await this.makeConnection(userCfg);
    await dbRequest.query(`EXEC PantrySchema.DropAllPantryObjects`);
  }

  // Extra methods to manage tests

  static async dropCurrentConstraints(
    userCfg: sql.config,
    schemaName: string,
  ): Promise<void> {
    const dbRequest = await this.makeConnection(userCfg);
    await dbRequest.query(
      `EXEC PantrySchema.DropCurrentConstraints [${schemaName}]`,
    );
  }
  static async DropSchema(
    userCfg: sql.config,
    schemaName: string,
  ): Promise<void> {
    const dbRequest = await this.makeConnection(userCfg);
    await dbRequest.query(`EXEC PantrySchema.DropSchema [${schemaName}]`);
  }
  static async dropCurrentLogin(
    userCfg: sql.config,
    loginName: string,
  ): Promise<void> {
    const dbRequest = await this.makeConnection(userCfg);
    await dbRequest.query(`EXEC PantrySchema.DropCurrentLogin [${loginName}]`);
  }

  public get currentSchema(): string {
    return this._schemaName;
  }

  public get currentLogin(): string {
    return this.userCfg.user || 'unknown';
  }

  // PRIVATE METHODS

  private _initTableCfgs = async () => {
    //create main table if it does not exist yet
    const tableCfg = IoTools.tableCfgsTableCfg;

    try {
      const dbRequest = new sql.Request(this._conn);
      await dbRequest.query(this.stm.createTable(tableCfg));
    } catch (error) {
      /* v8 ignore start */
      console.error('Error creating table:', error);
      /* v8 ignore end */
    }

    // Write tableCfg as first row into tableCfgs tables
    // As this is the first row to be entered, it is entered manually
    const values = this.stm.serializeRow(tableCfg, tableCfg);

    const insertQuery = this.stm.insertTableCfg();
    const sqlReq = new sql.Request(this._conn);
    // Add each value as an input parameter
    values.forEach((val, idx) => {
      sqlReq.input(`p${idx}`, val);
    });
    try {
      await sqlReq.query(insertQuery);
    } catch (error) {
      if ((error as any).number === 2627) {
        // Duplicate entry, tableCfgs already exists
        return;
      }
      /* v8 ignore start */
      console.error('Error inserting table configuration:', error);
      /* v8 ignore end */
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
        const keyWithSuffix = this.stm.addColumnSuffix(key);
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
}
