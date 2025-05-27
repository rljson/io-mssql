// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io, IoTools } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { Json, JsonValue, JsonValueType } from '@rljson/json';
import {
  ColumnCfg,
  iterateTables,
  Rljson,
  TableCfg,
  TableKey,
  TableType,
} from '@rljson/rljson';

import sql from 'mssql';

import { MsSqlStatements } from './mssql-statements.ts';

export class IoMssql implements Io {
  stm = new MsSqlStatements();
  private _conn: sql.ConnectionPool;
  private _ioTools!: IoTools;
  private _isReady = new IsReady();
  private _currentUserCfg: sql.config;

  constructor(private readonly userCfg: sql.config) {
    this._currentUserCfg = userCfg;
    // Create a new connection pool this.userCfg
    this._conn = new sql.ConnectionPool(this._currentUserCfg!);
    this._conn.on('error', (err) => {
      console.error('SQL Server error:', err);
    });
    // Connection will be established in the async init() method
  }

  async init(): Promise<void> {
    this._conn = new sql.ConnectionPool(this._currentUserCfg);
    this._conn.on('error', (err) => {
      console.error('SQL Server error:', err);
    });
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
        }
      }

      if (errorCount > 0) {
        const errorMessages = Array.from(errorStore.values()).join('\n');
        throw new Error(
          `Failed to write data to MSSQL database. Errors:\n${errorMessages}`,
        );
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
    const result = await sqlReq.query(
      `SELECT COUNT(*) as count FROM [${table}]`,
    );
    return result.recordset[0]?.count ?? 0;
  }

  public example = async () => {
    // Connect to the server
    if (!this._conn.connected) {
      await this._conn.connect();
    }
    const req = new sql.Request(this._conn);
    // Create random names
    const randomString = Math.random().toString(36).substring(2, 22);
    const dbName = `CDM-Test-${randomString}`;
    const schemaName = `Pastry`;
    const loginName = `login_${randomString}`;
    const loginPassword = `P@ssw0rd!${randomString}`;
    // Create database and schema
    try {
      await req.query(this.stm.createDatabase(dbName));
      await req.query(this.stm.useDatabase(dbName));
      await req.query(this.stm.createSchema(schemaName));
    } catch (error) {
      console.error('Error creating database:', error);
      throw error;
    }
    // Create login and user
    try {
      await req.query(this.stm.createLogin(loginName, dbName, loginPassword));
      await req.query(this.stm.useDatabase(dbName));
      await req.query(this.stm.createUser(loginName, loginName, schemaName));
      await req.query(this.stm.addUserToRole('db_datareader', loginName));
      await req.query(this.stm.addUserToRole('db_datawriter', loginName));
      await req.query(this.stm.addUserToRole('db_ddladmin', loginName));
      await req.query(this.stm.grantSchemaPermission(schemaName, loginName));
    } catch (error) {
      console.error('Error creating database:', error);
      throw error;
    }

    const loginUser: sql.config = {
      server: this.userCfg.server,
      database: dbName,
      options: {
        encrypt: true,
        trustServerCertificate: true,
      },
      user: loginName,
      password: loginPassword,
    };

    return new IoMssql(loginUser);
  };

  public get isOpen(): boolean {
    return this._conn.connected;
  }

  // PRIVATE METHODS

  private _initTableCfgs = async () => {
    //create main table if it does not exist yet
    const tableCfg = IoTools.tableCfgsTableCfg;

    try {
      const dbRequest = new sql.Request(this._conn);
      await dbRequest.query(this.stm.createTable(tableCfg));
    } catch (error) {
      console.error('Error creating table:', error);
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
    await sqlReq.query(insertQuery);
  };

  static async dropAllTestDatabases(userCfg: sql.config): Promise<void> {
    const dbConnection = new sql.ConnectionPool(userCfg);
    const msStatements = new MsSqlStatements();
    const sqlRequest = new sql.Request(dbConnection);

    sqlRequest.on('error', (err) => {
      console.error('SQL Server error:', err);
    });
    await dbConnection.connect();
    // Get all databases with names starting with 'CDM-Test-'
    const result = await sqlRequest.query(`
      SELECT name FROM sys.databases WHERE name LIKE 'CDM-Test-%'
    `);
    const dbs: string[] = result.recordset.map((row: any) => row.name);
    for (const dbName of dbs) {
      try {
        // Set database to single user mode to force disconnects
        await sqlRequest.query(
          `ALTER DATABASE [${dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE`,
        );
        await sqlRequest.query(`ALTER DATABASE [${dbName}] SET MULTI_USER`);
        await sqlRequest.query(msStatements.dropDatabase(dbName));
      } catch (err) {
        throw err;
      }
    }
  }

  static async dropAllLogins(userCfg: sql.config): Promise<void> {
    const serverPool = new sql.ConnectionPool(userCfg);
    const dbRequest = new sql.Request(serverPool);
    dbRequest.on('error', (err) => {
      console.error('SQL Server error:', err);
      throw err;
    });
    await serverPool.connect();
    // Get all logins with names starting with 'login_%'
    const result = await dbRequest.query(`
      SELECT name FROM sys.server_principals WHERE name LIKE 'login_%'
    `);
    const logins: string[] = result.recordset.map((row: any) => row.name);
    for (const loginName of logins) {
      try {
        await dbRequest.query(`DROP LOGIN [${loginName}]`);
      } catch (err) {
        console.error(`Failed to delete login ${loginName}:`, err);
      }
    }
  }

  private async _extendTable(newTableCfg: TableCfg): Promise<void> {
    // Estimate added columns
    const tableKey = newTableCfg.key;
    try {
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
    } catch (e) {
      console.error(e);
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
        if (val === undefined) {
          continue;
        }

        if (val === null) {
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
