// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { Io, IoTools } from '@rljson/io';
import { IsReady } from '@rljson/is-ready';
import { JsonValue } from '@rljson/json';
import { ColumnCfg, Rljson, TableCfg, TableKey } from '@rljson/rljson';

import sql from 'mssql';

import { MsSqlStatements } from './mssql-statements.ts';

export class IoMssql implements Io {
  stat = new MsSqlStatements();
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
    const req = new sql.Request(this._conn);
    const tablesResult = await req.query(`
      SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    `);
    const tables = tablesResult.recordset.map((row: any) => row.TABLE_NAME);
    const result: Rljson = {};
    for (const table of tables) {
      const tablereq = new sql.Request(this._conn);
      const data = await tablereq.query(`SELECT * FROM [${table}]`);
      result[table] = { _data: data.recordset };
    }
    return result;
  }

  async dumpTable(request: { table: string }): Promise<Rljson> {
    const tablereq = new sql.Request(this._conn);
    const data = await tablereq.query(`SELECT * FROM [${request.table}]`);
    return { [request.table]: { _data: data.recordset } };
  }

  async tableExists(tableKey: TableKey): Promise<boolean> {
    const req = new sql.Request(this._conn);
    req.input('tableName', sql.NVarChar, tableKey);
    const result = await req.query(this.stat.tableExists);
    return result.recordset[0]['tableExists'];
  }

  async createOrExtendTable(request: { tableCfg: TableCfg }): Promise<void> {
    // Make sure that the table config is compatible
    await this._ioTools.throwWhenTableIsNotCompatible(request.tableCfg);

    // Create table in sqlite database
    const tableKey = request.tableCfg.key;

    // Create config hash
    const tableCfgHashed = hsh(request.tableCfg);

    if (await this.tableExists(tableKey)) {
      await this._extendTable(tableCfgHashed);
      console.log('to be extended');
    } else {
      await this._createTable(tableCfgHashed, request);

      console.log('to be created');
    }
  }

  async rawTableCfgs(): Promise<TableCfg[]> {
    const tableCfg = IoTools.tableCfgsTableCfg;
    console.log(tableCfg);
    // const returnValue =  await req.query(this.stat.tableCfgs).all() as Json[];
    // const parsedReturnValue = this.stat.parseData(returnValue, tableCfg);
    const tst: TableCfg[] = [];
    return tst;
  }

  async write(request: { data: Rljson }): Promise<void> {
    for (const [table, rows] of Object.entries(request.data)) {
      if (!Array.isArray(rows)) continue;
      for (const row of rows) {
        const columns = Object.keys(row)
          .map((col) => `[${col}]`)
          .join(', ');
        const values = Object.values(row)
          .map((val) => (val === null ? 'NULL' : `'${val}'`))
          .join(', ');
        const query = `INSERT INTO [${table}] (${columns}) VALUES (${values})`;
        const sqlRequest = new sql.Request(this._conn);
        await sqlRequest.query(query);
      }
    }
  }

  async readRows(request: {
    table: string;
    where: { [column: string]: JsonValue | null };
  }): Promise<Rljson> {
    const whereClauses = Object.entries(request.where)
      .map(([col, val]) =>
        val === null ? `[${col}] IS NULL` : `[${col}] = '${val}'`,
      )
      .join(' AND ');
    const query = `SELECT * FROM [${request.table}]${
      whereClauses ? ' WHERE ' + whereClauses : ''
    }`;
    const sqlReq = new sql.Request(this._conn);
    const result = await sqlReq.query(query);
    return { [request.table]: { _data: result.recordset } };
  }

  async rowCount(table: string): Promise<number> {
    const sqlReq = new sql.Request(this._conn);
    const result = await sqlReq.query(
      `SELECT COUNT(*) as count FROM [${table}]`,
    );
    return result.recordset[0]?.count ?? 0;
  }

  public async DoSomething() {
    try {
      console.log(this.stat.addColumnSuffix('test'));
      const req = new sql.Request(this._conn);
      const result = await req.query('SELECT * FROM dbo.FirstTable');
      console.table(result.recordset);
    } catch (error) {
      console.error('Error executing query:', error);
    }
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
      await req.query(this.stat.createDatabase(dbName));
      await req.query(this.stat.useDatabase(dbName));
      await req.query(this.stat.createSchema(schemaName));
    } catch (error) {
      console.error('Error creating database:', error);
      throw error;
    }
    // Create login and user
    try {
      await req.query(this.stat.createLogin(loginName, dbName, loginPassword));
      await req.query(this.stat.useDatabase(dbName));
      await req.query(this.stat.createUser(loginName, loginName, schemaName));
      await req.query(this.stat.addUserToRole('db_datareader', loginName));
      await req.query(this.stat.addUserToRole('db_datawriter', loginName));
      await req.query(this.stat.addUserToRole('db_ddladmin', loginName));
      await req.query(this.stat.grantSchemaPermission(schemaName, loginName));
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
    const tableCfg = IoTools.tableCfgsTableCfg;

    //create main table if it does not exist yet
    try {
      const req = new sql.Request(this._conn);
      const result = await req.query(this.stat.createTable(tableCfg));
      if (result.rowsAffected[0] > 0) {
        console.log(`Table ${tableCfg.key} created.`);
      } else {
        console.log(`Table ${tableCfg.key} already exists.`);
      }
    } catch (error) {
      console.error('Error creating table:', error);
    }

    // Write tableCfg as first row into tableCfgs tables
    // As this is the first row to be entered, it is entered manually
    const values = this.stat.serializeRow(tableCfg, tableCfg);

    const insertQuery = this.stat.insertTableCfg();
    const sqlReq = new sql.Request(this._conn);
    // Add each value as an input parameter
    values.forEach((val, idx) => {
      sqlReq.input(`p${idx}`, val);
    });
    await sqlReq.query(insertQuery);
  };

  static async deleteAllTestDatabases(userCfg: sql.config): Promise<void> {
    const serverPool = new sql.ConnectionPool(userCfg);
    const s1 = new MsSqlStatements();

    const req = new sql.Request(serverPool);

    req.on('error', (err) => {
      console.error('SQL Server error:', err);
    });
    await serverPool.connect();
    // Get all databases with names starting with 'CDM-Test-'
    const result = await req.query(`
      SELECT name FROM sys.databases WHERE name LIKE 'CDM-Test-%'
    `);
    const dbs: string[] = result.recordset.map((row: any) => row.name);
    for (const dbName of dbs) {
      try {
        // Set database to single user mode to force disconnects
        await req.query(
          `ALTER DATABASE [${dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE`,
        );
        await req.query(`ALTER DATABASE [${dbName}] SET MULTI_USER`);
        await req.query(s1.dropDatabase(dbName));
      } catch (err) {
        console.error(`Failed to delete database ${dbName}:`, err);
        if (err instanceof Error) {
          console.log(err.message);
        } else {
          console.log(err);
        }
        throw err;
      }
    }
  }

  static async dropAllLogins(userCfg: sql.config): Promise<void> {
    const serverPool = new sql.ConnectionPool(userCfg);
    const req = new sql.Request(serverPool);
    req.on('error', (err) => {
      console.error('SQL Server error:', err);
      throw err;
    });
    await serverPool.connect();
    // Get all logins with names starting with 'login_%'
    const result = await req.query(`
      SELECT name FROM sys.server_principals WHERE name LIKE 'login_%'
    `);
    const logins: string[] = result.recordset.map((row: any) => row.name);
    for (const loginName of logins) {
      try {
        await req.query(`DROP LOGIN [${loginName}]`);
        console.log(`Deleted login: ${loginName}`);
      } catch (err) {
        console.error(`Failed to delete login ${loginName}:`, err);
      }
    }
  }

  private async _extendTable(newTableCfg: TableCfg): Promise<void> {
    // Estimate added columns
    const tableKey = newTableCfg.key;
    try {
      const req = new sql.Request(this._conn);
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
      const alter = this.stat.alterTable(tableKey, addedColumns);
      for (const statement of alter) {
        await req.query(statement);
      }
    } catch (e) {
      console.error(e);
    }
  }

  private async _insertTableCfg(tableCfgHashed: TableCfg) {
    const req = new sql.Request(this._conn);
    hip(tableCfgHashed);
    const values = this.stat.serializeRow(
      tableCfgHashed,
      IoTools.tableCfgsTableCfg,
    );
    // Add each value as an input parameter
    values.forEach((val, idx) => {
      req.input(`p${idx}`, val);
    });
    await req.query(this.stat.insertTableCfg());
  }

  private async _createTable(
    tableCfgHashed: TableCfg,
    request: { tableCfg: TableCfg },
  ) {
    const req = new sql.Request(this._conn);
    this._insertTableCfg(tableCfgHashed);
    await req.query(this.stat.createTable(request.tableCfg));
  }
}
