// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// import { IoSqlite } from '@rljson/io-sqlite';
// import { Json } from '@rljson/json';
// import { IoSqlite } from '@rljson/io-sqlite';
// import {Io} from '@rljson/io';

import sql from 'mssql';

export class IoMssql {
  constructor(private readonly serverPool: sql.ConnectionPool) {
    // super('dbPath');
    // IoMssql._serverPool = serverPool;
  }

  public async DoSomething() {
    try {
      const request = new sql.Request(this.serverPool);
      const result = await request.query('SELECT * FROM dbo.FirstTable');
      console.table(result.recordset);
    } catch (error) {
      console.error('Error executing query:', error);
    }
  }

  static example = async (serverPool: sql.ConnectionPool) => {
    const randomString = Math.random().toString(36).substring(2, 22);
    const dbName = `CDM-Test-${randomString}`;
    const request = new sql.Request(serverPool);
    await request.query(`CREATE DATABASE [${dbName}]`);
    console.log(`Database created: ${dbName}`);
  };
}
