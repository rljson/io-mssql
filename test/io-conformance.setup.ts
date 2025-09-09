// @license
// Copyright (c) 2025 Rljson
// Use of this source code is governed by terms that can be
import { Io, IoTestSetup } from '@rljson/io';

// used to store logins
import 'dotenv/config';
// found in the LICENSE file in the root of this package.
import sql from 'mssql';

import { DbBasics } from '../src/db-basics';
import { IoMssql } from '../src/io-mssql';

// ..............................................................................
class MyIoTestSetup implements IoTestSetup {
  // If you have 'mssql' installed, import and use its config type:
  // import type { config as MssqlConfig } from 'mssql';
  // const userCfg: MssqlConfig = { ... };

  // Otherwise, define the type inline:
  userCfg: sql.config = {
    user: 'sa',
    password: process.env.SA_PASSWORD,
    server: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 1431,
    database: process.env.DB_NAME,
    options: {
      encrypt: false, // set to true if using SSL
      trustServerCertificate: true, // needed for local dev
    },
  };
  masterMind: IoMssql;
  mio: IoMssql;
  dbName = 'TestDb-For-Io-Conformance';

  async beforeAll(): Promise<void> {
    await DbBasics.createDatabase(this.userCfg, this.dbName);
    await DbBasics.createSchema(this.userCfg, this.dbName, 'main');
    await DbBasics.installProcedures(this.userCfg, this.dbName);
    // No setup needed before all tests
    this.masterMind = new IoMssql(this.userCfg, 'main');
  }

  async beforeEach(): Promise<void> {
    // Create example
    this.mio = await this.masterMind.example(this.dbName);
    this._io = this.mio;
  }

  async afterEach(): Promise<void> {
    // Clean up environment after each test
    const currentSchema = this.mio.currentSchema;
    await DbBasics.dropTables(this.userCfg, this.dbName, currentSchema);
    await DbBasics.dropSchema(this.userCfg, this.dbName, currentSchema);
    const currentLogin = this.mio.currentLogin;
    await this.io.close().then(async () => {
      await DbBasics.dropLogin(this.userCfg, this.dbName, currentLogin);
    });
    await DbBasics.dropDatabase(this.userCfg, this.dbName);
    this._io = null;
  }

  async afterAll(): Promise<void> {
    // Clean up environment after all tests
    // await IoMssql.dropTestLogins(this.userCfg);
    // await IoMssql.dropTestSchemas(this.userCfg);
  }

  get io(): Io {
    if (!this._io) {
      throw new Error('Call beforeEach() before accessing io');
    }
    return this._io;
  }

  private _io: Io | null = null;
}

// .............................................................................
export const testSetup = () => new MyIoTestSetup();
