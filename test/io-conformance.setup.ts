// @license
// Copyright (c) 2025 Rljson
import { Io, IoTestSetup } from '@rljson/io';

import { adminCfg } from '../src/admin-cfg';
import { DbBasics } from '../src/db-basics';
import { IoMssql } from '../src/io-mssql';


// ..............................................................................
class MyIoTestSetup implements IoTestSetup {
  afterAll: () => Promise<void>;
  // If you have 'mssql' installed, import and use its config type:
  // import type { config as MssqlConfig } from 'mssql';
  // const userCfg: MssqlConfig = { ... };

  // Otherwise, define the type inline:

  masterMind: IoMssql;
  mio: IoMssql;
  dbName = 'TestDb-For-Io-Conformance';

  async beforeAll(): Promise<void> {
    await DbBasics.createDatabase(adminCfg, this.dbName);
    await DbBasics.createSchema(adminCfg, this.dbName, 'main');
    await DbBasics.installProcedures(adminCfg, this.dbName);
    // No setup needed before all tests
    this.masterMind = new IoMssql(adminCfg, 'main');
  }

  async beforeEach(): Promise<void> {
    // Create example
    this.mio = await this.masterMind.example(this.dbName);
    this._io = this.mio;
  }

  async afterEach(): Promise<void> {
    // Clean up environment after each test
    const currentSchema = this.mio.currentSchema;
    await DbBasics.dropTables(adminCfg, this.dbName, currentSchema);
    await DbBasics.dropSchema(adminCfg, this.dbName, currentSchema);
    const currentLogin = this.mio.currentLogin;
    await this.io.close().then(async () => {
      await DbBasics.dropLogin(adminCfg, this.dbName, currentLogin);
    });
    await DbBasics.dropDatabase(adminCfg, this.dbName);
    this._io = null;
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
