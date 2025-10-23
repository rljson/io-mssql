// @license
// Copyright (c) 2025 Rljson

import { Io, IoTestSetup } from '@rljson/io';

import { adminCfg } from '../src/admin-cfg';
import { DbBasics } from '../src/db-basics';
import { IoMssql } from '../src/io-mssql';

// ..............................................................................
class MyIoTestSetup implements IoTestSetup {
  masterMind!: IoMssql;
  mio!: IoMssql;
  dbName = 'TestDb-For-Io-Conformance';

  async beforeAll(): Promise<void> {
    await DbBasics.createDatabase(adminCfg, this.dbName);
    await DbBasics.createSchema(adminCfg, this.dbName, 'main');
    const result = await DbBasics.installProcedures(adminCfg, this.dbName);
    console.log(`Installed ${result.length} procedures`);
    // No setup needed before all tests
    this.masterMind = new IoMssql(adminCfg, 'main');
  }

  async beforeEach(): Promise<void> {
    // Create example
    this.mio = await this.masterMind.example(this.dbName);
    this._io = this.mio;
  }

  async afterEach(): Promise<void> {
    const currentLogin = this.mio.currentLogin;
    await this.mio.close().then(async () => {
      await DbBasics.dropLogin(adminCfg, this.dbName, currentLogin);
    });
    this._io = null;
  }

  async afterAll(): Promise<void> {
    // No cleanup needed after all tests
    // await this.masterMind.close();
    await DbBasics.dropDatabase(adminCfg, this.dbName);
  }
  get io(): Io {
    if (!this._io) {
      throw new Error('Call beforeEach() before accessing io');
    }
    return this._io;
  }

  protected _io: Io | null = null;
}

// .............................................................................
export const testSetup = () => new MyIoTestSetup();
