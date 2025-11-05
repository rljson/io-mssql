// @license
// Copyright (c) 2025 Rljson

import { Io, IoTestSetup } from '@rljson/io';
import { adminCfg } from '../src/admin-cfg.ts';
import { DbBasics } from '../src/db-basics.ts';
import { IoMssql } from '../src/io-mssql.ts';



// ..............................................................................
class MyIoTestSetup implements IoTestSetup {
  masterMind: any;
  mio: any;
  dbName = 'TestDb-For-Io-Conformance';
  dbBasics = new DbBasics();

  async beforeAll(): Promise<void> {
    await this.dbBasics.createDatabase(adminCfg, this.dbName);
    await this.dbBasics.createSchema(adminCfg, this.dbName, 'main');
    await  this.dbBasics.installProcedures(adminCfg, this.dbName);
    
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
      await this.dbBasics.dropLogin(adminCfg, this.dbName, currentLogin);
    });
    this._io = null;
  }

  async afterAll(): Promise<void> {
    // No cleanup needed after all tests
    // await this.masterMind.close();
    await this.dbBasics.dropDatabase(adminCfg, this.dbName);
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
