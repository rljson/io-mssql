// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { Io, IoTestSetup } from '@rljson/io';

// found in the LICENSE file in the root of this package.
import sql from 'mssql';

import { IoMssql } from '../src/io-mssql';


// ..............................................................................
class MyIoTestSetup implements IoTestSetup {
  // If you have 'mssql' installed, import and use its config type:
  // import type { config as MssqlConfig } from 'mssql';
  // const userCfg: MssqlConfig = { ... };

  // Otherwise, define the type inline:
  userCfg: sql.config = {
    server: 'localhost\\LOCALTESTSERVER',
    database: 'CDM-Test',
    options: {
      encrypt: true,
      trustServerCertificate: true,
    },
    user: 'CDM-Login',
    password: 'OneTwoThree',
  };
  masterMind: IoMssql;
  mio: IoMssql;

  async beforeAll(): Promise<void> {
    // IoMssql.installScripts(this.userCfg);
    // No setup needed before all tests
    this.masterMind = new IoMssql(this.userCfg, 'PantrySchema');
  }

  async beforeEach(): Promise<void> {
    // Create example
    this.mio = await this.masterMind.example();
    this._io = this.mio;
  }

  async afterEach(): Promise<void> {
    // Clean up environment after each test
    const currentSchema = this.mio.currentSchema;
    await IoMssql.dropCurrentConstraints(this.userCfg, currentSchema);
    await IoMssql.dropCurrentSchema(this.userCfg, currentSchema);
    const currentLogin = this.mio.currentLogin;
    await this.io.close().then(async () => {
      await IoMssql.dropCurrentLogin(this.userCfg, currentLogin);
    });
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
