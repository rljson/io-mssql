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
  async init(): Promise<void> {
    const masterMind = new IoMssql(this.userCfg);
    this._io = await masterMind.example();
  }

  async tearDown(): Promise<void> {
    await IoMssql.dropAllTestDatabases(this.userCfg);
    await IoMssql.dropAllLogins(this.userCfg);
    await this.io.close();
  }

  get io(): Io {
    if (!this._io) {
      throw new Error('Call init() before accessing io');
    }
    return this._io;
  }

  private _io: Io | null = null;
}

// .............................................................................
export const testSetup = () => new MyIoTestSetup();
