// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoTestSetup } from '@rljson/io';

import { IoMssql } from '../src/io-mssql';

// ..............................................................................
class MyIoTestSetup implements IoTestSetup {
  async init(): Promise<void> {
    const masterMind = new IoMssql({
      server: 'localhost\\LOCALTESTSERVER',
      database: 'CDM-Test',
      options: {
        encrypt: true,
        trustServerCertificate: true,
      },
      user: 'CDM-Login',
      password: 'OneTwoThree',
    });
    this._io = await masterMind.example();
  }

  async tearDown(): Promise<void> {
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
