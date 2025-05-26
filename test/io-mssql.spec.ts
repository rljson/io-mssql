import sql from 'mssql';
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';

import { IoMssql } from '../src/io-mssql'; // Adjust the path as needed

// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

describe('IoMssql', () => {
  let ioSql: IoMssql;
  const adminCfg: sql.config = {
    server: 'localhost\\LOCALTESTSERVER',
    database: 'CDM-Test',
    options: {
      encrypt: true,
      trustServerCertificate: true,
    },
    user: 'CDM-Login',
    password: 'OneTwoThree',
  };

  beforeAll(async () => {
    await IoMssql.deleteAllTestDatabases(adminCfg);
    await IoMssql.dropAllLogins(adminCfg);
  });

  beforeEach(async () => {
    // Create general access to the server
    const masterMind = new IoMssql(adminCfg);

    // Create a new database for testing
    ioSql = await masterMind.example();
    await ioSql.init();
    await ioSql.isReady();
  });

  afterEach(async () => {
    await ioSql.close();
  });

  // Clean up after all tests have run
  afterAll(async () => {
    await IoMssql.deleteAllTestDatabases(adminCfg);
  });

  it('should connect to the database', async () => {
    expect(ioSql.isOpen).toBe(true);
  });
});
