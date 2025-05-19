import sql from 'mssql';
import { beforeEach, describe, expect, it } from 'vitest';

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
  let serverPool: sql.ConnectionPool;

  beforeEach(async () => {
    serverPool = await sql.connect({
      server: 'localhost\\LOCALTESTSERVER',
      database: 'CDM-Test',
      options: {
        encrypt: true,
        trustServerCertificate: true,
      },
      authentication: {
        type: 'ntlm',
        options: {
          domain: process.env.USERDOMAIN || '',
          userName: process.env.USERNAME || '',
          password: process.env.USERPASSWORD || '', // Set USERPASSWORD in your environment variables
        },
      },
    });

    console.log(serverPool);
  });

  it('should create a database', async () => {
    const test = new IoMssql(serverPool);
    await test.DoSomething();
    expect(serverPool).toBeDefined();
  });
});
