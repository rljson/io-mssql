import { describe, it } from 'vitest';

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
  it('should create a database', async () => {
    const test = new IoMssql();
    test.connectToDatabase();
  });

  it('should log an error if the database connection fails', async () => {});
});
