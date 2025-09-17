// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
import sql from 'mssql';

export const adminCfg: sql.config = {
  user: 'sa',
  password: 'Password123!',
  server: 'localhost',
  port: 1431,
  database: 'master',
  options: {
    encrypt: false,
    trustServerCertificate: true,
  },
};
