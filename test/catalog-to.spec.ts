// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
import { describe, expect, it } from 'vitest';

import { CatalogTo } from '../src/catalog-to';

describe('ImportCatalog', () => {
  it('should import into MSSQL database', async () => {
    const result = await CatalogTo.mssqlDb('outputcatalog.json');
    expect(result).toBe('OK');
  });
  // it('should import into SQLite database', async () => {
  //   await expect(CatalogTo.sqliteDb('outputcatalog.json')).resolves.toBe('OK');
  // });
});
