// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
// import { adminCfg, DbBasics, IoMssql } from '@rljson/io-mssql';
// import { IoSqliteNode } from '@rljson/io-sqlite-node';
import { TableCfg } from '@rljson/rljson';

import { adminCfg } from './admin-cfg.ts';
import CatalogData from './catalog-data.ts';
import { DbBasics } from './db-basics.ts';
import { IoMssql } from './io-mssql.ts';

export class CatalogTo {
  public static async mssqlDb(catalogName: string): Promise<string> {
    //catalog name always expected in data folder
    const testDbName = 'db_' + catalogName.replace(/\.json$/, '');
    const testSchemaName = 'main';

    const dbBasics = new DbBasics();
    await dbBasics.createDatabase(adminCfg, testDbName);
    await dbBasics.createSchema(adminCfg, testDbName, testSchemaName);

    const userCfg = { ...adminCfg, database: testDbName };

    const io: IoMssql = new IoMssql(userCfg, testSchemaName);
    await io.init();
    const [tableCfgs, catalogData] = await this._prepareData(catalogName);
    await this._insertTables(tableCfgs, io);
    await this._insertData(catalogData, tableCfgs, io);
    await io.close();

    return 'OK';
  }

  // public static async sqliteDb(catalogName: string): Promise<string> {
  //   const io: IoSqliteNode = new IoSqliteNode();
  //   io.dbFileName = `db_${catalogName.replace(/\.json$/, '')}.sqlite`;
  //   await io.init();
  //   await io.openOrCreateDatabase();
  //   const [tableCfgs, catalogData] = await this._prepareData(catalogName);
  //   await this._insertTables(tableCfgs, io).then(() =>
  //     this._insertData(catalogData, tableCfgs, io),
  //   );
  //   await io.close();
  //   return 'OK';
  // }

  private static async _prepareData(
    catalogName: string,
  ): Promise<[TableCfg[], JSON]> {
    const catalogData: JSON = await CatalogData.get(catalogName);
    const tableCfgs: TableCfg[] = (catalogData as any)['tableCfgs']['_data'];
    return [tableCfgs, catalogData];
  }

  private static async _insertTables(
    tableCfgs: TableCfg[],
    io: IoMssql,
  ): Promise<string> {
    for (const tableCfg of tableCfgs) {
      try {
        await io.createOrExtendTable({
          tableCfg: tableCfg as TableCfg,
        });
      } catch (e) {
        const error = e instanceof Error ? e : new Error(String(e));
        console.log(tableCfg.key, error.message);
        throw error;
      }
    }
    return 'Tables created/extended';
  }

  private static async _insertData(
    catalogData: JSON,
    tableCfgs: TableCfg[],
    io: IoMssql,
  ): Promise<string> {
    // Insert data
    console.log(`Inserting data for ${tableCfgs.length} tables`);

    for (const tableCfg of tableCfgs) {
      const tableKey = tableCfg.key;
      const tableBlock: JSON = (catalogData as any)[tableKey];
      const tableData: any[] = (tableBlock as any)['_data'] || [];
      if (tableData.length > 2100) {
        console.log(`Debug ${tableKey}`, tableData.length);
      }

      if (tableData.length > 0) {
        try {
          await io.write({
            data: {
              [tableKey]: {
                _type: 'components',
                _data: tableData,
              },
            },
          });
        } catch (e) {
          const error = e instanceof Error ? e : new Error(String(e));
          console.log(tableKey, error.message);
          // throw error;
        }
      }
    }

    return 'Data insertion complete';
  }
}
