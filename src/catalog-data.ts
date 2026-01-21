// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
import fs from 'fs';
import { fileURLToPath } from 'node:url';
import path from 'path';

export default class CatalogData {
  public static async get(fileName: string): Promise<JSON> {
    return await this._openJsonFile(fileName);
  }

  private static async _openJsonFile(fileName: string): Promise<JSON> {
    try {
      const __filename = fileURLToPath(import.meta.url);
      const __dirname = path.dirname(__filename);
      const completePath = path.join(__dirname, '../data/', fileName);
      if (!fs.existsSync(completePath)) {
        throw new Error(`File not found: ${completePath}`);
      }
      const fileContent = fs.readFileSync(completePath, 'utf-8');
      return JSON.parse(fileContent);
    } catch (error) {
      console.error(`Error reading JSON file: ${error}`);
      throw error;
    }
  }
}
