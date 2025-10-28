import { IoTools } from "@rljson/io";
import { Json, JsonValue, JsonValueType } from "@rljson/json";
import { ColumnCfg, TableCfg, TableKey } from "@rljson/rljson";
import { dbProcedures as DbProcs } from './db-procedures.ts';
export class DbStatements  {
  
  private _mainSchema: string;
  // ********************************************************************
  // Initialization needs the schema name
	constructor(public schemaName: string, mainSchema: string = 'main') {
    this._mainSchema = mainSchema;
  }

  // ********************************************************************
  // General statements and properties
  queryIntro: string = 'SELECT DISTINCT';
  connectingColumn: string = '_hash';

  // Names for the main tables in the database
  tbl: { [key: string]: string } = {
    main: 'tableCfgs',
    revision: 'revisions',
  };

  /// Postfix handling for the database
  suffix: { [key: string]: string } = {
    col: '_col',
    tbl: '_tbl',
    tmp: '_tmp',
    Ref: 'Ref',
  };

   /**
    * Converts a JSON value type to an SQL data type.
    * @param dataType - The JSON value type to convert.
    * @returns - The corresponding SQL data type.
    */
     jsonToSqlType(dataType: JsonValueType): string {
    // Custom implementation or call super if needed
    // Example: return this.stat.jsonToSqlType(dataType);
    switch (dataType) {
      case 'string':
        return 'NVARCHAR(MAX)';
      case 'jsonArray':
        return 'NVARCHAR(MAX)';
      case 'json':
        return 'NVARCHAR(MAX)';
      case 'number':
        return 'FLOAT';
      case 'boolean':
        return 'BIT';
      case 'jsonValue':
        return 'NVARCHAR(MAX)';
      default:
        throw new Error(`Unknown JsonValueType: ${dataType}`);
    }
  }
  
  // ********************************************************************
  // Database-level statements
    public createDatabase = (dbName: string) => `CREATE DATABASE [${dbName}]`;
    public dropDatabase = (dbName: string) => `DROP DATABASE [${dbName}]`;
  
  // ********************************************************************
  // add and remove suffixes for internal use  
    addFix(name: string, fix: string): string {
      return name.endsWith(fix) ? name : name + fix;
    }

    addTableSuffix(name: string): string {
      return this.addFix(name, this.suffix.tbl);
    }

    addColumnSuffix(name: string): string {
      return this.addFix(name, this.suffix.col);
    }

    removeFix(name: string, fix: string): string {
      return name.endsWith(fix) ? name.slice(0, -fix.length) : name;
    }

    removeTableSuffix(name: string): string {
      return this.removeFix(name, this.suffix.tbl);
    }

    removeColumnSuffix(name: string): string {
      return this.removeFix(name, this.suffix.col);
    }


  // ********************************************************************
  // General statements for tables
  public createTable(tableCfg: TableCfg): string {
    const sqltableKey = this.addTableSuffix(tableCfg.key);
    const columnsCfg = tableCfg.columns;

    const sqlCreateColumns = columnsCfg
      .map((col) => {
        let sqlType = this.jsonToSqlType(col.type);
        if (
          (col.key.endsWith('Ref') || col.key === this.connectingColumn) &&
          sqlType.toLowerCase() === 'nvarchar(max)'
        ) {
          sqlType = 'NVARCHAR(256)';
        }
        return `${this.addColumnSuffix(col.key)} ${sqlType}`;
      })
      .join(', ');

    // standard primary key - do not remove ;-)
    const primaryKey = `CONSTRAINT PK_${
      tableCfg.key
    } PRIMARY KEY ([${this.addColumnSuffix(this.connectingColumn)}])`;

    const foreignKeysArr = this.foreignKeys(
      columnsCfg
        .map((col) => col.key)
        .filter((col) => col.endsWith(this.suffix.ref)),
    );

    /* v8 ignore next -- @preserve */
    const foreignKeys = Array.isArray(foreignKeysArr)
      ? foreignKeysArr.filter(Boolean).join(', ')
      : foreignKeysArr || '';

    const colsWithPrimaryKey = `${sqlCreateColumns}, ${primaryKey}`;
    const colsWithPrimaryKeyAndForeignKeys = foreignKeys
      ? `${colsWithPrimaryKey}, ${foreignKeys}`
      : colsWithPrimaryKey;

    const sqlIfNotExists = `IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '${sqltableKey.replace(
      /^\[|\]$/g,
      '',
    )}' AND TABLE_SCHEMA = '${this.schemaName}')
  BEGIN
    CREATE TABLE [${
      this.schemaName
    }].${sqltableKey} (${colsWithPrimaryKeyAndForeignKeys})
  END`;
    return sqlIfNotExists;
  }

  public alterTable(tableKey: TableKey, addedColumns: ColumnCfg[]): string[] {
      const tableKeyWithSuffix = this.addTableSuffix(tableKey);
      const statements: string[] = [];
      for (const col of addedColumns) {
        const columnKey = this.addColumnSuffix(col.key);
        const columnType = this.jsonToSqlType(col.type);
        statements.push(
          `ALTER TABLE [${this.schemaName}].${tableKeyWithSuffix} ADD ${columnKey} ${columnType};`,
        );
      }
      return statements;
    }

 
  public insertTableCfg() {
    const columnKeys = IoTools.tableCfgsTableCfg.columns.map((col) => col.key);
    const columnKeysWithPostfix = columnKeys.map((col) =>
      this.addColumnSuffix(col),
    );
    const columnsSql = columnKeysWithPostfix.join(', ');
    const valuesSql = columnKeys.map((_, i) => `@p${i}`).join(', ');

    return `INSERT INTO [${this.schemaName}].${this.tbl.main}${this.suffix.tbl} ( ${columnsSql} ) VALUES (${valuesSql})`;
  }

  public getContentType(tableName: string, schemaName: string) {
    return  `EXEC ${this._mainSchema}.${DbProcs.contentType} @schemaName = '${schemaName}', @tableKey = '${tableName}'`;
  }

  public foreignKeys(refColumnNames: string[]) {
    return refColumnNames
      .map(
        (col) =>
          `CONSTRAINT FK_${col}${this.suffix.col} FOREIGN KEY (${col}${
            this.suffix.col
          }) REFERENCES ${this.schemaName}.${this.addTableSuffix(
            col.slice(0, -this.suffix.ref.length),
          )}(${this.addColumnSuffix(this.connectingColumn)})`,
      )
      .join(', ');
    } 

  public tableReferences(referenceArray: string[]): string {
    return referenceArray
      .map(
        (col) =>
          `FOREIGN KEY (${col}) REFERENCES ${col.slice(
            0,
            -this.suffix.ref.length,
          )} (${this.addColumnSuffix(this.connectingColumn)})`,
      )
      .join(', ');
  }
  
  public get tableKeys() {
    return `SELECT TABLE_NAME AS tableKey FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '${this.schemaName}'`;
  }

  public get tableExists() {
    return `SELECT CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = '${this.schemaName}') THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS tableExists`;
  }

  public get tableCfg() {
    return `SELECT * FROM [${this.schemaName}].${this.tbl.main}${this.suffix.tbl} WHERE key${this.suffix.col} = ?`;
  }

  public get tableCfgs() {
    return `SELECT * FROM [${this.schemaName}].${this.tbl.main}${this.suffix.tbl}`;
  }


  public get currentTableCfg(): string {
    const sql: string[] = [
      'WITH versions AS (',
      ' SELECT _hash_col, key_col, MAX(json_each.key) AS max_val',
      ' FROM tableCfgs_tbl, json_each(columns_col)',
      ' WHERE json_each.value IS NOT NULL',
      ' AND key_col = ? GROUP BY _hash_col, key_col)',
      'SELECT * FROM tableCfgs_tbl tt',
      ' LEFT JOIN versions ON tt._hash_col = versions._hash_col',
      ' WHERE versions.max_val = (SELECT MAX(max_val) FROM versions);',
    ];
    return sql.join('\n');
  }

  public get currentTableCfgs(): string {
    const sql: string[] = [
      'SELECT',
      '  *',
      'FROM',
      '  tableCfgs_tbl',
      'WHERE',
      '  _hash_col IN (',
      '    WITH',
      '      column_count AS (',
      '        SELECT',
      '          _hash_col,',
      '          key_col,',
      '          MAX(json_each.key) AS max_val',
      '        FROM',
      '          tableCfgs_tbl,',
      '          json_each (columns_col)',
      '        WHERE',
      '          json_each.value IS NOT NULL',
      '        GROUP BY',
      '          _hash_col,',
      '          key_col',
      '      ),',
      '      max_tables AS (',
      '        SELECT',
      '          key_col,',
      '          MAX(max_val) AS newest',
      '        FROM',
      '          column_count',
      '        GROUP BY',
      '          key_col',
      '      )',
      '    SELECT',
      '      cc._hash_col',
      '    FROM',
      '      column_count cc',
      '      LEFT JOIN max_tables mt ON cc.key_col = mt.key_col',
      '      AND cc.max_val = mt.newest',
      '    WHERE',
      '      mt.newest IS NOT NULL',
      '  );',
    ];
    return sql.join('\n');
  }

  public rowCount(tableKey: string) {
    return `SELECT COUNT(*) AS totalCount FROM [${this.schemaName}].[${this.addTableSuffix(tableKey)}]`;
  }
  
  public selection(tableKey: string, columns: string, whereClause: string) {
    return `SELECT ${columns} FROM ${tableKey} WHERE ${whereClause}`;
  }

  public allData(tableKey: string, namedColumns?: string) {
    if (!namedColumns) {
      namedColumns = `*`;
    }
    return `SELECT ${namedColumns} FROM [${
      this.schemaName
    }].[${this.addTableSuffix(tableKey)}]`;
  }

  public whereString(whereClause: [string, JsonValue][]): string {
    let constraint: string = ' ';
      for (const [column, value] of whereClause) {
        const columnWithFix = this.addColumnSuffix(column);
        /* v8 ignore next -- @preserve */
  
        if (typeof value === 'string') {
          constraint += `${columnWithFix} = '${value}' AND `;
        } else if (typeof value === 'number') {
          constraint += `${columnWithFix} = ${value} AND `;
        } else if (typeof value === 'boolean') {
          constraint += `${columnWithFix} = ${value ? 1 : 0} AND `;
        } else if (value === null) {
          constraint += `${columnWithFix} IS NULL AND `;
        } else if (typeof value === 'object') {
          constraint += `${columnWithFix} = '${JSON.stringify(value)}' AND `;
        } else {
          throw new Error(`Unsupported value type for column ${column}`);
        }
      }
      
    /* v8 ignore next -- @preserve */
    constraint = constraint.endsWith('AND ')
      ? constraint.slice(0, -5)
      : constraint; // remove last ' AND '

    return constraint;
  }
  
  public joinExpression(tableKey: string, alias: string) {
    return `LEFT JOIN ${tableKey} AS ${alias} \n`;
  }

  public parseData(data: Json[], tableCfg: TableCfg): Json[] {
      const columnTypes = tableCfg.columns.map((col) => col.type);
      const columnKeys = tableCfg.columns.map((col) => col.key);  
      const convertedResult: Json[] = [];
  
      for (const row of data) {
        const convertedRow: { [key: string]: any } = {};
        for (let colNum = 0; colNum < columnKeys.length; colNum++) {
          const key = columnKeys[colNum];
          const keyWithSuffix = this.addColumnSuffix(key);
          const type = columnTypes[colNum] as JsonValueType;
          const val = row[keyWithSuffix];
  
          // Null or undefined values are ignored
          // and not added to the converted row
          if (val === undefined) {
            continue;
          }
  
          if (val === null) {
            continue;
          }
  
          switch (type) {
            case 'boolean':
              convertedRow[key] = val !== 0;
              break;
            /* v8 ignore next -- @preserve */
            case 'jsonArray':
  
            case 'json':
              convertedRow[key] = JSON.parse(val as string);
              break;
            case 'string':
            case 'number':
              convertedRow[key] = val;
              break;
  
            default:
              throw new Error('Unsupported column type ' + type);
          }
        }
  
        convertedResult.push(convertedRow);
      }
  
      return convertedResult;
    }
  
  public serializeRow(
      rowAsJson: Json,
      tableCfg: TableCfg,
    ): (JsonValue | null)[] {
      const result: (JsonValue | null)[] = [];
  
      // Iterate all columns in the tableCfg
      for (const col of tableCfg.columns) {
        const key = col.key;
        let value = rowAsJson[key] ?? null;
        const valueType = typeof value;
  
        // Stringify objects and arrays
        if (value !== null && valueType === 'object') {
          value = JSON.stringify(value);
        }
  
        // Convert booleans to 1 or 0
        else if (valueType === 'boolean') {
          value = value ? 1 : 0;
        }
  
        result.push(value);
      }
  
      return result;
    }
}