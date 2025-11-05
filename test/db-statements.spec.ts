import { describe, it, expect } from "vitest";
import { IoTools } from "@rljson/io";
import { ColumnCfg } from "@rljson/rljson";
import { DbStatements } from '../src/db-statements.ts';

describe("DbStatements", () => {
    const schemaName = "myschema";
    const dbStatements = new DbStatements(schemaName);

    it("should set schemaName correctly", () => {
        expect(dbStatements.schemaName).toBe(schemaName);
    });

    it("should have correct table names", () => {
        expect(dbStatements.tbl.main).toBe("tableCfgs");
        expect(dbStatements.tbl.revision).toBe("revisions");
    });

    it("should have correct suffixes", () => {
        expect(dbStatements.suffix.col).toBe("_col");
        expect(dbStatements.suffix.tbl).toBe("_tbl");
        expect(dbStatements.suffix.tmp).toBe("_tmp");
    });

    
    describe('MsSqlStatements.jsonToSqlType', () => {
    
      it('should return NVARCHAR(MAX) for "string"', () => {
        expect(dbStatements.jsonToSqlType('string')).toBe('NVARCHAR(MAX)');
      });
    
      it('should return NVARCHAR(MAX) for "jsonArray"', () => {
        expect(dbStatements.jsonToSqlType('jsonArray')).toBe('NVARCHAR(MAX)');
      });
    
      it('should return NVARCHAR(MAX) for "json"', () => {
        expect(dbStatements.jsonToSqlType('json')).toBe('NVARCHAR(MAX)');
      });
    
      it('should return FLOAT for "number"', () => {
        expect(dbStatements.jsonToSqlType('number')).toBe('FLOAT');
      });
    
      it('should return BIT for "boolean"', () => {
        expect(dbStatements.jsonToSqlType('boolean')).toBe('BIT');
      });
    
      it('should return NVARCHAR(MAX) for "jsonValue"', () => {
        expect(dbStatements.jsonToSqlType('jsonValue')).toBe('NVARCHAR(MAX)');
      });
    
    });
    it("should generate correct CREATE DATABASE statement", () => {
        expect(dbStatements.createDatabase("testdb")).toBe("CREATE DATABASE [testdb]");
    });

    it("should generate correct DROP DATABASE statement", () => {
        expect(dbStatements.dropDatabase("testdb")).toBe("DROP DATABASE [testdb]");
    });

    describe("foreignKeys", () => {
      it("should generate correct foreign key constraint for single reference", () => {
        const refCol = "userRef";
        const result = dbStatements.foreignKeys([refCol]);
        expect(result).toBe(
          `CONSTRAINT FK_userRef_col FOREIGN KEY (userRef_col) REFERENCES myschema.user_tbl(_hash_col)`
        );
      });

      it("should generate correct foreign key constraints for multiple references", () => {
        const refCols = ["userRef", "groupRef"];
        const result = dbStatements.foreignKeys(refCols);
        expect(result).toBe(
          `CONSTRAINT FK_userRef_col FOREIGN KEY (userRef_col) REFERENCES myschema.user_tbl(_hash_col), ` +
          `CONSTRAINT FK_groupRef_col FOREIGN KEY (groupRef_col) REFERENCES myschema.group_tbl(_hash_col)`
        );
      });

      it("should return empty string for empty input", () => {
        expect(dbStatements.foreignKeys([])).toBe("");
      });
    });

    it("addTableSuffix and addColumnSuffix should add correct suffixes", () => {
        expect(dbStatements.addTableSuffix("bar")).toBe("bar_tbl");
        expect(dbStatements.addColumnSuffix("baz")).toBe("baz_col");
    });

    it("removeTableSuffix and removeColumnSuffix should remove correct suffixes", () => {
        expect(dbStatements.removeTableSuffix("bar_tbl")).toBe("bar");
        expect(dbStatements.removeColumnSuffix("baz_col")).toBe("baz");
         expect(dbStatements.removeTableSuffix("bar")).toBe("bar");
        expect(dbStatements.removeColumnSuffix("baz")).toBe("baz");
    });

    it("should generate correct rowCount SQL", () => {
        expect(dbStatements.rowCount("mytable")).toBe("SELECT COUNT(*) AS totalCount FROM [myschema].[mytable_tbl]");
    });

    it("should generate correct allData SQL", () => {
        expect(dbStatements.allData("mytable")).toBe("SELECT * FROM [myschema].[mytable_tbl]");
        expect(dbStatements.allData("mytable", "foo_col,bar_col")).toBe("SELECT foo_col,bar_col FROM [myschema].[mytable_tbl]");
    });

    describe('MsSqlStatements.whereString', () => {
      
    
        it('should generate correct SQL for string value', () => {
          const whereClause: [string, string][] = [['name', 'John']];
          expect(dbStatements.whereString(whereClause)).toBe(
            " name_col = 'John'",
          );
        });
    
        it('should generate correct SQL for number value', () => {
          const whereClause: [string, number][] = [['age', 30]];
          expect(dbStatements.whereString(whereClause)).toBe(' age_col = 30');
        });
    
        it('should generate correct SQL for boolean true value', () => {
          const whereClause: [string, boolean][] = [['isActive', true]];
          expect(dbStatements.whereString(whereClause)).toBe(
            ' isActive_col = 1',
          );
        });
    
        it('should generate correct SQL for boolean false value', () => {
          const whereClause: [string, boolean][] = [['isActive', false]];
          expect(dbStatements.whereString(whereClause)).toBe(
            ' isActive_col = 0',
          );
        });
    
        it('should generate correct SQL for boolean false value', () => {
          const whereClause: [string, boolean][] = [['isActive', true]];
          expect(dbStatements.whereString(whereClause)).toBe(
            ' isActive_col = 1',
          );
        });
      });

    it("should generate correct joinExpression", () => {
        expect(dbStatements.joinExpression("mytable", "t")).toBe("LEFT JOIN mytable AS t \n");
    });

    it("should serialize and parse data correctly", () => {
        const tableCfg = {
            key: "test",
            columns: [
                { key: "a", type: "string" },
                { key: "b", type: "number" },
                { key: "c", type: "boolean" },
                { key: "d", type: "json" }
            ]
        } as any;
        const row = { a: "foo", b: 1, c: true, d: { x: 1 } };
        const serialized = dbStatements.serializeRow(row, tableCfg);
        expect(serialized[0]).toBe("foo");
        expect(serialized[1]).toBe(1);
        expect(serialized[2]).toBe(1);
        expect(serialized[3]).toBe(JSON.stringify({ x: 1 }));

        const parsed = dbStatements.parseData([
            { a_col: "foo", b_col: 1, c_col: 1, d_col: JSON.stringify({ x: 1 }) }
        ], tableCfg);
        expect(parsed[0].a).toBe("foo");
        expect(parsed[0].b).toBe(1);
        expect(parsed[0].c).toBe(true);
        expect(parsed[0].d).toEqual({ x: 1 });
    });

    it("should generate correct tableKeys SQL", () => {
        expect(dbStatements.tableKeys).toContain("SELECT TABLE_NAME AS tableKey");
        expect(dbStatements.tableKeys).toContain("TABLE_SCHEMA = 'myschema'");
    });

    it("should generate correct tableExists SQL", () => {
        expect(dbStatements.tableExists).toContain("SELECT CASE WHEN EXISTS");
        expect(dbStatements.tableExists).toContain("TABLE_SCHEMA = 'myschema'");
    });

    it("should generate correct tableCfg SQL", () => {
        expect(dbStatements.tableCfg).toContain("SELECT * FROM [myschema].tableCfgs_tbl WHERE key_col = ?");
    });

    it("should generate correct tableCfgs SQL", () => {
        expect(dbStatements.tableCfgs).toBe("SELECT * FROM [myschema].tableCfgs_tbl");
    });

    it("should generate correct currentTableCfg SQL", () => {
        expect(dbStatements.currentTableCfg).toContain("WITH versions AS");
        expect(dbStatements.currentTableCfg).toContain("SELECT * FROM tableCfgs_tbl tt");
    });

    it("should generate correct currentTableCfgs SQL", () => {
        expect(dbStatements.currentTableCfgs).toContain("SELECT");
        expect(dbStatements.currentTableCfgs).toContain("FROM");
        expect(dbStatements.currentTableCfgs).toContain("WHERE");
    });

    it("should generate correct insertTableCfg SQL", () => {
        // This test assumes IoTools.tableCfgsTableCfg.columns exists and is an array of objects with .key
        // If not, this test should be adjusted or mocked accordingly.
        if (IoTools.tableCfgsTableCfg && Array.isArray(IoTools.tableCfgsTableCfg.columns)) {
            const sql = dbStatements.insertTableCfg();
            expect(sql).toContain("INSERT INTO [myschema].tableCfgs_tbl");
            expect(sql).toContain("VALUES");
        }
    });

    it("should generate correct getContentType SQL", async () => {
      // Assuming dbProcedures.contentType is a string like 'getContentTypeProc'
      // and _mainSchema is 'main' by default
      const tableName = "mytable";
      const schema = "myschema";
      // We need to get the value of dbProcedures.contentType
      // Since dbProcedures is imported dynamically, we access it here
      const { dbProcedures } = await import("../src/db-procedures.ts");
      const expected = `EXEC main.${dbProcedures.contentType} @schemaName = '${schema}', @tableKey = '${tableName}'`;
      expect(dbStatements.getContentType(tableName, schema)).toBe(expected);
    });

    it("should generate correct tableReferences SQL for single reference", () => {
      const refCol = "userRef";
      const result = dbStatements.tableReferences([refCol]);
      expect(result).toBe(
        `FOREIGN KEY (userRef) REFERENCES user (${dbStatements.addColumnSuffix(dbStatements.connectingColumn)})`
      );
    });

    it("should generate correct tableReferences SQL for multiple references", () => {
      const refCols = ["userRef", "groupRef"];
      const result = dbStatements.tableReferences(refCols);
      expect(result).toBe(
        `FOREIGN KEY (userRef) REFERENCES user (${dbStatements.addColumnSuffix(dbStatements.connectingColumn)}), ` +
        `FOREIGN KEY (groupRef) REFERENCES group (${dbStatements.addColumnSuffix(dbStatements.connectingColumn)})`
      );
    });

    it("should return empty string for tableReferences with empty input", () => {
      expect(dbStatements.tableReferences([])).toBe("");
    });

    it("should generate correct selection SQL", () => {
      const sql = dbStatements.selection("mytable", "foo_col,bar_col", "foo_col = 1");
      expect(sql).toBe("SELECT foo_col,bar_col FROM mytable WHERE foo_col = 1");
    });

    it("should generate correct alterTable SQL statements", () => {
      const addedColumns: ColumnCfg[] = [
        { key: "foo", type: "string", titleLong: "Foo Column", titleShort: "Foo" },
        { key: "bar", type: "number", titleLong: "Bar Column", titleShort: "Bar" }
      ];
      
      const stmts = dbStatements.alterTable("mytable", addedColumns);
      const result1 = "ALTER TABLE [myschema].[mytable_tbl] ADD foo_col NVARCHAR(MAX);";
      const result2 = "ALTER TABLE [myschema].[mytable_tbl] ADD bar_col FLOAT;";
      expect(stmts[0]).toEqual(result1);
      expect(stmts[1]).toEqual(result2);
    });

    describe("tableReferences", () => {
      it("should generate correct SQL for single reference", () => {
      const refCol = "userRef";
      const result = dbStatements.tableReferences([refCol]);
      expect(result).toBe(
        `FOREIGN KEY (userRef) REFERENCES user (${dbStatements.addColumnSuffix(dbStatements.connectingColumn)})`
      );
      });

      it("should generate correct SQL for multiple references", () => {
      const refCols = ["userRef", "groupRef"];
      const result = dbStatements.tableReferences(refCols);
      expect(result).toBe(
        `FOREIGN KEY (userRef) REFERENCES user (${dbStatements.addColumnSuffix(dbStatements.connectingColumn)}), ` +
        `FOREIGN KEY (groupRef) REFERENCES group (${dbStatements.addColumnSuffix(dbStatements.connectingColumn)})`
      );
      });

      it("should return empty string for empty input", () => {
      expect(dbStatements.tableReferences([])).toBe("");
      });
    });


});