import { describe, it, expect } from "vitest";
import { DbStatements } from "../src/db-statements";
import { IoTools } from "@rljson/io";

describe("DbStatements", () => {
    const schemaName = "myschema";
    const dbStatements = new DbStatements(schemaName);

    it("should set schemaName correctly", () => {
        expect(dbStatements.schemaName).toBe(schemaName);
    });

    it("should have correct queryIntro", () => {
        expect(dbStatements.queryIntro).toBe("SELECT DISTINCT");
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


    it("addFix should add suffix if not present", () => {
        expect(dbStatements.addFix("foo", "_col")).toBe("foo_col");
        expect(dbStatements.addFix("foo_col", "_col")).toBe("foo_col");
    });

    it("addTableSuffix and addColumnSuffix should add correct suffixes", () => {
        expect(dbStatements.addTableSuffix("bar")).toBe("bar_tbl");
        expect(dbStatements.addColumnSuffix("baz")).toBe("baz_col");
    });

    it("removeFix should remove suffix if present", () => {
        expect(dbStatements.removeFix("foo_col", "_col")).toBe("foo");
        expect(dbStatements.removeFix("foo", "_col")).toBe("foo");
    });

    it("removeTableSuffix and removeColumnSuffix should remove correct suffixes", () => {
        expect(dbStatements.removeTableSuffix("bar_tbl")).toBe("bar");
        expect(dbStatements.removeColumnSuffix("baz_col")).toBe("baz");
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

});