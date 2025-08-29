import { runScript } from './run-script.ts';

/// Database Initialization (create database, schema etc.)
export class DbInit {
  static async createDatabase(
    adminConfig: any,
    databaseName: string,
  ): Promise<string[]> {
    const createDbScript = `
      IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'${databaseName}')
      BEGIN
        CREATE DATABASE [${databaseName}];
        SELECT 'Database ${databaseName} created' AS Status;
      END
      ELSE
      BEGIN
        SELECT 'Database ${databaseName} already exists' AS Status;
      END
    `;
    return await runScript(adminConfig, createDbScript);
  }

  static async dropDatabase(
    adminConfig: any,
    databaseName: string,
  ): Promise<string[]> {
    const deleteDbScript = `
      IF EXISTS (SELECT name FROM sys.databases WHERE name = N'${databaseName}')
      BEGIN
          DROP DATABASE [${databaseName}];
          SELECT 'Database ${databaseName} dropped' AS Status;
      END
       ELSE
      BEGIN
        SELECT 'Database ${databaseName} does not exist' AS Status;
      END
    `;
    return await runScript(adminConfig, deleteDbScript);
  }

  static async useDatabase(
    adminConfig: any,
    databaseName: string,
  ): Promise<string[]> {
    const useDbScript = `USE [${databaseName}];
    SELECT 'Using database ${databaseName}' AS Status;`;
    return await runScript(adminConfig, useDbScript);
  }

  /// Create Schema
  // The statement must be executed as one line in a batch,
  // otherwise an error will occur.************************
  static async createSchema(
    adminConfig: any,
    databaseName: string,
    schemaName: string,
  ): Promise<string[]> {
    const askSchemaExists = `IF NOT EXISTS (SELECT * FROM sys.schemas
        WHERE name = N'${schemaName}'
        AND schema_id = SCHEMA_ID('${schemaName}'))
      BEGIN
        SELECT 'Schema ${schemaName} does not exist' AS Status;
      END
      ELSE
      BEGIN
        SELECT 'Schema ${schemaName} already exists' AS Status;
      END
    `;
    const x = await runScript(adminConfig, askSchemaExists);
    if (x[0] === `Schema ${schemaName} does not exist`) {
      const createSchema = `USE [${databaseName}];\n GO --REM \n CREATE SCHEMA [${schemaName}]; \n GO --REM \n SELECT 'Schema ${schemaName} created' AS Status;`;
      const y = await runScript(adminConfig, createSchema);
      return y;
    }

    return x;
  }

  static async dropSchema(
    adminConfig: any,
    databaseName: string,
    schemaName: string,
  ): Promise<string> {
    const dropSchemaScript = `
      IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'${schemaName}' AND schema_id = SCHEMA_ID('${databaseName}'))
      BEGIN
          EXEC('DROP SCHEMA [${schemaName}]');
      END
    `;
    await runScript(adminConfig, dropSchemaScript);
    return schemaName;
  }

  static async dropLogins(adminConfig: any, schemaName: string): Promise<void> {
    const dropLoginsScript = `CREATE OR ALTER PROCEDURE
        ${schemaName}.DropCurrentConstraints(@SchemaName NVARCHAR(50))
        AS
        BEGIN
        -- Drop all foreign key constraints
        DECLARE @sql NVARCHAR(MAX) = N''
              SELECT @sql += 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + f.name + '];'
              FROM sys.foreign_keys f
              INNER JOIN sys.tables t ON f.parent_object_id = t.object_id
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE s.name = @SchemaName;
              EXEC sp_executesql @sql;
        END
        GO --REM;
    `;
    await runScript(adminConfig, dropLoginsScript);
  }
}
