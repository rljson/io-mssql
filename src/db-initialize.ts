import { runScript } from './run-script.ts';

/// Database Initialization (create database, schema etc.)
export class DbInit {
  /// Create Database
  static async createDatabase(
    adminConfig: any,
    databaseName: string,
  ): Promise<string[]> {
    const script = `
      IF NOT EXISTS (SELECT name FROM sys.databases
      WHERE name = N'${databaseName}')
      BEGIN
        CREATE DATABASE [${databaseName}];
        SELECT 'Database ${databaseName} created' AS Status;
      END
      ELSE
      BEGIN
        SELECT 'Database ${databaseName} already exists' AS Status;
      END
    `;
    return await runScript(adminConfig, script);
  }

  /// Drop Database (only for testing)
  static async dropDatabase(
    adminConfig: any,
    databaseName: string,
  ): Promise<string[]> {
    const script = `
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
    return await runScript(adminConfig, script);
  }

  /// Use database, so that subsequent commands
  /// are executed in the context of the specified database
  static async useDatabase(
    adminConfig: any,
    databaseName: string,
  ): Promise<string[]> {
    const useDbScript = `USE [${databaseName}];
    SELECT 'Using database ${databaseName}' AS Status;`;
    return await runScript(adminConfig, useDbScript);
  }

  /// Create Schema
  static async createSchema(
    adminConfig: any,
    databaseName: string,
    schemaName: string,
  ): Promise<string[]> {
    const askScript = `USE [${databaseName}];
    GO --REM
    IF NOT EXISTS (SELECT * FROM sys.schemas
        WHERE name = N'${schemaName}')
      BEGIN
        SELECT 'Schema ${schemaName} does not exist' AS Status;
      END
      ELSE
      BEGIN
        SELECT 'Schema ${schemaName} already exists' AS Status;
      END
    `;
    const schemaExists = await runScript(adminConfig, askScript);
    if (schemaExists[0] === `Schema ${schemaName} does not exist`) {
      const createScript = `USE [${databaseName}];\n GO --REM
       \n CREATE SCHEMA [${schemaName}]; \n GO --REM
       \n SELECT 'Schema ${schemaName} created' AS Status;`;
      return await runScript(adminConfig, createScript);
    } else {
      return schemaExists;
    }
  }
  /// Drop Schema (only for testing)
  static async dropSchema(
    adminConfig: any,
    databaseName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `USE [${databaseName}];
      GO --REM
      IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'${schemaName}')
      BEGIN
          EXEC('DROP SCHEMA [${schemaName}]');
          SELECT 'Schema ${schemaName} dropped' AS Status;
      END
    `;
    return await runScript(adminConfig, script);
  }

  /// Drop Logins (only for testing)
  static async dropLogins(
    adminConfig: any,
    databaseName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `USE [${databaseName}];
    GO --REM
    CREATE OR ALTER PROCEDURE
        ${schemaName}.DropCurrentConstraints(@SchemaName NVARCHAR(50))
        AS
        BEGIN
        -- Drop all foreign key constraints
        DECLARE @sql NVARCHAR(MAX) = N''
              SELECT @sql += 'ALTER TABLE [' + s.name + '].[' + t.name + ']
              DROP CONSTRAINT [' + f.name + '];'
              FROM sys.foreign_keys f
              INNER JOIN sys.tables t ON f.parent_object_id = t.object_id
              INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE s.name = @SchemaName;
              EXEC sp_executesql @sql;
        END
        GO --REM;
        SELECT 'Procedure to drop logins created' AS Status;
    `;
    return await runScript(adminConfig, script);
  }

  static async dropObjects(
    adminConfig: any,
    // databaseName: string,
    schemaName: string,
  ): Promise<void> {
    const script = `CREATE OR ALTER   PROCEDURE
[${schemaName}].[DropAllPantryObjects]
AS
-- Delete all schemas that have been created
-- apart from the main schema
BEGIN
  DECLARE @SchemaName nvarchar(50)
  DECLARE @sql NVARCHAR(MAX) = N'';
  DECLARE SchemaNames CURSOR FOR
  SELECT name
  FROM sys.schemas
  WHERE name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys','PantrySchema');
  OPEN SchemaNames
  FETCH NEXT FROM SchemaNames INTO @SchemaName
  WHILE @@FETCH_STATUS = 0
    BEGIN
    PRINT @SchemaName
    EXEC PantrySchema.DropCurrentSchema @SchemaName

	   FETCH NEXT FROM SchemaNames INTO @SchemaName

    END

	CLOSE SchemaNames
	DEALLOCATE SchemaNames

  END;
GO --REM
SELECT 'All pantry objects dropped' AS Status;`;

    await runScript(adminConfig, script);
  }
}
