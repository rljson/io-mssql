import sql from 'mssql';

import { runScript } from './run-script.ts';

/// Database Initialization (create database, schema etc.)
export class DbInit {
  //****Database */
  /// Create Database
  ///(the only situation where the master must be accessed first)
  static async createDatabase(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `USE master;
    GO --REM
      IF NOT EXISTS (SELECT name FROM sys.databases
      WHERE name = N'${dbName}')
      BEGIN
        CREATE DATABASE [${dbName}];
        SELECT 'Database ${dbName} created' AS Status;
      END
      ELSE
      BEGIN
        SELECT 'Database ${dbName} already exists' AS Status;
      END
    `;
    return await runScript(adminConfig, script, dbName);
  }
  /// Use database, so that subsequent commands
  /// are executed in the context of the specified database
  static async useDatabase(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const useDbScript = `USE [${dbName}];
    SELECT 'Using database ${dbName}' AS Status;`;
    return await runScript(adminConfig, useDbScript, dbName);
  }
  /// Drop Database (only for testing)
  static async dropDatabase(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `USE master;
    GO --REM
      IF EXISTS (SELECT name FROM sys.databases WHERE name = N'${dbName}')
      BEGIN
      ALTER DATABASE [${dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
          DROP DATABASE [${dbName}];
          SELECT 'Database ${dbName} dropped' AS Status;
      END
       ELSE
      BEGIN
        SELECT 'Database ${dbName} does not exist' AS Status;
      END
    `;
    return await runScript(adminConfig, script, dbName);
  }

  //***Schema */
  /// Create Schema
  static async createSchema(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `
    IF NOT EXISTS (SELECT * FROM sys.schemas
        WHERE name = N'${schemaName}')
      BEGIN
       EXEC('CREATE SCHEMA [${schemaName}];');
       SELECT 'Schema ${schemaName} created' AS Status;
      END
      ELSE
      BEGIN
        SELECT 'Schema ${schemaName} already exists' AS Status;
      END
    `;
    return await runScript(adminConfig, script, dbName);
  }
  /// Drop Schema (only for testing)
  static async dropSchema(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `
      IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'${schemaName}')
      BEGIN
          EXEC('DROP SCHEMA [${schemaName}]');
          SELECT 'Schema ${schemaName} dropped' AS Status;
      END
      ELSE
      BEGIN
        SELECT 'Schema ${schemaName} does not exist' AS Status;
      END
    `;
    return await runScript(adminConfig, script, dbName);
  }

  // Procedures (creation & dropping)
  /// Drop Logins (only for testing)
  static async createProcDropLogins(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `
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
    return await runScript(adminConfig, script, dbName);
  }

  static async dropProcDropLogins(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `
    DROP PROCEDURE IF EXISTS [${schemaName}].[DropCurrentConstraints];
    GO --REM
    SELECT 'Procedure to drop logins dropped' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }

  static async createProcDropObjects(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `    CREATE OR ALTER   PROCEDURE
      [${schemaName}].[DropObjects]
      AS
      -- Delete all schemas that have been created
      -- apart from the main schema
      BEGIN
        DECLARE @SchemaName nvarchar(50)
        DECLARE @sql NVARCHAR(MAX) = N'';
        DECLARE SchemaNames CURSOR FOR
        SELECT name
        FROM sys.schemas
        WHERE name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys','${schemaName}');
        OPEN SchemaNames
        FETCH NEXT FROM SchemaNames INTO @SchemaName
        WHILE @@FETCH_STATUS = 0
          BEGIN
          PRINT @SchemaName
          EXEC ${schemaName}.DropSchema @SchemaName

          FETCH NEXT FROM SchemaNames INTO @SchemaName

          END

        CLOSE SchemaNames
        DEALLOCATE SchemaNames

        END;
GO --REM
SELECT 'Procedure DropObjects for ${schemaName} created' AS Status;`;

    return await runScript(adminConfig, script, dbName);
  }

  static async createProcDropSchema(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `
      CREATE OR ALTER PROCEDURE
      PantrySchema.DropSchema (@SchemaName NVARCHAR(50))
AS
BEGIN
DECLARE @sql nvarchar(max) = N''
 -- Drop all foreign key constraints
      SELECT @sql += 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + f.name + '];'
      FROM sys.foreign_keys f
      INNER JOIN sys.tables t ON f.parent_object_id = t.object_id
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = @SchemaName;
      EXEC sp_executesql @sql;

      -- Drop all tables
      SET @sql = N'';
      SELECT @sql += 'DROP TABLE IF EXISTS [' + s.name + '].[' + t.name + '];'
      FROM sys.tables t
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = @SchemaName;
      EXEC sp_executesql @sql;

       -- Drop all views
       SET @sql = N'';
       SELECT @sql += 'DROP VIEW [' + s.name + '].[' + v.name + '];'
       FROM sys.views v
       INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
       WHERE s.name = @SchemaName;
       EXEC sp_executesql @sql;

       -- Drop all sequences
       SET @sql = N'';
       SELECT @sql += 'DROP SEQUENCE [' + s.name + '].[' + seq.name + '];'
       FROM sys.sequences seq
       INNER JOIN sys.schemas s ON seq.schema_id = s.schema_id
       WHERE s.name = @SchemaName;
       EXEC sp_executesql @sql;

       -- Drop all functions
       SET @sql = N'';
       SELECT @sql += 'DROP FUNCTION [' + s.name + '].[' + o.name + '];'
       FROM sys.objects o
       INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
       WHERE s.name = @SchemaName AND o.type IN ('FN', 'IF', 'TF');
       EXEC sp_executesql @sql;

      -- Drop all stored procedures
      SET @sql = N'';
      SELECT @sql += 'DROP PROCEDURE [' + s.name + '].[' + p.name + '];'
      FROM sys.procedures p
      INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
      WHERE s.name = @SchemaName;
      EXEC sp_executesql @sql;

       -- Drop all types
       SET @sql = N'';
       SELECT @sql += 'DROP TYPE [' + s.name + '].[' + t.name + '];'
       FROM sys.types t
       INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
       WHERE s.name = @SchemaName AND t.is_user_defined = 1;
       EXEC sp_executesql @sql;

       SET @sql = N'DROP SCHEMA IF EXISTS [' + @SchemaName +']'
       EXEC sp_executesql @sql;

      END
      GO --REM
      SELECT 'Procedure DropSchema for ${schemaName} created' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }

  static async createProcDropConstraints(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `
      CREATE OR ALTER PROCEDURE
      ${schemaName}.DropConstraints (@TableName NVARCHAR(50))
AS
BEGIN
 DECLARE @sql NVARCHAR(MAX) = N''
      SELECT @sql += 'ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + f.name + '];'
      FROM sys.foreign_keys f
      INNER JOIN sys.tables t ON f.parent_object_id = t.object_id
      INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = '${schemaName}' AND t.name = @TableName;
      EXEC sp_executesql @sql;
END
GO --REM
SELECT 'Procedure DropConstraints for ${schemaName} created' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }

  //***user procedures  */

  static async dropUser(
    adminConfig: sql.config,
    userName: string,
    dbName: string,
  ) {
    const script = `IF EXISTS (SELECT name FROM sys.database_principals WHERE name = N'${userName}')
    BEGIN
      DROP USER [${userName}];
      SELECT 'USER [${userName}] DROPPED' AS Status;
    END
    ELSE
    BEGIN
      SELECT 'USER [${userName}] DOES NOT EXIST' AS Status;
    END
    GO --REM`;

    return await runScript(adminConfig, script, dbName);
  }

  static async createLogin(
    adminConfig: sql.config,
    dbName: string,
    loginName: string,
    loginPassword: string,
  ): Promise<string[]> {
    const script = `IF EXISTS (SELECT name FROM sys.server_principals WHERE name = N'${loginName}')
        BEGIN
        SELECT 'LOGIN [${loginName}] ALREADY EXISTS' AS Status;
      END
      ELSE
      BEGIN
        CREATE LOGIN [${loginName}] WITH PASSWORD='${loginPassword}', DEFAULT_DATABASE=[${dbName}], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;
        SELECT 'LOGIN [${loginName}] CREATED' AS Status;
      END`;
    return await runScript(adminConfig, script, dbName);
  }

  static async dropLogin(
    adminConfig: sql.config,
    loginName: string,
    dbName: string,
  ) {
    const script = `IF EXISTS (SELECT name FROM sys.server_principals WHERE name = N'${loginName}')
    BEGIN
      DROP LOGIN [${loginName}];
      SELECT 'LOGIN [${loginName}] DROPPED' AS Status;
    END
    ELSE
    BEGIN
      SELECT 'LOGIN [${loginName}] DOES NOT EXIST' AS Status;
    END`;
    return await runScript(adminConfig, script, dbName);
  }

  static async createUser(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    userName: string,
    loginName: string,
  ) {
    const script = `
      IF EXISTS (SELECT name FROM sys.database_principals
      WHERE type_desc = 'SQL_USER'
      AND name = '${userName}')
      BEGIN
        SELECT 'USER [${userName}] ALREADY EXISTS' AS Status;
      END
      ELSE
      BEGIN
        CREATE USER [${userName}] FOR LOGIN [${loginName}] WITH DEFAULT_SCHEMA=[${schemaName}];
        SELECT 'USER [${userName}] CREATED' AS Status;
      END
      GO --REM`;

    return await runScript(adminConfig, script, dbName);
  }

  public addUserToRole = (roleName: string, userName: string) =>
    `ALTER ROLE [${roleName}] ADD MEMBER [${userName}]`;

  static async getUsers(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `
                  SELECT name FROM sys.database_principals
                  WHERE type = 'S' AND name NOT LIKE '##%';
            `;
    return await runScript(adminConfig, script, dbName);
  }

  static async getTableNames(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `
      SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_TYPE = 'BASE TABLE';
    `;
    const result = await runScript(adminConfig, script, dbName);
    return result.map((row: any) => row.TABLE_NAME);
  }

  /// Initialize the database including scripts
  static async initDb(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    loginName: string,
    loginPassword: string,
  ) {
    await DbInit.createDatabase(adminConfig, dbName);
    await DbInit.useDatabase(adminConfig, dbName);
    await DbInit.createSchema(adminConfig, dbName, schemaName);
    await DbInit.createLogin(adminConfig, dbName, loginName, loginPassword);
    await DbInit.createUser(
      adminConfig,
      dbName,
      schemaName,
      loginName,
      loginName,
    );
  }
}
