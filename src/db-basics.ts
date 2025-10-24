import { ContentType } from '@rljson/rljson';

import sql from 'mssql';

import { runScript } from './run-script.ts';
import { SqlStatements } from './sql-statements.ts';

/// Database Initialization (create database, schema etc.)
/// These are static methods to deal with the database itself
export class DbBasics {
  static _mainSchema: string = 'main';
  static _dropConstraintsProc: string = 'DropConstraints';
  static _dropObjectsProc: string = 'DropObjects';
  static _dropLoginsProc: string = 'DropLogins';
  static _dropSchemaProc: string = 'DropSchema';
  static _contentTypeProc: string = 'GetContentType';

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

  /// Enable Change Data Capture (CDC) for a database
  static async enableCdcDb(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `EXEC sys.sp_cdc_enable_db;
    SELECT 'CDC enabled for database ${dbName}' AS Status;`;
    return await runScript(adminConfig, script, dbName);
  }

  static async disableCdcDb(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `EXEC sys.sp_cdc_disable_db;
    SELECT 'CDC disabled for database ${dbName}' AS Status;`;
    return await runScript(adminConfig, script, dbName);
  }

  static async enableCDCTable(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    tableName: string,
  ): Promise<string[]> {
    const script = `
     EXEC sys.sp_cdc_enable_table
    @source_schema = N'${schemaName}',
    @source_name   = N'${tableName}',
    @role_name     = NULL;

    SELECT 'CDC enabled for table ${schemaName}.${tableName}' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }
  static async disableCDCTable(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    tableName: string,
  ): Promise<string[]> {
    const script = `
    EXEC sys.sp_cdc_disable_table
    @source_schema = N'${schemaName}',
    @source_name   = N'${tableName}',
    @capture_instance = N'${schemaName}_${tableName}';
    SELECT 'CDC disabled for table ${schemaName}.${tableName}' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
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
    if (schemaName === 'dbo' || schemaName === 'main') {
      return [`Cannot drop schema ${schemaName}`];
    }
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
  ): Promise<string[]> {
    const script = `
      CREATE OR ALTER PROCEDURE
          ${this._mainSchema}.${this._dropLoginsProc}( @SchemaName NVARCHAR(50))
          AS
      BEGIN
        DECLARE @Prefix NVARCHAR(100) = 'test_';
        DECLARE @LoginName NVARCHAR(128);
        DECLARE @SQL NVARCHAR(MAX);

        DECLARE login_cursor CURSOR FOR
        SELECT name
        FROM sys.server_principals
        WHERE type_desc = 'SQL_LOGIN'
          AND name LIKE @Prefix + '%';

        OPEN login_cursor;
        FETCH NEXT FROM login_cursor INTO @LoginName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SQL = 'DROP LOGIN [' + @LoginName + ']';
            EXEC sp_executesql @SQL;

            FETCH NEXT FROM login_cursor INTO @LoginName;
        END

        CLOSE login_cursor;
        DEALLOCATE login_cursor;

      END
      GO --REM;
      SELECT 'Procedure to drop logins created' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }

  static async createProcDropObjects(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `    CREATE OR ALTER   PROCEDURE
      [${this._mainSchema}].[${this._dropObjectsProc}]
      AS
      -- Delete all schemas that have been created
      -- apart from the main schema
      BEGIN
        DECLARE @SchemaName nvarchar(50)
        DECLARE @sql NVARCHAR(MAX) = N'';
        DECLARE SchemaNames CURSOR FOR
        SELECT name
        FROM sys.schemas
        WHERE name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys','${this._mainSchema}');
        OPEN SchemaNames
        FETCH NEXT FROM SchemaNames INTO @SchemaName
        WHILE @@FETCH_STATUS = 0
          BEGIN
          PRINT @SchemaName
          EXEC ${this._mainSchema}.DropSchema @SchemaName

          FETCH NEXT FROM SchemaNames INTO @SchemaName

          END

        CLOSE SchemaNames
        DEALLOCATE SchemaNames

        END;
      GO --REM
      SELECT 'Procedure DropObjects for ${this._mainSchema} created' AS Status;`;

    return await runScript(adminConfig, script, dbName);
  }

  static async createProcDropSchema(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `
      CREATE OR ALTER PROCEDURE
      ${this._mainSchema}.${this._dropSchemaProc} (@SchemaName NVARCHAR(50))
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
      SELECT 'Procedure ${this._dropSchemaProc} for ${this._mainSchema} created' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }

  static async createProcDropConstraints(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const script = `
      CREATE OR ALTER PROCEDURE
      ${this._mainSchema}.${this._dropConstraintsProc} (@SchemaName NVARCHAR(50), @TableName NVARCHAR(50))
      AS
      BEGIN
        DECLARE @sql NVARCHAR(MAX) = N''
        DECLARE @FullName nvarchar(256) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

      -- SELECT Foreign Keys, Default Constraints, Check Constraints, Primary Keys, Unique Constraints
      DECLARE @KeyName nvarchar(50)
      DECLARE fkeys CURSOR FOR
      SELECT name FROM sys.foreign_keys
      WHERE parent_object_id = OBJECT_ID(@FullName)
      UNION SELECT name FROM sys.default_constraints
      WHERE parent_object_id = OBJECT_ID(@FullName)
      UNION SELECT name FROM sys.check_constraints
      WHERE parent_object_id = OBJECT_ID(@FullName)
      UNION SELECT name FROM sys.key_constraints
      WHERE parent_object_id = OBJECT_ID(@FullName)
      OPEN fkeys
      FETCH NEXT FROM fkeys INTO @KeyName

      WHILE @@FETCH_STATUS = 0
        BEGIN
        SET @SQL = 'ALTER TABLE ' + @FullName + ' DROP CONSTRAINT [' + @KeyName + '];'
        EXEC sp_executesql @SQL;
        FETCH NEXT FROM fkeys INTO @KeyName
        END

      CLOSE fkeys
      DEALLOCATE fkeys

	END
      GO --REM
      SELECT 'Procedure ${this._dropConstraintsProc} for ${this._mainSchema} created' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }

  static async createContentTypeProc(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const basicStatements = new SqlStatements();
    const sourceTable = basicStatements.addTableSuffix('tableCfgs');
    const resultCol = basicStatements.addColumnSuffix('type');
    const script = `
      CREATE OR ALTER PROCEDURE
      ${this._mainSchema}.${this._contentTypeProc} (@schemaName NVARCHAR(256), @tableKey NVARCHAR(256))
      AS
      BEGIN
        DECLARE @SQL NVARCHAR(MAX) = N''
        SELECT @SQL += 'SELECT ${resultCol} FROM [' + @schemaName + '].[${sourceTable}] WHERE key_col = @tableKey;'
        EXEC sp_executesql @SQL, N'@tableKey NVARCHAR(256)', @tableKey
      END
      GO --REM
      SELECT 'Procedure ${this._contentTypeProc} for ${this._mainSchema} created' AS Status;
    `;
    return await runScript(adminConfig, script, dbName);
  }

  static async contentType(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    tableName: string,
  ): Promise<ContentType> {
    const script = `EXEC ${this._mainSchema}.${this._contentTypeProc} @schemaName = '${schemaName}', @tableKey = '${tableName}'`;
    const result = await runScript(adminConfig, script, dbName);

    if (result.length === 0) {
      throw new Error(`Table "${tableName}" not found`);
    }
    return result as unknown as ContentType;
  }

  //***user procedures  */

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
    dbName: string,
    loginName: string,
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
      ALTER ROLE db_datareader ADD MEMBER [${userName}];
      ALTER ROLE db_datawriter ADD MEMBER [${userName}];
      ALTER ROLE db_ddladmin ADD MEMBER [${userName}];
      GRANT ALTER ON SCHEMA:: [${schemaName}] TO [${userName}];
      GRANT EXECUTE ON SCHEMA:: [main] TO [${userName}];
      SELECT 'USER [${userName}] CREATED' AS Status;
      END
      GO --REM`;

    return await runScript(adminConfig, script, dbName);
  }

  static async dropUser(
    adminConfig: sql.config,
    dbName: string,
    userName: string,
  ) {
    const script = `IF EXISTS (SELECT name FROM sys.database_principals WHERE name = N'${userName}')
    BEGIN
      DROP USER [${userName}];
      SELECT 'USER [${userName}] DROPPED' AS Status;
    END
    ELSE
    BEGIN
      SELECT 'USER [${userName}] DOES NOT EXIST' AS Status;
    END`;

    return await runScript(adminConfig, script, dbName);
  }
  static async addUserToRole(
    adminConfig: sql.config,
    dbName: string,
    roleName: string,
    userName: string,
  ) {
    const script = `ALTER ROLE [${roleName}] ADD MEMBER [${userName}]`;
    return await runScript(adminConfig, script, dbName);
  }

  static async grantSchemaPermission(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    userName: string,
  ) {
    const script = `GRANT ALTER ON SCHEMA:: [${schemaName}] TO [${userName}]`;
    return await runScript(adminConfig, script, dbName);
  }

  static async getUsers(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `SELECT name FROM sys.database_principals
                  WHERE type = 'S'
                  AND default_schema_name = '${schemaName}'
                  AND name NOT LIKE '##%';
            `;
    return await runScript(adminConfig, script, dbName);
  }

  static async getTableNames(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<string[]> {
    const script = `
      SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_TYPE = 'BASE TABLE';
    `;
    const result = await runScript(adminConfig, script, dbName);
    const tableNames: string[] = [];
    for (const row of result) {
      if (JSON.parse(row).TABLE_NAME) {
        tableNames.push(JSON.parse(row).TABLE_NAME);
      }
    }
    return tableNames;
  }

  //Transaction handling
  static async transact(
    adminConfig: sql.config,
    dbName: string,
    type: 'begin' | 'commit' | 'rollback',
    transactionName: string,
  ): Promise<string[]> {
    const script = `${type} TRANSACTION ${transactionName};`;
    return await runScript(adminConfig, script, dbName);
  }

  //****Compilations */
  /// Initialize the database including scripts
  static async initDb(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    loginName: string,
    loginPassword: string,
  ) {
    await this.createDatabase(adminConfig, dbName);
    await this.useDatabase(adminConfig, dbName);
    await this.createSchema(adminConfig, dbName, this._mainSchema);
    await this.createSchema(adminConfig, dbName, schemaName);
    await this.createLogin(adminConfig, dbName, loginName, loginPassword);
    await this.createUser(
      adminConfig,
      dbName,
      schemaName,
      loginName,
      loginName,
    );
    await DbBasics.installProcedures(adminConfig, dbName);
  }

  static async installProcedures(
    adminConfig: sql.config,
    dbName: string,
  ): Promise<string[]> {
    const dropLoginsProc = await this.createProcDropLogins(adminConfig, dbName);
    const dropObjectsProc = await this.createProcDropObjects(
      adminConfig,
      dbName,
    );
    const dropSchemaProc = await this.createProcDropSchema(adminConfig, dbName);
    const dropConstraintsProc = await this.createProcDropConstraints(
      adminConfig,
      dbName,
    );
    const contentTypeProc = await this.createContentTypeProc(
      adminConfig,
      dbName,
    );

    return [
      ...dropLoginsProc,
      ...dropObjectsProc,
      ...dropSchemaProc,
      ...dropConstraintsProc,
      ...contentTypeProc,
    ];
  }

  static async dropProcedures(adminConfig: sql.config, dbName: string) {
    const procedureNames = [
      this._dropConstraintsProc,
      this._dropObjectsProc,
      this._dropLoginsProc,
      this._dropSchemaProc,
    ];
    for (const proc of procedureNames) {
      const script = `DROP PROCEDURE IF EXISTS [${this._mainSchema}].[${proc}]`;
      await runScript(adminConfig, script, dbName);
    }
  }

  static async dropConstraints(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
    tableName: string,
  ) {
    const script = `EXEC ${this._mainSchema}.${this._dropConstraintsProc} @SchemaName = N'${schemaName}', @TableName = N'${tableName}'`;
    await runScript(adminConfig, script, dbName);
  }

  static async dropUsers(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ) {
    const users = await this.getUsers(adminConfig, dbName, schemaName);
    for (const user of users) {
      const userName = JSON.parse(user).name;

      const script = `USE [${dbName}];
      GO
      DROP USER [${userName}];
      GO
      USE [master];
      GO
      DROP LOGIN [${userName}];`;
      await runScript(adminConfig, script, dbName);
    }
  }

  static async dropTables(
    adminConfig: sql.config,
    dbName: string,
    schemaName: string,
  ): Promise<void> {
    const tables = await this.getTableNames(adminConfig, dbName, schemaName);
    for (const tableName of tables) {
      await this.dropConstraints(adminConfig, dbName, schemaName, tableName);
      const script = `DROP TABLE IF EXISTS [${schemaName}].[${tableName}]`;
      await runScript(adminConfig, script, dbName);
    }
  }
}
