USE [CDM-Test]
GO --REM

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'PantrySchema')
BEGIN
    EXEC('CREATE SCHEMA PantrySchema');
END
GO --REM

CREATE OR ALTER PROCEDURE
PantrySchema.DropCurrentConstraints(@SchemaName NVARCHAR(50))
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
GO --REM

CREATE OR ALTER PROCEDURE
PantrySchema.DropCurrentSchema (@SchemaName NVARCHAR(50))
AS
BEGIN
PRINT @SchemaName
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
      SELECT @sql += 'DROP TABLE [' + s.name + '].[' + t.name + '];'
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


CREATE OR ALTER PROCEDURE
PantrySchema.DropAllPantryObjects
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
  -- The problem is that the END statement is missing before the GO batch separator at the end of the procedure.
  -- In T-SQL, each BEGIN must have a corresponding END. Without it, you'll get a syntax error.
  -- You should move END; before the GO statement, like this:

  END;

GO --REM

CREATE OR ALTER PROCEDURE PantrySchema.DropCurrentLogin (@LoginName NVARCHAR(128))
AS
BEGIN
  DECLARE @sql NVARCHAR(MAX)


IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @LoginName)
AND NOT EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE login_name = @LoginName)
BEGIN
  SET @sql = N'DROP LOGIN [' + @LoginName + ']'
  EXEC sp_executesql @sql
 END
END
GO --REM

CREATE OR ALTER PROCEDURE PantrySchema.DropAllPantryLogins
AS
  BEGIN

  DECLARE @LoginName nvarchar(128)
  DECLARE @sql NVARCHAR(MAX)

  DECLARE LoginCursor CURSOR FOR
    SELECT name FROM sys.server_principals
    WHERE type_desc = 'SQL_LOGIN' AND name LIKE 'login\_%' ESCAPE '\'

  OPEN LoginCursor
  FETCH NEXT FROM LoginCursor INTO @LoginName

  WHILE @@FETCH_STATUS = 0
  BEGIN
    EXEC PantrySchema.DropCurrentLogin @LoginName
    FETCH NEXT FROM LoginCursor INTO @LoginName
  END

  CLOSE LoginCursor
  DEALLOCATE LoginCursor

  END

GO --REM
CREATE OR ALTER PROCEDURE PantrySchema.DropAllPantryUsers
AS
BEGIN
  DECLARE @UserName nvarchar(128)
  DECLARE @sql NVARCHAR(MAX)

  DECLARE UserCursor CURSOR FOR
    SELECT name FROM sys.database_principals
    WHERE type_desc = 'SQL_USER' AND name LIKE 'login\_%' ESCAPE '\'

  OPEN UserCursor
  FETCH NEXT FROM UserCursor INTO @UserName

  WHILE @@FETCH_STATUS = 0
  BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM sys.dm_exec_sessions
    WHERE login_name = @UserName
  )
  BEGIN
    SET @sql = N'DROP USER [' + @UserName + ']'
    EXEC sp_executesql @sql
  END
    FETCH NEXT FROM UserCursor INTO @UserName
  END

  CLOSE UserCursor
  DEALLOCATE UserCursor
END
GO --REM
