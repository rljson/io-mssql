USE [CDM-Test]
GO

/****** Object:  StoredProcedure [PantrySchema].[DropCurrentSchema]    Script Date: 28.08.2025 13:53:10 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   PROCEDURE 
[PantrySchema].[DropCurrentSchema] (@SchemaName NVARCHAR(50)) 
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
GO

