USE [CDM-Test]
GO

/****** Object:  StoredProcedure [PantrySchema].[DropCurrentConstraints]    Script Date: 28.08.2025 13:52:39 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   PROCEDURE 
[PantrySchema].[DropCurrentConstraints](@SchemaName NVARCHAR(50)) 
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
GO

