USE [CDM-Test]
GO

/****** Object:  StoredProcedure [PantrySchema].[DropAllPantryObjects]    Script Date: 28.08.2025 13:52:07 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   PROCEDURE 
[PantrySchema].[DropAllPantryObjects] 
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
GO

