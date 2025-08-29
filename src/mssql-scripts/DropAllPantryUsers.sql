USE [CDM-Test]
GO

/****** Object:  StoredProcedure [PantrySchema].[DropAllPantryUsers]    Script Date: 28.08.2025 13:52:24 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   PROCEDURE [PantrySchema].[DropAllPantryUsers] 
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
GO

