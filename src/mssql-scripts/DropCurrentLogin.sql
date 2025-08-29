USE [CDM-Test]
GO

/****** Object:  StoredProcedure [PantrySchema].[DropCurrentLogin]    Script Date: 28.08.2025 13:52:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER   PROCEDURE [PantrySchema].[DropCurrentLogin] (@LoginName NVARCHAR(128)) 
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
GO

