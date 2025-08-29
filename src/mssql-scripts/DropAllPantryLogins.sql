

CREATE OR ALTER PROCEDURE [PantrySchema].[DropAllPantryLogins]
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
GO

