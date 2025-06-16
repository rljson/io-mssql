use [CDM-Test]
go



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
	PRINT @LoginName
    --SET @sql = N'DROP LOGIN [' + @LoginName + ']'
	PRINT @sql
    --EXEC sp_executesql @sql
    FETCH NEXT FROM LoginCursor INTO @LoginName
  END

  CLOSE LoginCursor
  DEALLOCATE LoginCursor

  END
  GO


  exec PantrySchema.DropAllPantryLogins
  GO

  create or alter procedure PantrySchema.xx
  AS
	BEGIN
		--DECLARE test_logins CURSOR FOR
		 SELECT name FROM sys.server_principals
		 --OPEN test_logins
		 --DECLARE @login nvarchar(50)
		 --FETCH NEXT FROM test_logins INTO @login

	END
GO

	exec PantrySchema.xx





CREATE OR ALTER PROCEDURE PantrySchema.DropAllPantryUsers
AS
BEGIN
  DECLARE @UserName nvarchar(128)
  DECLARE @sql NVARCHAR(MAX)

 
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
    SET @sql = N'DROP USER [' + @UserName + ']'
    EXEC sp_executesql @sql
    FETCH NEXT FROM UserCursor INTO @UserName
  END

  CLOSE UserCursor
  DEALLOCATE UserCursor
END
GO --REM


exec PantrySchema.DropAllPantryUsers