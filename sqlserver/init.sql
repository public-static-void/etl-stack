USE [master]
GO
RESTORE DATABASE [AdventureWorks2022]
FROM DISK = '/usr/src/app/AdventureWorks2022.bak'
WITH
    MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks2022_Data.mdf',
    MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks2022_log.ldf',
    FILE = 1,
    NOUNLOAD,
    STATS = 5;
GO

USE [AdventureWorks2022]
CREATE LOGIN [etl] WITH PASSWORD='demopass', DEFAULT_DATABASE=[AdventureWorks2022], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;
GO
CREATE USER [etl] FOR LOGIN [etl]
GO
ALTER ROLE [db_datareader] ADD MEMBER [etl]
GO
USE [master]
GO
GRANT CONNECT SQL TO [etl]
GO
