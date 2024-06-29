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

RESTORE DATABASE [AdventureWorksDW2022]
FROM DISK = '/usr/src/app/AdventureWorksDW2022.bak'
WITH
    MOVE 'AdventureWorksDW2022' TO '/var/opt/mssql/data/AdventureWorksDW2022_Data.mdf',
    MOVE 'AdventureWorksDW2022_log' TO '/var/opt/mssql/data/AdventureWorksDW2022_log.ldf',
    FILE = 1,
    NOUNLOAD,
    STATS = 5;
GO

CREATE LOGIN [$(ETL_USER)] WITH PASSWORD='$(ETL_PASS)', CHECK_EXPIRATION = OFF, CHECK_POLICY = OFF;
GO
GRANT CONNECT SQL TO [$(ETL_USER)]
GO

USE [AdventureWorks2022]
GO
CREATE USER [$(ETL_USER)] FOR LOGIN [$(ETL_USER)]
GO
GRANT CONNECT TO [$(ETL_USER)]
GO
GRANT SELECT TO [$(ETL_USER)]
GO

USE [AdventureWorksDW2022]
GO
CREATE USER [$(ETL_USER)] FOR LOGIN [$(ETL_USER)]
GO
GRANT CONNECT TO [$(ETL_USER)]
GO
GRANT SELECT TO [$(ETL_USER)]
GO

EXIT
