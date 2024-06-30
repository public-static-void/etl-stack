# Modern ETL Stack Tutorial

## Moving data from SQLServer to Postgres using Airflow

### How to run:

##### Set up environment variables, such as:

    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=postgres
    POSTGRES_DB=postgres
    PG_PORT=5432

    PGADMIN_DEFAULT_EMAIL=pgadmin@pgadmin.com
    PGADMIN_DEFAULT_PASSWORD=pgadmin
    PGA_PORT=5050

    SA_USER=sa
    SA_PASSWORD=SQLserver22!
    SA_PORT=1433
    MSSQL_PID=Developer

    ETL_USER=etl
    ETL_PASS=ETLpass123!

    AIRFLOW_UID=1000
    AIRFLOW_PROJ_DIR=./airflow
    AIRFLOW_PORT=9099
    FLOWER_PORT=5559

###### Put them in an .env file in the root directory of the project, set them temporarily in the command line, e.g. export VAR=VAL, or prepend them directly to docker-compose, e.g. VAR=VAL docker-compose up.

##### Spin up the containers:

    docker-compose up -d --build

##### How to stop:

    docker-compose down

##### How to purge volumes:

    docker-compose down --volumes
