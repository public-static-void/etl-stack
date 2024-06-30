FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

RUN apt-get update && apt-get install -y gnupg gnupg2 gnupg1 curl apt-transport-https

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN curl -LJO https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks2022.bak
RUN curl -LJO https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorksDW2022.bak

COPY . /usr/src/app
RUN chmod +x /usr/src/app/init.sh
RUN chmod +x /usr/src/app/init.sql
RUN chmod +x /usr/src/app/entrypoint.sh



EXPOSE 1433

USER mssql

ENTRYPOINT /bin/bash ./entrypoint.sh
