FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app
RUN chmod +x /usr/src/app/init.sh
RUN chmod +x /usr/src/app/init.sql
RUN chmod +x /usr/src/app/entrypoint.sh

EXPOSE 1433

USER mssql

ENTRYPOINT /bin/bash ./entrypoint.sh
