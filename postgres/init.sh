#!/bin/bash

createdb dvdrental
pg_restore -Ft -d dvdrental -U postgres /docker-entrypoint-initdb.d/dvdrental.tar
