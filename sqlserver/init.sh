for i in {1..99}; do
  /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d master -i init.sql
  if [ $? -eq 0 ]; then
    echo "initialization completed"
    break
  else
    echo "not ready yet..."
    sleep 1
  fi
done
