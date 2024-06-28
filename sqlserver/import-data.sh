for i in {1..99}; do
  /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d master -i setup.sql
  if [ $? -eq 0 ]; then
    echo "setup.sql completed"
    break
  else
    echo "not ready yet..."
    sleep 1
  fi
done
