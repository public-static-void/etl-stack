for i in {1..60}; do
  /opt/mssql-tools/bin/sqlcmd -S localhost -U "${SA_USER}" -P "${SA_PASSWORD}" -d master -i init.sql
  if [ $? -eq 0 ]; then
    echo "*************************"
    echo "initialization completed"
    echo "*************************"
    break
  else
    echo "*************************"
    echo "not ready yet..."
    echo "*************************"
    sleep 5
  fi
done
