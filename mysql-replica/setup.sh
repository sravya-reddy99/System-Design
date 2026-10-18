#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "Starting the primary and the replica..."
docker compose up -d

echo "Waiting for both to be ready..."
for name in mysql-primary mysql-replica; do
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$name")" = "healthy" ]; do
    sleep 2
  done
done

echo "Creating the replication user and demo table on the primary..."
docker exec -i -e MYSQL_PWD=rootpass mysql-primary mysql -uroot < primary-setup.sql

echo "Pointing the replica at the primary..."
docker exec -i -e MYSQL_PWD=rootpass mysql-replica mysql -uroot < replica-setup.sql

sleep 3
echo
echo "Replica status:"
docker exec -i -e MYSQL_PWD=rootpass mysql-replica mysql -uroot -e "SHOW REPLICA STATUS\G" \
  | grep -E "Replica_IO_Running|Replica_SQL_Running|Seconds_Behind_Source|Last_IO_Error:|Last_SQL_Error:"
echo
echo "Done. Primary is on localhost:3306, replica is on localhost:3307."
