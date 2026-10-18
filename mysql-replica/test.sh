#!/usr/bin/env bash
set -uo pipefail

P="docker exec -i -e MYSQL_PWD=rootpass mysql-primary mysql -uroot"
R="docker exec -i -e MYSQL_PWD=rootpass mysql-replica mysql -uroot"

echo "1. Write to the primary"
$P -e "INSERT INTO shop.products (name, price) VALUES ('Notebook', 4.50), ('Pen', 1.20), ('Backpack', 39.00);" 2>/dev/null
echo "   inserted 3 products"

sleep 1

echo
echo "2. Read the same table from the replica"
$R -e "SELECT id, name, price FROM shop.products;" 2>/dev/null

echo
echo "3. Try to write to the replica (this should fail)"
if $R -e "INSERT INTO shop.products (name, price) VALUES ('Sneaky', 0);" 2>/dev/null; then
  echo "   UNEXPECTED: the write worked"
else
  echo "   write was refused, as expected"
fi

echo
echo "4. Replica health"
$R -e "SHOW REPLICA STATUS\G" 2>/dev/null \
  | grep -E "Replica_IO_Running|Replica_SQL_Running|Seconds_Behind_Source"
