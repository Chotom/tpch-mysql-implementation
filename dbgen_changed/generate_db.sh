#!/bin/bash
echo "start generating data..."

cd /tpch-dbgen
mysql -uroot -p1234 < dss.ddl
mysql -uroot -p1234 < dss.ri
./load.sh
mysql -uroot -p1234 < loaddata.sql

echo "script ended successfully."