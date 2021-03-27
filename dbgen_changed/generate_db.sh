#!/bin/bash
echo "start generating data..."

cd /tpch-dbgen

echo "run dss.dll..."
mysql -uroot -p1234 < dss.ddl

echo "run dss.ri..."
mysql -uroot -p1234 < dss.ri

echo "run load.sh..."
#set global local_infile=true;
mysql -p1234 -D TPCD -e "SET GLOBAL local_infile=true"
./load.sh

echo "loading data..."
mysql -uroot -p1234 --local-infile < loaddata.sql

echo "script ended successfully."