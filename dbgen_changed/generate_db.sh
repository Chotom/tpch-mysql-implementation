#!/bin/bash
echo "start generating data..."
cd /tpch-dbgen

echo "run dss.dll..."
mysql -uroot -p1234 --local-infile < dss.ddl

echo "run dss.ri..."
mysql -uroot -p1234 --local-infile < dss.ri

echo "run load.sh..."
./load.sh

echo "loading data..."
mysql -uroot -p1234 --local-infile < loaddata.sql

echo "script ended successfully."
