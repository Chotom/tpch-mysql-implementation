#!/bin/bash

cd /tpch-dbgen

echo "compiling dbgen, qgen"
make

echo "start generating data..."
./dbgen -s 0.1 -f
#chmod -R +755 /tpch-dbgen

mkdir db_data -v
mv ./*.tbl ./db_data

echo "run dss.dll..."
mysql -uroot -p1234 < dss.ddl

echo "run dss.ri..."
mysql -uroot -p1234 < dss.ri

echo "run load.sh..."
#set global local_infile=true;
mysql -p1234 -D tpcd -e "SET GLOBAL local_infile=true"

echo "loading data..."
mysql -uroot -p1234 --local-infile < loaddata.sql

echo "script ended successfully."