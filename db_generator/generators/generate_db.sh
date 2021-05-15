#!/bin/bash

cd /tpch-dbgen

echo "Compiling dbgen, qgen"
make

echo "Start generating data..."
#chmod -R +755 /tpch-dbgen
./dbgen -s 0.1 -f
mkdir db_data -v
mv ./*.tbl ./db_data

echo "Create tables..."
mysql -uroot -p1234 < dss.ddl

echo "Create indexes and relations..."
mysql -uroot -p1234 < dss.ri

echo "Loading data..."
#set global local_infile=true;
mysql -p1234 -D tpcd -e "SET GLOBAL local_infile=true"
mysql -uroot -p1234 --local-infile < loaddata.sql

echo "script ended successfully."