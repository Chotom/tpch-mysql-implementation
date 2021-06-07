#!/bin/bash
SCALE_FACTOR=1

cd /tpch_tools/dbgen

echo "Compiling dbgen, qgen"
make

echo "Start generating data..."
./dbgen -vf -s $SCALE_FACTOR
mkdir /db_data -v
mv ./*.tbl /db_data

echo "Create tables in $MYSQL_HOST..."
mysql -h $MYSQL_HOST -P $PORT -uroot -p1234 < dss.ddl

echo "Create indexes and relations..."
mysql -h $MYSQL_HOST -P $PORT -uroot -p1234 < dss.ri

echo "Loading data..."
mysql -h $MYSQL_HOST -P $PORT -p1234 -D tpcd -e "SET GLOBAL local_infile=true"
mysql -h $MYSQL_HOST -P $PORT -uroot -p1234 --local-infile < loaddata.sql

echo "script ended successfully."