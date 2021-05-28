#!/bin/bash
SCALE_FACTOR=0.1
UPDATES=1

cd /tpch_tools/dbgen
./dbgen -v -U $UPDATES -s $SCALE_FACTOR
mkdir /db_refresh_data -v
mv ./*.tbl.u* /db_refresh_data
mv ./delete.* /db_refresh_data