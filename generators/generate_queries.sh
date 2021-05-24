#!/bin/bash
mkdir db_queries -v
cd /tpch_tools/dbgen

for q in {1..22}; do
    DSS_QUERY=./queries ./qgen $q > /db_queries/$q.sql;
done