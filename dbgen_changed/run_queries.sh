#!/bin/bash

for i in {1..22}; do
    mysql -u root -p password < dbgen/queries/query-$i.sql
done