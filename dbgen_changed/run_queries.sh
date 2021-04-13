#!/bin/bash

for i in {1..22}; do
    mysql -uroot -p1234 -DTPCD < db_queries/$i.sql
done