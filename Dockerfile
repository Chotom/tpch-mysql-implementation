FROM ubuntu:20.04

RUN true \
    && apt-get -y update \
    && apt-get -y install git make gcc \
    && apt-get -y install mysql-client \
    && apt-get -y install python3

COPY generators /generators
COPY benchmark_cli /benchmark_cli
COPY tpch_tools /tpch_tools
COPY dbgen_mysql_patch /tpch_tools/dbgen