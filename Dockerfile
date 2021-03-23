FROM mysql:latest
#RUN apt-get update && apt-get install -y libmagickwand-dev --no-install-recommends && rm -rf /var/lib/apt/lists/*
#RUN printf "\n" | pecl install imagick
#RUN docker-php-ext-enable imagick

RUN apt-get update -y
RUN apt-get install -y git make gcc

RUN git clone https://github.com/electrum/tpch-dbgen

COPY ./dbgen_changed/ /tpch-dbgen/

RUN cd tpch-dbgen; make
RUN cd tpch-dbgen; ./dbgen -s 1 -f

#RUN cd tpch-dbgen; mysql -uroot -p1234 < dss.ddl
#RUN cd tpch-dbgen; mysql -uroot -p1234 < dss.ri
#RUN cd tpch-dbgen; ./load.sh
#RUN cd tpch-dbgen; mysql -uroot -p1234 < loaddata.sql

#ADD create_table.sql /docker-entrypoint-initdb.d/
#ADD generate_data.sh /docker-entrypoint-initdb.d/
#
#ADD *.ctl /load_files/