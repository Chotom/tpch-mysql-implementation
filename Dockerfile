FROM mysql:latest
#RUN apt-get update && apt-get install -y libmagickwand-dev no-install-recommends && rm -rf /var/lib/apt/lists/*
#RUN printf "\n" | pecl install imagick
#RUN docker-php-ext-enable imagick

RUN echo "INSTALLING git, make, gcc"

RUN true \
    && apt-get -y update \
    && apt-get -y install git make gcc \

#RUN echo "CLONING tpch-dbgen"
#
#RUN git clone https://github.com/electrum/tpch-dbgen
#
#RUN echo "COPYING scripts"
#
#COPY ./dbgen_changed/ /tpch-dbgen/
#
#RUN echo "COMPILING tpch-dbgen"
#
#RUN cd tpch-dbgen; make

#ENTRYPOINT ["docker-entrypoint.sh"]
#CMD ["mysqld"]

#CMD ["/tpch-dbgen/generate_db.sh"]

#ADD create_table.sql /docker-entrypoint-initdb.d/
#ADD generate_data.sh /docker-entrypoint-initdb.d/
#ADD *.ctl /load_files/