FROM postgres:15 as basepg

ENV POSTGRES_PASSWORD docker
ENV POSTGRES_DB world
COPY init.sql /docker-entrypoint-initdb.d/

RUN apt-get update && apt-get install -y  wget \
                       build-essential \
                       postgresql-server-dev-15 \
                       postgresql-plpython3-15 \
                       postgresql-contrib-15 \
                       openssl \
                       libssl-dev \
                       libboost-all-dev \
                       m4 \
                       wget \
                       vim \
                       pgxnclient \
                       flex \
                       bison \
                       graphviz \
                       pgxnclient \
                       cmake \
                       g++ \                       
                       software-properties-common
                       
RUN apt-get -y install git
RUN apt-get -y install python2
RUN git clone https://github.com/apache/madlib
RUN mkdir madlib/build 
WORKDIR madlib/build
RUN cmake .. 
RUN make -j8 
#RUN src/bin/madpack -p postgres -c postgres/postgres@localhost:5432/postgres install

# RUN add-apt-repository ppa:timescale/timescaledb-ppa
# RUN apt update


# FROM basepg
# # Install MADLib
# RUN pgxnclient install madlib
# RUN pgxnclient load madlib 


# # Include the TimescaleDB PPA



# # Install the TimescaleDB
# RUN apt install timescaledb-postgresql-15
# RUN timescaledb-tune --quiet --yes



# # Install CStore
# RUN apt-get install bison flex git libreadline-dev libz-dev git libpq-dev libprotobuf-c0-dev make protobuf-c-compiler
# RUN pgxn install cstore_fdw
# RUN pgxn load cstore_fdw