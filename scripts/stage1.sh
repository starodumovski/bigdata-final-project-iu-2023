#!/bin/bash

## Check and past two line into pg_hba.cong file for postgress
if ! grep -q 'local all all trust' /var/lib/pgsql/data/pg_hba.conf ; then
  sed -i '1s/^/local all all trust\n/' /var/lib/pgsql/data/pg_hba.conf
fi
if ! grep -q 'host all all 0.0.0.0\/0 trust' /var/lib/pgsql/data/pg_hba.conf ; then
  sed -i '1s/^/host all all 0.0.0.0\/0 trust\n/' /var/lib/pgsql/data/pg_hba.conf
fi

## Check f the file jar for postgres is downloaded, otherwse download
if [ ! -f /usr/hdp/current/sqoop-client/lib/postgresql-42.6.0.jar ]; then
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar --no-check-certificate; mv  postgresql-42.6.0.jar /usr/hdp/current/sqoop-client/lib/
fi
psql -U postgres -f ./sql/bd.sql

bash ./scripts/sqoop.sh

