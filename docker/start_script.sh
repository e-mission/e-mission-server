#!/usr/bin/env bash
#Configure web server

#set database URL using environment variables
echo ${DB_URL}
if [ -z ${DB_URL} ] ; then
    local_host=`hostname -i`
    sed "s_localhost_${local_host}_" conf/storage/db.conf.sample > conf/storage/db.conf
else
    sed "s_localhost_${DB_URL}_" conf/storage/db.conf.sample > conf/storage/db.conf
fi
cat conf/storage/db.conf

echo ${WEB_SERVER_HOST}
if [ -z ${WEB_SERVER_HOST}} ] ; then
    local_host=`hostname -i`
    sed "s_localhost_${local_host}_" conf/net/api/webserver.conf.sample > conf/net/api/webserver.conf
else
    sed "s_localhost_${WEB_SERVER_HOST}_" conf/net/api/webserver.conf.sample > conf/net/api/webserver.conf
fi
cat conf/net/api/webserver.conf
# change python environment
source activate emission


# launch the webapp
./e-mission-py.bash emission/net/api/cfc_webapp.py

