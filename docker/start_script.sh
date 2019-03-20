#!/usr/bin/env bash
#Configure web server

#set database URL using environment variable
echo ${DB_HOST}
if [ -z ${DB_HOST} ] ; then
    local_host=`hostname -i`
    sed "s_localhost_${local_host}_" conf/storage/db.conf.sample > conf/storage/db.conf
else
    sed "s_localhost_${DB_HOST}_" conf/storage/db.conf.sample > conf/storage/db.conf
fi
cat conf/storage/db.conf

#set Web Server host using environment variable
echo ${WEB_SERVER_HOST}
if [ -z ${WEB_SERVER_HOST}} ] ; then
    local_host=`hostname -i`
    sed "s_localhost_${local_host}_" conf/net/api/webserver.conf.sample > conf/net/api/webserver.conf
else
    sed "s_localhost_${WEB_SERVER_HOST}_" conf/net/api/webserver.conf.sample > conf/net/api/webserver.conf
fi
cat conf/net/api/webserver.conf

#TODO: start cron jobs
# change python environment
source activate emission

# launch the webapp
./e-mission-py.bash emission/net/api/cfc_webapp.py
