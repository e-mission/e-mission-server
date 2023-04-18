#!/usr/bin/env bash
#Configure web server

# cd /usr/src/app/e-mission-server

#set database URL using environment variable
echo ${DB_HOST}
if [ -z ${DB_HOST} ] ; then
    local_host=`hostname -i`
    jq --arg db_host "$local_host" '.timeseries.url = $db_host' conf/storage/db.conf.sample > conf/storage/db.conf
else
    jq --arg db_host "$DB_HOST" '.timeseries.url = $db_host' conf/storage/db.conf.sample > conf/storage/db.conf
fi
cat conf/storage/db.conf

#set Web Server host using environment variable
echo ${WEB_SERVER_HOST}
if [ -z ${WEB_SERVER_HOST} ] ; then
    local_host=`hostname -i`
    sed "s_localhost_${local_host}_" conf/net/api/webserver.conf.sample > conf/net/api/webserver.conf
else
    sed "s_localhost_${WEB_SERVER_HOST}_" conf/net/api/webserver.conf.sample > conf/net/api/webserver.conf
fi
cat conf/net/api/webserver.conf

if [ -z ${LIVERELOAD_SRC} ] ; then
    echo "Live reload disabled, "
else
    echo "Enabling bottle live reload"
    ORIG="run.host=server_host"
    NEW="run(reloader=True,host=server_host"
    echo "Replacing $ORIG -> $NEW"
    sed -i -e "s|$ORIG|$NEW|g" /usr/src/app/e-mission-server/emission/net/api/cfc_webapp.py
fi

#TODO: start cron jobs
# change python environment
source setup/activate.sh

# launch the webapp
./e-mission-py.bash emission/net/api/cfc_webapp.py
