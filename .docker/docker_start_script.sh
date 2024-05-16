#!/usr/bin/env bash
#Configure web server

# cd /usr/src/app/e-mission-server

#set database URL using environment variable
echo ${DB_HOST}
if [ -z ${DB_HOST} ] ; then
    local_host=`hostname -i`
    export DB_HOST=$local_host
    echo "Setting db host environment variable to localhost"
fi
cat conf/storage/db.conf

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
echo "Starting up e-mission-environment..."
source setup/activate.sh

# launch the webapp
./e-mission-py.bash emission/net/api/cfc_webapp.py
