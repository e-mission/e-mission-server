#!/usr/bin/env bash

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
source setup/activate.sh

# launch the webapp
./e-mission-py.bash emission/net/api/cfc_webapp.py
