echo "About to start conda update, this may take some time..."
source setup/setup_conda.sh Linux-x86_64
source setup/setup.sh
conda clean -t
conda clean -p

if [ -d "webapp/www/" ]; then
    cp /index.html webapp/www/index.html
fi

if [ -d "/conf" ]; then
    echo "Found configuration, overriding..."
    cp -r /conf/* conf/
fi

if [ -z ${LIVERELOAD_SRC} ] ; then
    echo "Live reload disabled, "
else
    echo "Enabling bottle live reload"
    ORIG="run.host=server_host"
    NEW="run(reloader=True,host=server_host"
    echo "Replacing $ORIG -> $NEW"
    sed -i -e "s|$ORIG|$NEW|g" emission/net/api/cfc_webapp.py
fi