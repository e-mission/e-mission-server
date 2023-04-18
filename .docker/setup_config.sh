echo "About to start conda update, this may take some time..."
source setup/setup_conda.sh Linux-x86_64

## Only in the docker environment, update the base image to the emission versions as well
## So that during vulnerability checks, we will sink or swim together
## aka if there is a vulnerabity in the base image, it will also be in emission
## and can be updated at the same time.
conda install -c conda-forge cryptography=40.0.2 wheel=0.40.0
# conda env update -n base --file setup/environment36.yml

# now install the emission environment
source setup/setup.sh

conda clean -t
find /root/miniconda-*/pkgs -wholename \*info/test\* -type d | xargs rm -rf

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
