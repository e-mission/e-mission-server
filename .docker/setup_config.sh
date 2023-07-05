echo "About to start conda update, this may take some time..."
source setup/setup_conda.sh Linux-x86_64

echo "setup_conda.sh successful"

# now install the emission environment
source setup/setup.sh

echo "finished installing e-mission environment"

## Only in the docker environment, force upgrade the base image
## I tried to do this by just installing from the emission environment
## But that doesn't update all packages (e.g. cryptography=38 stays at that
## level instead of upgrading to cryptography=40)
## So we just manually upgrade the failing dependencies in the base image

set -eE

trap 'echo Something went wrong!' ERR 
conda install -c conda-forge cryptography=40.0.2 wheel=0.40.0 


echo "finished installing conda"


## Remove the old, unused packages to avoid tripping up the checker
rm -rf /root/miniconda-23.1.0/pkgs/cryptography-38.0.4-py39h9ce1e76_0
rm -rf /root/miniconda-23.1.0/pkgs/wheel-0.37.1-pyhd3eb1b0_0


# Clean up the conda install
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
