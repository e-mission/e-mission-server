set -e
echo "About to start conda update, this may take some time..."
source setup/setup_conda.sh Linux-x86_64
# now install the emission environment
source setup/setup.sh

## Only in the docker environment, force upgrade the base image
## I tried to do this by just installing from the emission environment
## But that doesn't update all packages (e.g. cryptography=38 stays at that
## level instead of upgrading to cryptography=40)
## So we just manually upgrade the failing dependencies in the base image
## 
## 10/02 - Mukul
## - Above comments talk about manually updating cryptography to version 40
## - I have upgraded to 41.0.4 as per latest vulnerability fixes.
##
## 04/2025 - Shankari
## - The most recent version of anaconda has the correct version so we don't
##   need to override
# conda install -c conda-forge cryptography=42.0.0 wheel=0.40.0

## Remove the old, unused packages to avoid tripping up the checker
## This is an example in case we need to remove them again
# rm -rf /root/miniconda-23.1.0/pkgs/cryptography-38.0.4-py39h9ce1e76_0
rm -rf /root/miniconda-25.1.1/pkgs/conda-25.1.1-py312hca03da5_0/lib/python3.12/site-packages/tests
rm -rf /root/miniconda-25.1.1/lib/python3.12/site-packages/tests
rm -rf /root/miniconda-25.1.1/envs/emission/lib/python3.9/ensurepip/_bundled/setuptools-58.1.0-py3-none-any.whl

# Clean up the conda install
conda clean -t
find /root/miniconda-*/pkgs -wholename \*info/test\* -type d | xargs rm -rf
find ~/miniconda-25.1.1 -name \*tests\* -path '*/site-packages/*' | grep ".*/site-packages/tests" | xargs rm -rf

# Updating bash package to latest version manually 
apt-get update
apt-get install bash=5.1-6ubuntu1.1

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
