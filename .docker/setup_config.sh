set -e
echo "About to start conda update, this may take some time..."
source setup/setup_conda.sh Linux-x86_64
## The base environment sometimes has deprecated packages.
## Even if we upgrade the package list for the emission environment, it will not
## modify the base environment, which will still trip up the checker.
## Let's just upgrade the base environment to the latest everything without pinning
## this won't cause a regression in our code since we pin the versions that we use anyway
conda update -c conda-forge --all

# now install the emission environment
source setup/setup.sh

## Remove the old, unused packages to avoid tripping up the checker
## This is an example in case we need to remove them again
# rm -rf /root/miniconda-23.1.0/pkgs/cryptography-38.0.4-py39h9ce1e76_0
rm -rf /root/miniconda-25.11.1/pkgs/conda-25.11.1-py312hca03da5_0/lib/python3.12/site-packages/tests
rm -rf /root/miniconda-25.11.1/lib/python3.12/site-packages/tests

# Clean up the conda install
conda clean -t
find /root/miniconda-*/pkgs -wholename \*info/test\* -type d | xargs rm -rf
find ~/miniconda-25.1.1 -name \*tests\* -path '*/site-packages/*' | grep ".*/site-packages/tests" | xargs rm -rf

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
