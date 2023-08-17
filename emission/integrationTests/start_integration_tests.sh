# Run the tests in the docker environment
# Using an automated install 
cd /src/e-mission-server

echo "++++++++++"
echo "$PWD"


#set database URL using environment variable
echo ${DB_HOST}
if [ -z ${DB_HOST} ] ; then
    local_host=`hostname -i`
    sed "s_localhost_${local_host}_" conf/storage/db.conf.sample > conf/storage/db.conf
else
    sed "s_localhost_${DB_HOST}_" conf/storage/db.conf.sample > conf/storage/db.conf
fi
cat conf/storage/db.conf

echo "Python path before setting up conda: $PYTHONPATH"
echo "Setting up conda..."
source setup/setup_conda.sh Linux-x86_64

echo "Python path after setup_conda: $PYTHONPATH"

echo "Setting up the test environment..."
source setup/setup_tests.sh

echo "Python path after setup_tests: $PYTHONPATH"

echo "Running tests..."
source setup/activate_tests.sh
echo "Python path after activate_tests: $PYTHONPATH"
# tail -f /dev/null
chmod +x runIntegrationTests.sh
./runIntegrationTests.sh