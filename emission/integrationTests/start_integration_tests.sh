# Run the tests in the docker environment
# Using an automated install 
cd /src/e-mission-server

#set database URL using environment variable
echo ${DB_HOST}
if [ -z ${DB_HOST} ] ; then
    local_host=`hostname -i`
    export DB_HOST=$local_host
    echo "Setting db host environment variable to localhost"
fi
cat conf/storage/db.conf

echo "Setting up conda..."
source setup/setup_conda.sh Linux-x86_64

echo "Setting up the test environment..."
source setup/setup_tests.sh

echo "Running tests..."
source setup/activate_tests.sh

echo "Adding permissions for the runIntegrationTests.sh script"
chmod +x runIntegrationTests.sh
echo "Permissions added for the runIntegrationTests.sh script"

./runIntegrationTests.sh