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

export WEB_SERVER_HOST=0.0.0.0
cat conf/storage/db.conf

echo "Setting up conda..."
source setup/setup_conda.sh Linux-x86_64

echo "Setting up the test environment..."
source setup/setup_tests.sh

echo "Running tests..."
source setup/activate_tests.sh
./runAllTests.sh
# We don't need to teardown the tests in the docker version since we only install in the docker container
# source setup/teardown_tests.sh
