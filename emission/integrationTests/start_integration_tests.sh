# Run the tests in the docker environment
# Using an automated install 
cd /src/e-mission-server

echo ${DB_HOST}

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
