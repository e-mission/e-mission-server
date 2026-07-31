#Set up the testing environment
# Using an automated install 

echo ${DB_HOST}

echo "Setting up conda..."
source setup/setup_conda.sh

echo "Setting up the test environment..."
source setup/setup_tests.sh

echo "Running tests..."
source setup/activate_tests.sh

set -e
PYTHONPATH=. python -m unittest emission/individual_tests/TestOverpass.py
