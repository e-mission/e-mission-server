#!/usr/bin/env bash

# Set up the test environment and run a specific integration test target.

set -euo pipefail

TEST_TARGET="${1:-}"

if [ -z "$TEST_TARGET" ]; then
	echo "Usage: $0 <test_target>"
	echo "Example: $0 emission/individual_tests/TestOverpass.py"
	exit 1
fi

echo "${DB_HOST:-}"

echo "Setting up conda..."
source setup/setup_conda.sh

echo "Setting up the test environment..."
source setup/setup_tests.sh

echo "Activating test environment..."
source setup/activate_tests.sh

echo "Running test target: $TEST_TARGET"
PYTHONPATH=. python -m unittest "$TEST_TARGET"
