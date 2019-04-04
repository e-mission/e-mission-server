set -e
# Clean out the .pyc files so that we don't get false positives when we refactor code
# find . -name \*.pyc | xargs rm
# We can't do this - it looks like the automated test environment doesn't support xargs
# Removing emission/tests/storageTests/__init__.pyc
# + ./runAllTests.sh
# rm: missing operand
# Try `rm --help' for more information.
PYTHONPATH=. python -m unittest discover -s emission/individual_tests -p Test*;
