set -e
# Clean out the .pyc files so that we don't get false positives when we refactor code
find . -name \*.pyc | xargs rm
PYTHONPATH=. python -m unittest discover -s emission/tests -p Test*;
