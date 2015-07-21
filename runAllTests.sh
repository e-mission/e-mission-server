set -e
PYTHONPATH=. python -m unittest discover -s emission/tests -p Test*;
