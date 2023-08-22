set -e
# PYTHONPATH=. python -m unittest discover -s emission/integrationTests -p Test*;
lsof -i :8080
PYTHONPATH=. python -m unittest emission/individual_tests/TestNominatim.py
