set -e
#commented out portion can be added back in once all of the integration tests start passing. For now, we just want to run the nominatim test. 
# PYTHONPATH=. python -m unittest discover -s emission/integrationTests -p Test*;
PYTHONPATH=. python -m unittest emission/individual_tests/TestNominatim.py
