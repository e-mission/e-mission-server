pushd $PWD/CFC_WebApp
PYTHONPATH=. python -m unittest discover -s tests -p Test*
popd
pushd $PWD/CFC_DataCollector
PYTHONPATH=. python -m unittest discover -s tests -p Test*
popd

