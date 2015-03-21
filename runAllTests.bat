pushd "%cd%"/CFC_WebApp 
cp config.json.sample keys.json 
cp keys.json.sample keys.json 
py -m unittest discover -s tests -p Test || goto :error
popd

pushd "%cd%"/CFC_DataCollector
cp ../CFC_WebApp/config.json config.json
cp ../CFC_WebApp/keys.json.sample keys.json
set PYTHONPATH=. py
py -m unittest discover -s tests -p Test* || goto :error
popd

:error
	exit /b %errorlevel%