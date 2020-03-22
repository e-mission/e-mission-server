# If the conda binary is not found, specify the full path to it
# you can find it by searching for "conda" under the miniconda3 directory
# typical paths are:
# - on linux: /home/<user>/miniconda3/bin/conda
# - on OSX: /Users/<user>/miniconda3/bin/conda
# - on Windows: C:/Users/<user>/Miniconda3/Scripts/conda

echo "Setting up blank environment"
conda create --name emission python=3.6
conda activate emission

echo "Downloading packages"
curl -o /tmp/cachetools-2.1.0-py_0.tar.bz2 -L https://anaconda.org/conda-forge/cachetools/2.1.0/download/noarch/cachetools-2.1.0-py_0.tar.bz2
curl -o /tmp/geojson-2.3.0-py_0.tar.bz2 -L https://anaconda.org/conda-forge/geojson/2.3.0/download/noarch/geojson-2.3.0-py_0.tar.bz2
curl -o /tmp/jsonpickle-0.9.6-py_1.tar.bz2 -L https://anaconda.org/conda-forge/jsonpickle/0.9.6/download/noarch/jsonpickle-0.9.6-py_1.tar.bz2
curl -o /tmp/more-itertools-8.2.0-py_0.tar.bz2 -L https://anaconda.org/conda-forge/more-itertools/8.2.0/download/noarch/more-itertools-8.2.0-py_0.tar.bz2
curl -o /tmp/pyasn1-0.4.8-py_0.tar.bz2 -L https://anaconda.org/conda-forge/pyasn1/0.4.8/download/noarch/pyasn1-0.4.8-py_0.tar.bz2
curl -o /tmp/pyasn1-modules-0.2.7-py_0.tar.bz2 -L https://anaconda.org/conda-forge/pyasn1-modules/0.2.7/download/noarch/pyasn1-modules-0.2.7-py_0.tar.bz2

echo "Installing manually downloaded packages"
conda install /tmp/*.bz2

echo "Updating using conda now"
conda env update --name emission --file setup/environment36.nomkl.yml
