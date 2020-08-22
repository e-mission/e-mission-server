source setup/export_versions.sh

PLATFORM=$1
echo "Installing for version $EXP_CONDA_VER and platform $PLATFORM"

if [[ -z $EXP_CONDA_VER || -z $PLATFORM ]]; then
    echo "Usage: setup_conda.sh <platform>"
    echo "   Assumes that the EXP_CONDA_VER variable is set"
    echo "   Platform options are Linux-x86_64, MacOSX-x86_64"
    echo "   For Windows, manually download and install https://repo.anaconda.com/miniconda/Miniconda3-$EXP_CONDA_VER-Windows-x86_64.exe"
else
    INSTALL_PREFIX=$HOME/miniconda-$EXP_CONDA_VER
    SOURCE_SCRIPT="$HOME/miniconda-$EXP_CONDA_VER/etc/profile.d/conda.sh"
    
    curl -o miniconda.sh -L https://repo.continuum.io/miniconda/Miniconda3-py38_$EXP_CONDA_VER-$PLATFORM.sh;
    bash miniconda.sh -b -p $INSTALL_PREFIX
    source $SOURCE_SCRIPT
    hash -r
    conda config --set always_yes yes
    # Useful for debugging any issues with conda
    conda info -a
    echo "Successfully installed at $INSTALL_PREFIX. Please activate with 'source setup/activateXXX.sh' in every terminal where you want to use conda" 
fi
