if [[ ! -f setup/export_versions.sh ]]; then
    echo "Error: setup/export_versions.sh file is missing. Please ensure it exists before running this script."
    exit 1
fi

source setup/export_versions.sh
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to source setup/export_versions.sh. Please check the file for errors."
    exit 1
fi

if [[ -z $EXP_CONDA_VER ]]; then
    echo "Error: Requires the EXP_CONDA_VER variable to be set"
    echo "Please check the 'export_versions.sh' script and ensure that it works"
    exit 1
fi

PLATFORM=$1
INSTALL_PREFIX=$2

if [[ -z $PLATFORM ]]; then
    echo "Usage: setup_conda.sh <platform> [install_prefix]"
    echo "   Platform options are Linux-x86_64, MacOSX-x86_64, MacOSX-arm64"
    WINDOWS_INSTALLER_URL="https://repo.anaconda.com/miniconda/Miniconda3-py39_$EXP_CONDA_VER-$EXP_CONDA_VER_SUFFIX-Windows-x86_64.exe"
    echo "   For Windows, manually download and install $WINDOWS_INSTALLER_URL"
    exit 2
fi

if [[ -z $INSTALL_PREFIX ]]; then
    INSTALL_PREFIX=$HOME/miniconda-$EXP_CONDA_VER
fi
SOURCE_SCRIPT="$INSTALL_PREFIX/etc/profile.d/conda.sh"

echo "Installing for version $EXP_CONDA_VER and platform $PLATFORM at $INSTALL_PREFIX"
curl -o miniconda.sh -L https://repo.anaconda.com/miniconda/Miniconda3-py312_$EXP_CONDA_VER-$EXP_CONDA_VER_SUFFIX-$PLATFORM.sh
bash miniconda.sh -b -p $INSTALL_PREFIX
source $SOURCE_SCRIPT
hash -r
conda install -n base conda-libmamba-solver
conda config --set solver libmamba
conda config --set always_yes yes
# Useful for debugging any issues with conda
conda info -a
echo "Successfully installed at $INSTALL_PREFIX. Please activate with 'source setup/activate_conda.sh' in every terminal where you want to use conda" 
