EXP_CONDA_VER=4.5.12

PLATFORM=$1
echo "Installing for platform $PLATFORM"
echo $?

if [ -z $PLATFORM ]; then
    echo "Usage: setup_conda.sh <platform>"
    echo "   Platform options are Linux-x86_64, MacOSX-x86_64"
    echo "   For Windows, manually download and install https://repo.anaconda.com/miniconda/Miniconda3-$EXP_CONDA_VER-Windows-x86_64.exe"
else
    curl -o miniconda.sh -L https://repo.continuum.io/miniconda/Miniconda3-$EXP_CONDA_VER-$PLATFORM.sh;
    echo $?
    bash miniconda.sh -b -p $HOME/miniconda
    echo $?
    source "$HOME/miniconda/etc/profile.d/conda.sh"
    echo $?
    hash -r
    echo $?
    conda config --set always_yes yes --set changeps1 no
    echo $?
    # Useful for debugging any issues with conda
    conda info -a
    echo $?
fi
