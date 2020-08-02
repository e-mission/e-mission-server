source setup/export_versions.sh
CURR_CONDA_VER=`conda --version | cut -d " " -f 2`

if [ $CURR_CONDA_VER == $EXP_CONDA_VER ]; then
    echo "For conda, found $CURR_CONDA_VER, expected $EXP_CONDA_VER, all is good!"
else
    echo "For conda, found $CURR_CONDA_VER, expected $EXP_CONDA_VER, run 'bash setup/setup_conda.sh <platform>' to get the correct version"
    echo "Or install manually after downloading from https://repo.anaconda.com/miniconda/"
fi

