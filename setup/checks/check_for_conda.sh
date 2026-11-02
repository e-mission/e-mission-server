source setup/export_versions.sh
CURR_CONDA_VER=`conda list -n base conda --fields name,version,build_number | awk '$1 == "conda" { print $2 "-" $3 }'`

if [ $CURR_CONDA_VER == $EXP_CONDA_VER ]; then
    echo "For conda, found $CURR_CONDA_VER, expected $EXP_CONDA_VER, all is good!"
else
    echo "For conda, found $CURR_CONDA_VER, expected $EXP_CONDA_VER, run 'bash setup/setup_conda.sh' to get the correct version"
    echo "Or install manually after downloading from https://repo.anaconda.com/miniconda/"
fi

