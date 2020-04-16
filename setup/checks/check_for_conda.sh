CURR_CONDA_VER=`conda --version | cut -d " " -f 2`
EXP_CONDA_VER=4.5.12

if [ $CURR_CONDA_VER == $EXP_CONDA_VER ]; then
    echo "For conda, found $CURR_CONDA_VER, expected $EXP_CONDA_VER, all is good!"
else
    echo "For conda, found $CURR_CONDA_VER, expected $EXP_CONDA_VER, run 'bash setup/setup_conda.sh' to get the correct version"
fi

