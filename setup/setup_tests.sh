#!/bin/bash 

export CONDA_TEMP_PREFIX=$(mktemp -d)
echo "Deploying environment in "${CONDA_TEMP_PREFIX}
echo "Make sure you are running this with conda >=4.2"
echo "Check with 'conda -V'"
echo "Upgrade with 'conda update conda' from the *root* environment"
conda env create --prefix ${CONDA_TEMP_PREFIX} --file setup/environment36.yml
source activate ${CONDA_TEMP_PREFIX}
python bin/deploy/habitica_conf.py
python bin/deploy/push_conf.py
python bin/deploy/model_copy.py
