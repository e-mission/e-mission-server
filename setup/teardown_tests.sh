#!/bin/bash

echo "Removing environment from "${CONDA_TEMP_PREFIX}
conda deactivate ${CONDA_TEMP_PREFIX}
conda env remove --yes --prefix ${CONDA_TEMP_PREFIX} 
unset CONDA_TEMP_PREFIX
# rm conf/net/ext_service/habitica.json
rm conf/net/ext_service/push.json
rm seed_model.json
