#!/bin/bash

echo "Removing environment from "${CONDA_TEMP_PREFIX}
conda deactivate

conda env remove --yes --name emissiontest
# rm conf/net/ext_service/habitica.json
# rm conf/net/ext_service/push.json
# rm seed_model.json
