#!/bin/bash 

source setup/checks/check_for_conda.sh

conda env update --name emissiontest --file setup/environment36.yml
# python bin/deploy/habitica_conf.py
python bin/deploy/push_conf.py
python bin/deploy/model_copy.py
