pushd $EMISSION_SERVER_HOME
source setup/checks/check_for_conda.sh
popd

conda env update --name emission-private-eval --file $EMISSION_SERVER_HOME/setup/environment36.yml
conda env update --name emission-private-eval --file $EMISSION_SERVER_HOME/setup/environment36.notebook.additions.yml
source activate emission-private-eval
cp -r $EMISSION_SERVER_HOME/conf .
