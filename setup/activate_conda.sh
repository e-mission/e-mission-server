source setup/export_versions.sh

INSTALL_PREFIX=$HOME/miniforge-$EXP_CONDA_VER
SOURCE_SCRIPT="$HOME/miniforge-$EXP_CONDA_VER/etc/profile.d/conda.sh"

echo "Activating conda at ${SOURCE_SCRIPT}"
source $SOURCE_SCRIPT
