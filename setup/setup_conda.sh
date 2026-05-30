if [[ ! -f setup/export_versions.sh ]]; then
    echo "Error: setup/export_versions.sh file is missing. Please ensure it exists before running this script."
    exit 1
fi

source setup/export_versions.sh
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to source setup/export_versions.sh. Please check the file for errors."
    exit 1
fi

if [[ -z $EXP_CONDA_VER ]]; then
    echo "Error: Requires the EXP_CONDA_VER variable to be set"
    echo "Please check the 'export_versions.sh' script and ensure that it works"
    exit 1
fi

INSTALL_PREFIX=$1
PLATFORM=""

if [[ $INSTALL_PREFIX == "-h" || $INSTALL_PREFIX == "--help" ]]; then
    echo "Usage: setup_conda.sh [install_prefix]"
    echo "   Platform is inferred automatically from uname"
    exit 0
fi

UNAME_S=$(uname -s)
UNAME_M=$(uname -m)
case "$UNAME_S:$UNAME_M" in
    Linux:x86_64)
        PLATFORM="Linux-x86_64"
        ;;
    Linux:aarch64|Linux:arm64)
        PLATFORM="Linux-aarch64"
        ;;
    Darwin:x86_64)
        PLATFORM="MacOSX-x86_64"
        ;;
    Darwin:arm64|Darwin:aarch64)
        PLATFORM="MacOSX-arm64"
        ;;
    *)
        echo "Error: Unsupported platform $UNAME_S/$UNAME_M"
        echo "Supported platform mappings are Linux-x86_64, Linux-aarch64, MacOSX-x86_64, MacOSX-arm64"
        WINDOWS_INSTALLER_URL="https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Windows-x86_64.exe"
        echo "For Windows, manually download and install $WINDOWS_INSTALLER_URL"
        exit 2
        ;;
esac

if [[ -z $INSTALL_PREFIX ]]; then
    INSTALL_PREFIX=$HOME/miniforge-$EXP_CONDA_VER
fi
SOURCE_SCRIPT="$INSTALL_PREFIX/etc/profile.d/conda.sh"

echo "Installing Miniforge for platform $PLATFORM at $INSTALL_PREFIX"
DOWNLOAD_URL=https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$PLATFORM.sh
INSTALLER_PATH=$(mktemp "${TMPDIR:-/tmp}/miniforge-XXXXXX.sh")
echo "Downloading installer from URL ${DOWNLOAD_URL}"
curl -o "$INSTALLER_PATH" -L ${DOWNLOAD_URL}
bash "$INSTALLER_PATH" -b -p $INSTALL_PREFIX
rm -f "$INSTALLER_PATH"
source $SOURCE_SCRIPT
hash -r
conda config --set solver libmamba
conda config --set always_yes yes
# Useful for debugging any issues with conda
conda info -a
echo "Successfully installed at $INSTALL_PREFIX. Please activate with 'source setup/activate_conda.sh' in every terminal where you want to use conda" 
