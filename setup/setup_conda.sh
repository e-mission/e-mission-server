EXP_CONDA_VER=4.5.12

wget https://repo.continuum.io/miniconda/Miniconda3-$EXP_CONDA_VER-Linux-x86_64.sh -O miniconda.sh;
bash miniconda.sh -b -p $HOME/miniconda
source "$HOME/miniconda/etc/profile.d/conda.sh"
hash -r
conda config --set always_yes yes --set changeps1 no
# Useful for debugging any issues with conda
conda info -a
