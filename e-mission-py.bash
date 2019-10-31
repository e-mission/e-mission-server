# 
# Simple script to ensure that the pythonpath is set correctly before executing the command.
# As we increase the number of directories that we need, this becomes useful.
# Maybe we can restructure the code to avoid this, but this is a useful placeholder until then.

# Make sure that the python here is the anaconda python if that is not the one in the path

PYTHONPATH=. python "$@"
