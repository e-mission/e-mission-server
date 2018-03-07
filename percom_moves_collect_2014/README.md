### Overview

This directory contains the analysis included in the PerCom 2014 paper. Unfortunately, the underlying dataset cannot be published because it contains privacy-sensitive location data. However, the corresponding author can run analyses based on the notebooks here against the raw data and return the results.

The analysis is represented in three notebooks:
- `mode_inference_percom_july_2014_data.ipynb`: base notebook. Since results are not included, can easily track changes to the analysis scripts. Needs access to the raw dataset to re-generate results.
- `mode_inference_percom_july_2014_data_with_result.ipynb`: copy of previous notebook with results included. Since results are included, diffs are going to be very hard to parse, but it shows a lot more detailed results than the paper without needing access to the data.
- `Project_scratch_mode_inference.ipynb`: original notebook used for paper. has bitrotted and cannot be run any more, but is useful as the source of the original results

### Running

The notebook *must* be run from the current directory because it uses some files that are relative to this directory.

1. Install the e-mission server, including setting it up

1. Set the home environment variable

```
$ export EMISSION_SERVER_HOME=<path_to_emission_server_repo>
```

1. Set up this analysis

```
$ source setup.sh
```

1. Start the notebook server

```
$ ../bin/em-jupyter-notebook.sh
```

After completing analysis, tear down

```
$ source teardown.sh
```
