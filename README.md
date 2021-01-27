This repository contains ipython notebooks for the evaluation of the e-mission
platform.  These notebooks re-use code from the e-mission-server codebase, so
it needs to be included while running them.

### Running.

1. Install the e-mission server, including setting it up
    https://github.com/e-mission/e-mission-server

1. Set the home environment variable

    ```
    $ export EMISSION_SERVER_HOME=<path_to_emission_server_repo>
    ```

1. If you haven't setup before, set up the evaluation system

    ```
    $ source setup.sh
    ```

1. If you have, activate

    ```
    $ source activate.sh
    ```

1. Start the notebook server

```
$ ../bin/em-jupyter-notebook.sh
```

### Loading data

- To get the data for the notebooks to run on, look at the dataset listed at
  the top of the notebook, and request the data for research purposes using 
    https://github.com/e-mission/e-mission-server/wiki/Requesting-data-as-a-collaborator

### Cleaning up

After completing analysis, tear down

```
$ source teardown.sh
```

### Checking in notebooks

Note that all notebooks checked in here are completely public. All results included in them can be viewed by anybody, even malicious users. 
Therefore, you need to split your analysis into two groups:
- *aggregate only*: results are not specific for a single user. The scripts in such notebooks should not include uuids, and should use the aggregate timeseries instead of the default timeseries.
   - example: number of walking and biking trips over all users in the control group
- *individual analyses*: results are specific for a single user. The scripts in such notebooks can include uuids, and potentially even user emails or tokens.
   - example: varation in walking and biking trips over time for user `uuid1`

Notebooks that include aggregate analyses can be checked in with outputs included. This is because it is hard to tease out the contributions by individuals to the aggregate statistics, and so the chances of leaking information are low. However, notebooks that include individual analyses should be checked in after deleting all outputs (Kernel -> Restart and clear output).

|              | Aggregate results | Individual results |
|--------------|--------------|--------------|
| with outputs |     Y        |     **N**    |
| after clearing outputs | Y  |     Y        | 
