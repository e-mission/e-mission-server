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

1. Set up the evaluation system

    ```
    $ source setup.sh
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
