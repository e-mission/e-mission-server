# Zephyr: using e-mission for evaluating power consumption #

## Collecting data ##
- Install the e-mission app for your platform
    - android: https://play.google.com/store/apps/details?id=edu.berkeley.eecs.emission
    - iOS: https://itunes.apple.com/us/app/emission/id1084198445?&mt=8
- Switch to the zephyr-specific UI (https://e-mission.eecs.berkeley.edu/#/client_setup?new_client=zephyr&clear_usercache=true&clear_local_storage=true)
- Turn off tracking (Profile -> Tracking)
- Change the sampling rate if needed. Default is every hour. (Profile -> Developer Zone -> Sync)
- Run your experiment
- At the end, perform the following steps to ensure that the data is pushed. The app currently only pushes data for completed trips.
  - Start a trip (Developer Zone -> tracking stopped -> start trip)
  - End a trip (Developer Zone -> tracking stopped -> end trip)
  - Force sync

## Analysing data ##
The data from your phone goes directly to a server for open data.
The password for the server is concatenation of the first names of the maintainer's advisors and associated labs (R....D....R...B...). All information can be found in the third sentence of her home page https://people.eecs.berkeley.edu/~shankari/.

The server contains several reference notebooks to help you evaluate the power drain of your sensing regime.
Copy them to a new directory and re-run them to match your experiment.
http://cardshark.cs.berkeley.edu:8888/tree/zephyr/reference
Make sure to change the `label`, `start_ts` and `end_ts` to match your experiment.

In particular, the steps are:
- Calibrate your approach (http://cardshark.cs.berkeley.edu:8888/tree/zephyr/reference/Calibration.ipynb)
- Run your experiment
- Report the difference in slope as your performance evaluation
