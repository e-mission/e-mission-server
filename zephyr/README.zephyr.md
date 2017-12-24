# Zephyr: using e-mission for evaluating power consumption #

## Collecting data ##
- Install the e-mission app for your platform
    - android: https://play.google.com/store/apps/details?id=edu.berkeley.eecs.emission
    - iOS: https://itunes.apple.com/us/app/emission/id1084198445?&mt=8
- Switch to the zephyr-specific UI (https://e-mission.eecs.berkeley.edu/#/client_setup?new_client=zephyr&clear_usercache=true&clear_local_storage=true)
- Turn off tracking (Profile -> Tracking)
- Change the sampling rate if needed. Default is every hour. (Profile -> Developer Zone -> Sync)

## Analysing data ##
Re-run the reference notebooks to match your experiment.
http://34.239.42.177:8888/tree/zephyr/reference
Make sure to change the `label`, `start_ts` and `end_ts` to match your experiment.

In particular, the steps are:
- Calibrate your approach (http://34.239.42.177:8888/tree/zephyr/reference/Calibration.ipynb)
- Run your experiment
- Report the difference in slope as your performance evaluation
