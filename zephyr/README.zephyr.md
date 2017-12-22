# Zephyr: using e-mission for evaluating power consumption #

## Collecting data ##
- Install the e-mission app for your platform
    - android: https://play.google.com/store/apps/details?id=edu.berkeley.eecs.emission
    - iOS: https://itunes.apple.com/us/app/emission/id1084198445?&mt=8
- Turn off tracking (Profile -> Tracking)
- Set the sampling rate (Profile -> Developer Zone -> Sync)
- Send shankari@eecs.berkeley.edu email from address you used to log in stating
  that you want the data collected for that email address to be public

## Analysing data ##
- Clone the e-mission server repo (https://github.com/e-mission/e-mission-server)
- Clone the data-collection-eval repo (https://github.com/shankari/data-collection-eval)
- Modify one of the existing notebooks in `analysis_summer_2016` to compare the power drain of your
  experiment. Good starting points would be:
  - `analysis_summer_2016/paper_plots/SW_data_plot_phones_Jul_17.ipynb`
  - `analysis_summer_2016/paper_plots/JF_phones/JF_phones_combined.ipynb`
- Make sure to change the `ids`, `start_ts` and `end_ts` to match your experiment
- Obtain the relative slopes of ground truth and experiment as described in the
  Zephyr paper
- Report the difference in slope as your performance evaluation
