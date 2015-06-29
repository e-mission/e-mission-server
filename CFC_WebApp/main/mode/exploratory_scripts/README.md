The various scripts in here are:

1. `generate_smoothing_from_ground_truth_clusters.py`: Given a file containing
the ground truthed clusters for a user, plots the smoothed versus unsmoothed
trip trajectories for the trips. It does this using the following steps:

a. Get all the section IDs for a particular ground truthed cluster
b. Get the appropriate sections from the database
c. Generate maps of the smoothed and unsmoothed sections
d. Create an overall HTML file that combines smoothed and unsmoothed values side by side in frames

2. `explore_smoothing_trajectories.py`: Given a file containing a binary
classification of trajectories based on whether they need smoothing or not,
generate various potential features that will let us quickly identify "bad"
trajectories and build a trajectory error model.

3. `plot_error_types.py`: Generates the number of errors from each category,
and some stats about the run length of the removed points.
