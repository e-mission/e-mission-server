from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import logging
import numpy as np
import datetime as pydt
import pandas as pd
import matplotlib.pyplot as plt

def plot_error_types():
    types_dict = json.load(open("/Users/shankari/cluster_ground_truth/smoothing/other_auto/smoothing_categories"))
    types_len_dict = {}
    for (category, cat_list) in types_dict.iteritems:
        types_len_dict[category] = len(cat_list)
    plt.xlabel("Number of sections")
    plt.ylabel("Category")
    plt.tight_layout()
    plt.save_fig("/tmp/smoothing_category_plot.png")

def find_consecutive_runs(array):
    prev_val = array[0]
    cons_runs = []
    curr_run = [prev_val]
    for curr_val in array[1:]:
        if curr_val == prev_val + 1:
            curr_run.append(curr_val)
        else:
            cons_runs.append(curr_run)
            curr_run = [curr_val]
        prev_val = curr_val
    cons_runs.append(curr_run)
    return cons_runs

get_numerator = lambda arr : np.sum(arr[np.nonzero(arr > 1)])

def convert_to_run_lengths(removed_pt_dict):
    removed_runs_dict = {}
    sid_list = []
    n_removed_pts_list = []
    number_of_runs_list = []
    max_run_length_list = []
    pct_removed_in_long_runs = []

    for (sid, removed_pt_list) in removed_pt_dict.items():
        curr_run_list = find_consecutive_runs(removed_pt_list)
        removed_runs_dict[sid] = curr_run_list
        sid_list.append(sid)
        n_removed_pts_list.append(len(removed_pt_list))
        number_of_runs_list.append(len(curr_run_list))
        run_length_arr = np.array([len(run) for run in curr_run_list])
        max_run_length_list.append(np.amax(run_length_arr))
        # Here, we want to find the pct of removed points that are in "long"
        # runs (runs of length > 1). So if the run lengths are 
        # [1, 2, 3, 4, 1, 2, 3], for example, we want the numerator to be 14 (2+3+4+2+3),
        # and the denominator to be 16 (14 + 1 + 1)
        #
        # 
        pct_removed_in_long_runs.append(get_numerator(run_length_arr) * 100/len(removed_pt_list))
    
    return pd.DataFrame({"Number of removed points": n_removed_pts_list,
                         "Number of runs": number_of_runs_list,
                         "Max Run Length": max_run_length_list,
                         "Pct in Long Runs": pct_removed_in_long_runs})

def plot_run_length_stats():
    removed_points_dict = json.load(open("/Users/shankari/cluster_ground_truth/smoothing/smoothing_removed_points_combined"))
    df = pet.convert_to_run_lengths(removed_points_dict)
    df["Pct in Long Runs"].hist(bins=50)
    plt.xlabel('Percentage')
    plt.title('Histogram of the percentage of removed points in "long" runs')
    plt.show()

    df["Max Run Length"].hist(bins=10)
    plt.title('Max Run Length')
    plt.xlabel('Number of points in the run')
    plt.show()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    plot_instances_for_gps_error_model()

    # Simple unit test right in the code
    cons_runs = find_consecutive_runs([12, 13, 15, 16, 17, 18, 19, 20, 21, 22])
    logging.debug("cons_runs = %s" % cons_runs)
    assert(len(cons_runs) == 2)
    assert(len(cons_runs[1]) == 8)

    numerator = get_numerator(np.array([1, 2, 3, 4, 1, 2, 3]))
    logging.debug("numerator = %s" % numerator)
    assert(numerator == 14)
