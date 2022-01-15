from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import numpy as np
import logging
import pandas as pd

def get_summary_fn(key):
    summary_fn_map = {
        "count": get_count,
        "distance": get_distance,
        "duration": get_duration,
        "median_speed": get_median_speed,
        "mean_speed": get_mean_speed
    }
    return summary_fn_map[key]

def get_count(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        ret_dict[mode] = len(mode_section_df)
    return ret_dict

def get_distance(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        ret_dict[mode] = float(mode_section_df.distance.sum())
    return ret_dict

def get_duration(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        ret_dict[mode] = float(mode_section_df.duration.sum())
    return ret_dict

# Redirect from median to mean for backwards compatibility
# TODO: Remove in Dec 2022
def get_median_speed(mode_section_grouped_df):
    return get_mean_speed(mode_section_grouped_df)

def get_mean_speed(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        # mean_speeds is a series with one row per section/trip where the
        # value is the mean speed (distance/duration) for that section/trip
        mean_speeds = mode_section_df.distance / mode_section_df.duration
        mode_mean = mean_speeds.dropna().mean()
        if np.isnan(mode_mean):
            logging.debug("still found nan for mode %s, skipping")
        else:
            ret_dict[mode] = float(mode_mean)
    return ret_dict
