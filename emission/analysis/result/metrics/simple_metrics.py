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
        "median_speed": get_median_speed
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

def get_median_speed(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        median_speeds = [pd.Series(sl).dropna().median() for sl
                            in mode_section_df.speeds]
        mode_median = pd.Series(median_speeds).dropna().median()
        if np.isnan(mode_median):
            logging.debug("still found nan for mode %s, skipping")
        else:
            ret_dict[mode] = float(mode_median)
    return ret_dict
