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
        # print("while getting median speed %s, %s" % (mode, mode_section_df.columns))
        if "speeds" in mode_section_df.columns:
            speeds_list = mode_section_df.speeds
        else:
            # we are using the confirmed trips, which don't have the speed list
            # let's get it by concatenating from the sections
            speeds_list = mode_section_df.apply(_get_speeds_for_trip, axis=1)

        # speeds series is a series with one row per section/trip where the
        # value is the list of speeds in that section/trip
        median_speeds = [pd.Series(sl).dropna().median() for sl
                            in speeds_list]
        mode_median = pd.Series(median_speeds).dropna().median()
        if np.isnan(mode_median):
            logging.debug("still found nan for mode %s, skipping")
        else:
            ret_dict[mode] = float(mode_median)
    return ret_dict

def _get_speeds_for_trip(trip_df_row):
    import itertools
    import emission.storage.decorations.trip_queries as esdt

    section_list = esdt.get_cleaned_sections_for_trip(trip_df_row.user_id, trip_df_row.cleaned_trip)
    logging.debug("Found %s matching sections for trip %s" % (len(section_list), trip_df_row._id))
    speed_list_of_lists = [s["data"]["speeds"] for s in section_list]
    speed_list = list(itertools.chain(*speed_list_of_lists))
    return speed_list
