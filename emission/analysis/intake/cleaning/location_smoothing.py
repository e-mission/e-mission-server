from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import *
from past.utils import old_div
import numpy as np
import json
import logging
from dateutil import parser
import math
import pandas as pd
import attrdict as ad
import datetime as pydt
import time as time
import pytz
import geojson as gj
# This change should be removed in the next server update, by which time hopefully the new geojson version will incorporate the long-term fix for their default precision
# See - jazzband/geojson#177
# See = https://github.com/e-mission/e-mission-server/pull/900/commits/d2ada640f260aad8cbcfecb81345f4087c810baa
gj.geometry.Geometry.__init__.__defaults__ = (None, False, 15)

# Our imports
import emission.analysis.point_features as pf
import emission.analysis.intake.cleaning.cleaning_methods.speed_outlier_detection as eaico
import emission.analysis.intake.cleaning.cleaning_methods.jump_smoothing as eaicj

import emission.storage.pipeline_queries as epq

import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.metadata as ecwm
import emission.core.wrapper.smoothresults as ecws
import emission.core.common as ecc

import emission.storage.decorations.useful_queries as taug
import emission.storage.decorations.location_queries as lq
import emission.core.get_database as edb

np.set_printoptions(suppress=True)

# This is what we use in the segmentation code to see if the points are "the same"
DEFAULT_SAME_POINT_DISTANCE = 100
MACH1 = 340.29

def recalc_speed(points_df):
    """
    The input dataframe already has "speed" and "distance" columns.
    Drop them and recalculate speeds from the first point onwards.
    The speed column has the speed between each point and its previous point.
    The first row has a speed of zero.
    Uses vectorized numpy operations for better performance.
    
    This is a wrapper around add_dist_heading_speed for backward compatibility.
    https://github.com/e-mission/e-mission-server/pull/1048#discussion_r2050693860
    """
    # First, drop the existing speed and distance columns
    stripped_df = points_df.drop("speed", axis=1).drop("distance", axis=1)
    
    # Call add_dist_heading_speed with add_heading=False
    return add_dist_heading_speed(stripped_df, add_heading=False)

def add_dist_heading_speed(points_df, add_heading=True):
    # type: (pandas.DataFrame, bool) -> pandas.DataFrame
    """
    Returns a new dataframe with added "distance", "speed", and optionally "heading" columns.
    Uses vectorized numpy operations for better performance.
    
    The distance, speed, and heading columns have values between each point and its previous point.
    The first row has values of zero.
    """
    from emission.core.common import haversine_numpy, calHeading_numpy
    
    if len(points_df) <= 1:
        # Handle empty or single-point dataframes
        distances = np.zeros(len(points_df))
        speeds = np.zeros(len(points_df))
        headings = np.zeros(len(points_df)) if add_heading else None
    else:
        # Extract coordinates and timestamps
        lons = points_df['longitude'].to_numpy()
        lats = points_df['latitude'].to_numpy()
        
        # Calculate distance using haversine_numpy
        distances = np.zeros(len(points_df))
        distances[1:] = haversine_numpy(
            lons[:-1], lats[:-1],
            lons[1:], lats[1:]
        )
        
        # Calculate time differences
        timestamps = points_df['ts'].to_numpy()
        time_diffs = np.zeros(len(points_df))
        time_diffs[1:] = timestamps[1:] - timestamps[:-1]
        
        # Calculate speeds
        speeds = np.zeros(len(points_df))
        # Avoid division by zero
        mask = time_diffs > 0
        speeds[mask] = distances[mask] / time_diffs[mask]
        
        # Calculate headings if requested
        if add_heading:
            headings = np.zeros(len(points_df))
            headings[1:] = calHeading_numpy(
                lons[:-1], lats[:-1],
                lons[1:], lats[1:]
            )
        else:
            headings = None
    
    # Add calculated columns to dataframe
    with_distances_df = pd.concat([points_df, pd.Series(distances, index=points_df.index, name="distance")], axis=1)
    with_speeds_df = pd.concat([with_distances_df, pd.Series(speeds, index=points_df.index, name="speed")], axis=1)
    
    if add_heading:
        # Add heading column
        if "heading" in with_speeds_df.columns:
            with_speeds_df.drop("heading", axis=1, inplace=True)
        with_speeds_df = pd.concat([with_speeds_df, pd.Series(headings, index=points_df.index, name="heading")], axis=1)
        
    return with_speeds_df

def add_heading_change(points_df):
    """
    Returns a new dataframe with an added "heading_change" column.
    The heading change column has the heading change between this point and the
    two points preceding it. The first two rows have a speed of zero.
    """
    point_list = [ad.AttrDict(row) for row in points_df.to_dict('records')]
    zipped_points_list = list(zip(point_list, point_list[1:], point_list[2:]))
    hcs = [pf.calHC(p1, p2, p3) for (p1, p2, p3) in zipped_points_list]
    hcs.insert(0, 0)
    hcs.insert(1, 0)
    with_hcs_df = pd.concat([points_df, pd.Series(hcs, name="heading_change")], axis=1)
    return with_hcs_df

def filter_current_sections(user_id):
    time_query = epq.get_time_range_for_smoothing(user_id)
    try:
        ts = esta.TimeSeries.get_time_series(user_id)
        sections_to_process = esda.get_entries(esda.RAW_SECTION_KEY, user_id,
                                               time_query)
        time_query.timeType = "data.ts"
        loc_df = ts.get_data_df("background/filtered_location", time_query)
        entries_to_insert = []
        for section in sections_to_process:
            logging.info("^" * 20 + ("Smoothing section %s for user %s" % (section['_id'], user_id)) + "^" * 20)
            smoothing_result = filter_jumps(user_id, section, loc_df)
            if smoothing_result:
                entries_to_insert.append(smoothing_result)
        if entries_to_insert:
            ts.bulk_insert(entries_to_insert, esta.EntryType.ANALYSIS_TYPE)
        if len(sections_to_process) == 0:
            # Didn't process anything new so start at the same point next time
            last_section_processed = None
        else:    
            last_section_processed = sections_to_process[-1]
        epq.mark_smoothing_done(user_id, last_section_processed)
    except:
        logging.exception("Marking smoothing as failed")
        epq.mark_smoothing_failed(user_id)

def filter_jumps(user_id, section, loc_df):
    """
    filters out any jumps in the points related to this section and creates a entry with the deleted points for
    this trip and this section.
    :param user_id: the user id to filter the trips for
    :param section: the Section object to filter the trips for
    :param loc_df: a dataframe of background/filtered_location in the current range being processed
    :return: a created analysis/smoothing entry with the filtered points, or None if no points were filtered
    """

    logging.debug("filter_jumps(%s, %s) called" % (user_id, section.get_id()))
    outlier_algo = eaico.BoxplotOutlier()

    ts_in_section = "@section.data.start_ts <= ts <= @section.data.end_ts"
    section_points_df = loc_df.query(ts_in_section).reset_index(drop=True)
    is_ios = section_points_df["filter"].dropna().unique().tolist() == ["distance"]
    if is_ios:
        logging.debug("Found iOS section, filling in gaps with fake data")
        section_points_df = _ios_fill_fake_data(section_points_df)
    filtering_algo = eaicj.SmoothZigzag(is_ios, DEFAULT_SAME_POINT_DISTANCE)
    backup_filtering_algo = eaicj.SmoothPosdap(MACH1)

    logging.debug("len(section_points_df) = %s" % len(section_points_df))
    (sel_algo, points_to_ignore_df) = get_points_to_filter(section_points_df, outlier_algo, filtering_algo, backup_filtering_algo)
    if points_to_ignore_df is None:
        # There were no points to delete
        return None
    points_to_ignore_df_filtered = points_to_ignore_df._id.dropna()
    logging.debug("after filtering ignored points, using %s, %s -> %s" %
                  (sel_algo, len(points_to_ignore_df), len(points_to_ignore_df_filtered)))
    # We shouldn't really filter any fuzzed points because they represent 100m in 60 secs
    # but let's actually check for that
    # assert len(points_to_ignore_df) == len(points_to_ignore_df_filtered)
    deleted_point_id_list = list(points_to_ignore_df_filtered)
    logging.debug("deleted %s points" % len(deleted_point_id_list))

    filter_result = ecws.Smoothresults()
    filter_result.section = section.get_id()
    filter_result.deleted_points = deleted_point_id_list
    filter_result.outlier_algo = "BoxplotOutlier"
    filter_result.filtering_algo = sel_algo.__class__.__name__.split(".")[-1]
    return ecwe.Entry.create_entry(user_id, "analysis/smoothing", filter_result)


def get_points_to_filter(section_points_df, outlier_algo, filtering_algo, backup_filtering_algo):
    """
    From the incoming dataframe, filter out large jumps using the specified outlier detection algorithm and
    the specified filtering algorithm.
    :param section_points_df: a dataframe of points for the current section
    :param outlier_algo: the algorithm used to detect outliers
    :param filtering_algo: the algorithm used to determine which of those outliers need to be filtered
    :return: a dataframe of points that need to be stripped, if any.
            None if none of them need to be stripped.
    """
    with_speeds_df = add_dist_heading_speed(section_points_df)
    logging.debug("section_points_df.shape = %s, with_speeds_df.shape = %s" %
                  (section_points_df.shape, with_speeds_df.shape))
    # if filtering algo is none, there's nothing that can use the max speed
    if outlier_algo is not None and filtering_algo is not None:
        maxSpeed = outlier_algo.get_threshold(with_speeds_df)
        # TODO: Is this the best way to do this? Or should I pass this in as an argument to filter?
        # Or create an explicit set_speed() method?
        # Or pass the outlier_algo as the parameter to the filtering_algo?
        filtering_algo.maxSpeed = maxSpeed
        logging.debug("maxSpeed = %s" % filtering_algo.maxSpeed)
    if filtering_algo is not None:
        try:
            filtering_algo.filter(with_speeds_df)
            outlier_arr = np.nonzero(np.logical_not(filtering_algo.inlier_mask_))
            logging.debug("After first filter, inliers = %s, outliers = %s of type %s" %
                (filtering_algo.inlier_mask_, outlier_arr, type(outlier_arr)))
            if outlier_arr[0].shape[0] == 0:
                sel_algo = filtering_algo
            else:
                recomputed_speeds_df = recalc_speed(with_speeds_df[filtering_algo.inlier_mask_])
                recomputed_threshold = outlier_algo.get_threshold(recomputed_speeds_df)
                logging.info("After first round, recomputed max = %s, recomputed threshold = %s" %
                    (recomputed_speeds_df.speed.max(), recomputed_threshold))
                # assert recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold].shape[0] == 0, "After first round, still have outliers %s" % recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold]
                if recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold].shape[0] == 0:
                    logging.info("No outliers after first round, default algo worked, to_delete = %s" %
                        np.nonzero(np.logical_not(filtering_algo.inlier_mask_)))
                    sel_algo = filtering_algo
                else:
                    logging.info("After first round, still have outliers %s" % recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold])
                    if backup_filtering_algo is None or recomputed_speeds_df.speed.max() < MACH1:
                        logging.debug("backup algo is %s, max < MACH1, so returning default algo outliers %s" %
                            (backup_filtering_algo, np.nonzero(np.logical_not(filtering_algo.inlier_mask_))))
                        sel_algo = filtering_algo
                    else:
                        backup_filtering_algo.filter(with_speeds_df)
                        recomputed_speeds_df = recalc_speed(with_speeds_df[backup_filtering_algo.inlier_mask_])
                        recomputed_threshold = outlier_algo.get_threshold(recomputed_speeds_df)
                        logging.info("After second round, max = %s, recomputed threshold = %s" %
                            (recomputed_speeds_df.speed.max(), recomputed_threshold))
                        # assert recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold].shape[0] == 0, "After first round, still have outliers %s" % recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold]
                        if recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold].shape[0] == 0:
                            logging.info("After second round, no outliers, returning backup to delete %s" %
                                np.nonzero(np.logical_not(backup_filtering_algo.inlier_mask_)))
                            sel_algo = backup_filtering_algo
                        else:
                            logging.info("After second round, still have outliers %s" % recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold])
                            if recomputed_speeds_df.speed.max() < MACH1:
                                logging.debug("But they are all < %s, so returning backup to delete %s" %
                                    (MACH1, np.nonzero(np.logical_not(backup_filtering_algo.inlier_mask_))))
                                sel_algo = backup_filtering_algo
                            else:
                                logging.info("And they are also > %s, backup algo also failed, returning default to delete = %s" %
                                    (MACH1, np.nonzero(np.logical_not(filtering_algo.inlier_mask_))))
                                sel_algo = filtering_algo

            to_delete_mask = np.logical_not(sel_algo.inlier_mask_)
            logging.info("After all checks, inlier mask = %s, outlier_mask = %s" %
                (np.nonzero(sel_algo.inlier_mask_), np.nonzero(to_delete_mask)))
            return (sel_algo, with_speeds_df[to_delete_mask])
        except Exception as e:
            logging.exception("Caught error %s while processing section, skipping..." % e)
            return (None, None)
    else:
        logging.debug("no filtering algo specified, returning None")
        return (None, None)

def _ios_fill_fake_data(locs_df):
    diff_ts = locs_df.ts.diff()
    fill_ends = diff_ts[diff_ts > 60].index.tolist()

    if len(fill_ends) == 0:
        logging.debug("No large gaps found, no gaps to fill")
        return locs_df
    else:
        logging.debug("Found %s large gaps, filling them all" % len(fill_ends))

    filled_df = locs_df

    for end in fill_ends:
        logging.debug("Found large gap ending at %s, filling it" % end)
        assert end > 0
        start = end - 1
        start_point = locs_df.iloc[start]["loc"]["coordinates"]
        end_point = locs_df.iloc[end]["loc"]["coordinates"]
        if ecc.calDistance(start_point, end_point) > DEFAULT_SAME_POINT_DISTANCE:
            logging.debug("Distance between %s and %s = %s, adding noise is not enough, skipping..." %
                          (start_point, end_point, ecc.calDistance(start_point, end_point)))
            continue

        # else
        # Design from https://github.com/e-mission/e-mission-server/issues/391#issuecomment-247246781
        logging.debug("start = %s, end = %s, generating entries between %s and %s" %
            (start, end, locs_df.ts[start], locs_df.ts[end]))
        ts_fill = np.arange(locs_df.ts[start] + 60, locs_df.ts[end], 60)
        # We only pick entries that are *greater than* 60 apart
        assert len(ts_fill) > 0
        dist_fill = np.random.uniform(low=0, high=100, size=len(ts_fill))
        angle_fill = np.random.uniform(low=0, high=2 * np.pi, size=len(ts_fill))
        # Formula from http://gis.stackexchange.com/questions/5821/calculating-latitude-longitude-x-miles-from-point
        lat_fill = locs_df.latitude[end] + np.multiply(dist_fill, old_div(np.sin(
            angle_fill), 111111))
        cl = np.cos(locs_df.latitude[end])
        lng_fill = locs_df.longitude[end] + np.multiply(dist_fill, np.cos(
            angle_fill) / cl / 111111)
        logging.debug("Fill lengths are: dist %s, angle %s, lat %s, lng %s" %
                      (len(dist_fill), len(angle_fill), len(lat_fill), len(lng_fill)))

        # Unsure if this is needed, but lets put it in just in case
        loc_fill = [gj.Point(l) for l in zip(lng_fill, lat_fill)]
        fill_df = pd.DataFrame(
            {"ts": ts_fill, "longitude": lng_fill, "latitude": lat_fill,
             "loc": loc_fill})
        filled_df = pd.concat([filled_df, fill_df])

    sorted_filled_df = filled_df.sort_values(by="ts").reset_index()
    logging.debug("after filling, returning head = %s, tail = %s" %
                  (sorted_filled_df[["fmt_time", "ts", "latitude", "longitude", "metadata_write_ts"]].head(),
                   sorted_filled_df[["fmt_time", "ts", "latitude", "longitude", "metadata_write_ts"]].tail()))
    return sorted_filled_df

