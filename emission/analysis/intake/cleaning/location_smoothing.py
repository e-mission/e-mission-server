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

def recalc_speed(points_df):
    """
    The input dataframe already has "speed" and "distance" columns.
    Drop them and recalculate speeds from the first point onwards.
    The speed column has the speed between each point and its previous point.
    The first row has a speed of zero.
    """
    stripped_df = points_df.drop("speed", axis=1).drop("distance", axis=1)
    logging.debug("columns in points_df = %s" % points_df.columns)
    point_list = [ad.AttrDict(row) for row in points_df.to_dict('records')]
    zipped_points_list = list(zip(point_list, point_list[1:]))
    distances = [pf.calDistance(p1, p2) for (p1, p2) in zipped_points_list]
    distances.insert(0, 0)
    with_speeds_df = pd.concat([stripped_df, pd.Series(distances, index=points_df.index, name="distance")], axis=1)
    speeds = [pf.calSpeed(p1, p2) for (p1, p2) in zipped_points_list]
    speeds.insert(0, 0)
    with_speeds_df = pd.concat([with_speeds_df, pd.Series(speeds, index=points_df.index, name="speed")], axis=1)
    return with_speeds_df

def add_dist_heading_speed(points_df):
    # type: (pandas.DataFrame) -> pandas.DataFrame
    """
    Returns a new dataframe with an added "speed" column.
    The speed column has the speed between each point and its previous point.
    The first row has a speed of zero.
    """
    point_list = [ad.AttrDict(row) for row in points_df.to_dict('records')]
    zipped_points_list = list(zip(point_list, point_list[1:]))

    distances = [pf.calDistance(p1, p2) for (p1, p2) in zipped_points_list]
    distances.insert(0, 0)
    speeds = [pf.calSpeed(p1, p2) for (p1, p2) in zipped_points_list]
    speeds.insert(0, 0)
    headings = [pf.calHeading(p1, p2) for (p1, p2) in zipped_points_list]
    headings.insert(0, 0)

    with_distances_df = pd.concat([points_df, pd.Series(distances, name="distance")], axis=1)
    with_speeds_df = pd.concat([with_distances_df, pd.Series(speeds, name="speed")], axis=1)
    if "heading" in with_speeds_df.columns:
        with_speeds_df.drop("heading", axis=1, inplace=True)
    with_headings_df = pd.concat([with_speeds_df, pd.Series(headings, name="heading")], axis=1)
    return with_headings_df

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
        sections_to_process = esda.get_entries(esda.RAW_SECTION_KEY, user_id,
                                               time_query)
        for section in sections_to_process:
            logging.info("^" * 20 + ("Smoothing section %s for user %s" % (section.get_id(), user_id)) + "^" * 20)
            filter_jumps(user_id, section.get_id())
        if len(sections_to_process) == 0:
            # Didn't process anything new so start at the same point next time
            last_section_processed = None
        else:    
            last_section_processed = sections_to_process[-1]
        epq.mark_smoothing_done(user_id, last_section_processed)
    except:
        logging.exception("Marking smoothing as failed")
        epq.mark_smoothing_failed(user_id)

def filter_jumps(user_id, section_id):
    """
    filters out any jumps in the points related to this section and stores a entry that lists the deleted points for
    this trip and this section.
    :param user_id: the user id to filter the trips for
    :param section_id: the section_id to filter the trips for
    :return: none. saves an entry with the filtered points into the database.
    """

    logging.debug("filter_jumps(%s, %s) called" % (user_id, section_id))
    outlier_algo = eaico.BoxplotOutlier()

    tq = esda.get_time_query_for_trip_like(esda.RAW_SECTION_KEY, section_id)
    ts = esta.TimeSeries.get_time_series(user_id)
    section_points_df = ts.get_data_df("background/filtered_location", tq)
    is_ios = section_points_df["filter"].dropna().unique().tolist() == ["distance"]
    if is_ios:
        logging.debug("Found iOS section, filling in gaps with fake data")
        section_points_df = _ios_fill_fake_data(section_points_df)
    filtering_algo = eaicj.SmoothZigzag(is_ios, DEFAULT_SAME_POINT_DISTANCE)

    logging.debug("len(section_points_df) = %s" % len(section_points_df))
    points_to_ignore_df = get_points_to_filter(section_points_df, outlier_algo, filtering_algo)
    if points_to_ignore_df is None:
        # There were no points to delete
        return
    points_to_ignore_df_filtered = points_to_ignore_df._id.dropna()
    logging.debug("after filtering ignored points, %s -> %s" %
                  (len(points_to_ignore_df), len(points_to_ignore_df_filtered)))
    # We shouldn't really filter any fuzzed points because they represent 100m in 60 secs
    # but let's actually check for that
    # assert len(points_to_ignore_df) == len(points_to_ignore_df_filtered)
    deleted_point_id_list = list(points_to_ignore_df_filtered)
    logging.debug("deleted %s points" % len(deleted_point_id_list))

    filter_result = ecws.Smoothresults()
    filter_result.section = section_id
    filter_result.deleted_points = deleted_point_id_list
    filter_result.outlier_algo = "BoxplotOutlier"
    filter_result.filtering_algo = "SmoothZigzag"

    result_entry = ecwe.Entry.create_entry(user_id, "analysis/smoothing", filter_result)
    ts.insert(result_entry)

def get_points_to_filter(section_points_df, outlier_algo, filtering_algo):
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
            to_delete_mask = np.logical_not(filtering_algo.inlier_mask_)
            return with_speeds_df[to_delete_mask]
        except Exception as e:
            logging.info("Caught error %s while processing section, skipping..." % e)
            return None
    else:
        logging.debug("no filtering algo specified, returning None")
        return None


def get_filtered_points(section_df, outlier_algo, filtering_algo):
    """
    Filter the points that correspond to the section object that is passed in.
    The section object is an AttrDict with the startTs and endTs fields.
    Returns a filtered df with the index after the initial filter for accuracy
    TODO: Switch this to the section wrapper object going forward
    TODO: Note that here, we assume that the data has already been chunked into sections.
    But really, we need to filter (at least for accuracy) before segmenting in
    order to avoid issues like https://github.com/e-mission/e-mission-data-collection/issues/45
    """
    with_speeds_df = add_dist_heading_speed(section_df)
    # if filtering algo is none, there's nothing that can use the max speed
    if outlier_algo is not None and filtering_algo is not None:
        maxSpeed = outlier_algo.get_threshold(with_speeds_df)
        # TODO: Is this the best way to do this? Or should I pass this in as an argument to filter?
        # Or create an explicit set_speed() method?
        # Or pass the outlier_algo as the parameter to the filtering_algo?
        filtering_algo.maxSpeed = maxSpeed
    if filtering_algo is not None:
        try:
            filtering_algo.filter(with_speeds_df)
            return with_speeds_df[filtering_algo.inlier_mask_]
        except Exception as e:
            logging.info("Caught error %s while processing section, skipping..." % e)
            return with_speeds_df
    else:
        return with_speeds_df

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

