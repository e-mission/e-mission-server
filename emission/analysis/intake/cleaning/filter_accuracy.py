"""
Creates a filtered stream (filtered_location) from the original 
unfiltered stream. Used for clients that don't perform their own
filtering, such as ongoing data collection clients on android.
The high level algorithm is to:
  - filter out all entries with an accuracy higher than the threshold
  - filter out all entries that are identical to the entries that we have
    seen before
  - see whether we have already saved this entry. This is important for two reasons:
    - we don't want to generate duplicate entries in case the client provides us
        with a pre-filtered stream (as in the case of android with geofencing)
    - we want to ensure that this operation, just like all our other operations,
        is idempotent. That ensures that we can simply reset the pipeline state
        and re-run everything if we have any changes that we need to make.
""" 
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

# Our imports
import emission.analysis.config as eac
import emission.storage.pipeline_queries as epq
import emission.storage.decorations.user_queries as esdu
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.aggregate_timeseries as estag

def check_prior_duplicate(df, idx, entry):
    """
    Returns true if there is an entry in the dataframe that exactly matches the
    latitude and longitude for this entry
    """
    # We need to stop at idx-1 because otherwise, the entry being checked will
    # be included in the results and we will think that everything has a
    # duplicate
    # logging.debug("When idx = %s, last entry checked = %s" % (idx, df.loc[0:idx-1].tail(2)))
    logging.debug("check_prior_duplicate called with size = %d, entry = %d" % (len(df), idx))
    if len(df) == 0:
        logging.info("len(df) == 0, early return")
        return False
    duplicates = df.loc[0:idx-1].query("latitude == @entry.latitude and longitude == @entry.longitude")
    # logging.debug("for entry with fmt_time = %s, ts = %s, lat = %s, lng = %s, found %d duplicates" % 
    #                 (entry.fmt_time, entry.ts, entry.latitude, entry.longitude, len(duplicates)))
    if len(duplicates) == 1:
        logging.debug("duplicate fields are fmt_time = %s, ts = %s, lat = %s, lng = %s" %
                        (duplicates.fmt_time.iloc[0], duplicates.ts.iloc[0],
                         duplicates.latitude.iloc[0], duplicates.longitude.iloc[0]))
    return len(duplicates) > 0
    
def check_existing_filtered_location(timeseries, entry):
    """
    Returns true if there is an existing filtered_location.
    We check for this by querying the database for an existing entry with the same timestamp.
    Note that we cannot check for the write_ts because in case the stream is filtered on the client,
    the write_ts for the filtered location may be slightly different.
    """
    existing_duplicate = timeseries.get_entry_at_ts("background/filtered_location", "data.ts", entry.ts)
    if existing_duplicate is not None:
        return True
    else:
        return False

def convert_to_filtered(entry):
    del entry["_id"]
    entry["metadata"]["key"] = "background/filtered_location"
    return entry

def filter_accuracy(user_id):
    time_query = epq.get_time_range_for_accuracy_filtering(user_id)
    timeseries = esta.TimeSeries.get_time_series(user_id)

    if not eac.get_config()["intake.cleaning.filter_accuracy.enable"]:
        logging.debug("filter_accuracy disabled, early return")
        epq.mark_accuracy_filtering_done(user_id, None)
        return

    SEL_FIELDS_FOR_DUP = ["latitude", "longitude", "ts", "accuracy", "metadata_write_ts"]

    try:
        unfiltered_points_list = list(timeseries.find_entries(["background/location"], time_query))
        unfiltered_points_df = timeseries.get_data_df("background/location", time_query)
        if len(unfiltered_points_df) == 0:
            epq.mark_accuracy_filtering_done(user_id, None) 
        else:        
            unfiltered_points_df = unfiltered_points_df[SEL_FIELDS_FOR_DUP]
            filtered_from_unfiltered_df = unfiltered_points_df[unfiltered_points_df.accuracy < 200].drop_duplicates()
            logging.info("filtered %d of %d points" % (len(filtered_from_unfiltered_df), len(unfiltered_points_df)))
            filtered_points_df = timeseries.get_data_df("background/filtered_location", time_query)
            if len(filtered_points_df) == 0:
                logging.debug("No filtered points found, inserting all %d newly filtered points" % len(filtered_from_unfiltered_df))
                to_insert_df = filtered_from_unfiltered_df
            else:
                logging.debug("Partial filtered points %d found" % len(filtered_points_df))
                filtered_points_df = filtered_points_df[SEL_FIELDS_FOR_DUP]
                matched_points_df = filtered_from_unfiltered.merge(filtered_points_df, on="ts", left_index=True)
                to_insert_df = filtered_from_unfiltered_df.drop(index=matched_points_df.index)
            for idx, entry in to_insert_df.iterrows():
                unfiltered_entry = unfiltered_points_list[idx]
                # logging.debug("Inserting %s filtered entry %s into timeseries" % (idx, entry))
                entry_copy = convert_to_filtered(unfiltered_entry)
                timeseries.insert(entry_copy)
            last_entry_processed = unfiltered_points_df.iloc[-1].metadata_write_ts
            epq.mark_accuracy_filtering_done(user_id, float(last_entry_processed))
    except:
        logging.exception("Marking accuracy filtering as failed")
        epq.mark_accuracy_filtering_failed(user_id)
