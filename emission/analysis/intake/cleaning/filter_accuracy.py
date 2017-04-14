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

# Standard imports
import logging

# Our imports
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

def continuous_collection_in_range(timeseries):
    return timeseries.user_id in estag.TEST_PHONE_IDS and \
           timeseries.user_id not in esdu.TEMP_HANDLED_PUBLIC_PHONES

def filter_accuracy(user_id):
    time_query = epq.get_time_range_for_accuracy_filtering(user_id)
    timeseries = esta.TimeSeries.get_time_series(user_id)
    if not continuous_collection_in_range(timeseries):
        logging.debug("Not a public phone, must already have filtered data, early return")
        epq.mark_accuracy_filtering_done(user_id, None)
        return

    try:
        unfiltered_points_df = timeseries.get_data_df("background/location", time_query)
        if len(unfiltered_points_df) == 0:
            epq.mark_accuracy_filtering_done(user_id, None) 
        else:        
            filtered_from_unfiltered_df = unfiltered_points_df[unfiltered_points_df.accuracy < 200]
            logging.info("filtered %d of %d points" % (len(filtered_from_unfiltered_df), len(unfiltered_points_df)))
            for idx, entry in filtered_from_unfiltered_df.iterrows():
                # First, we check to see if this is a duplicate of an existing entry.
                # If so, we will skip it since it is probably generated as a duplicate...
                if check_prior_duplicate(filtered_from_unfiltered_df, idx, entry):
                    logging.info("Found duplicate entry at index %s, id = %s, lat = %s, lng = %s, skipping" % 
                                    (idx, entry._id, entry.latitude, entry.longitude))
                    continue
                # Next, we check to see if there is an existing "background/filtered_location" point that corresponds
                # to this point. If there is, then we don't want to re-insert. This ensures that this step is idempotent
                if check_existing_filtered_location(timeseries, entry):
                    logging.info("Found existing filtered location for entry at index = %s, id = %s, ts = %s, fmt_time = %s, skipping" % (idx, entry._id, entry.ts, entry.fmt_time))
                    continue
                # logging.debug("Inserting %s filtered entry %s into timeseries" % (idx, entry))
                entry_copy = convert_to_filtered(timeseries.get_entry_at_ts(
                                                    "background/location",
                                                    "metadata.write_ts",
                                                    entry.metadata_write_ts))
                timeseries.insert(entry_copy)
            last_entry_processed = unfiltered_points_df.iloc[-1].metadata_write_ts
            epq.mark_accuracy_filtering_done(user_id, last_entry_processed) 
    except:
        logging.exception("Marking accuracy filtering as failed")
        epq.mark_accuracy_filtering_failed(user_id)
