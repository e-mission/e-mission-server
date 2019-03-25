from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import datetime as pydt

import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ps
import emission.storage.timeseries.timequery as estt

import time

END_FUZZ_AVOID_LTE = 5

def mark_usercache_done(user_id, last_processed_ts):
    if last_processed_ts is None:
        mark_stage_done(user_id, ps.PipelineStages.USERCACHE, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.USERCACHE, last_processed_ts + END_FUZZ_AVOID_LTE)

def get_time_range_for_usercache(user_id):
    tq = get_time_range_for_stage(user_id, ps.PipelineStages.USERCACHE)
    return tq

def get_time_range_for_accuracy_filtering(user_id):
    return get_time_range_for_stage(user_id, ps.PipelineStages.ACCURACY_FILTERING)

def mark_accuracy_filtering_done(user_id, last_processed_ts):
    if last_processed_ts is None:
        mark_stage_done(user_id, ps.PipelineStages.ACCURACY_FILTERING, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.ACCURACY_FILTERING, last_processed_ts + END_FUZZ_AVOID_LTE)

def mark_accuracy_filtering_failed(user_id):
    mark_stage_failed(user_id, ps.PipelineStages.ACCURACY_FILTERING)

def get_time_range_for_segmentation(user_id):
    return get_time_range_for_stage(user_id, ps.PipelineStages.TRIP_SEGMENTATION)

def mark_segmentation_done(user_id, last_processed_ts):
    if last_processed_ts is None:
        mark_stage_done(user_id, ps.PipelineStages.TRIP_SEGMENTATION, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.TRIP_SEGMENTATION,
                        last_processed_ts + END_FUZZ_AVOID_LTE)

def mark_segmentation_failed(user_id):
    mark_stage_failed(user_id, ps.PipelineStages.TRIP_SEGMENTATION)

def get_time_range_for_sectioning(user_id):
    # Returns the time range for the trips that have not yet been converted into sections.
    # Note that this is a query against the trip database, so we cannot search using the
    # "write_ts" query. Instead, we change the query to be against the trip's end_ts
    tq = get_time_range_for_stage(user_id, ps.PipelineStages.SECTION_SEGMENTATION)
    tq.timeType = "data.end_ts"
    return tq

def mark_sectioning_done(user_id, last_trip_done):
    if last_trip_done is None:
        mark_stage_done(user_id, ps.PipelineStages.SECTION_SEGMENTATION, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.SECTION_SEGMENTATION,
                        last_trip_done.data.end_ts + END_FUZZ_AVOID_LTE)

def mark_sectioning_failed(user_id):
    mark_stage_failed(user_id, ps.PipelineStages.SECTION_SEGMENTATION)

def get_time_range_for_smoothing(user_id):
    # type: (uuid.UUID) -> emission.storage.timeseries.timequery.TimeQuery
    # Returns the time range for the trips that have not yet been converted into sections.
    # Note that this is a query against the trip database, so we cannot search using the
    # "write_ts" query. Instead, we change the query to be against the trip's end_ts
    """

    :rtype: emission.storage.timeseries.timequery.TimeQuery
    """
    tq = get_time_range_for_stage(user_id, ps.PipelineStages.JUMP_SMOOTHING)
    tq.timeType = "data.end_ts"
    return tq

def mark_smoothing_done(user_id, last_section_done):
    if last_section_done is None:
        mark_stage_done(user_id, ps.PipelineStages.JUMP_SMOOTHING, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.JUMP_SMOOTHING,
                        last_section_done.data.end_ts + END_FUZZ_AVOID_LTE)
        

def mark_smoothing_failed(user_id):
    mark_stage_failed(user_id, ps.PipelineStages.JUMP_SMOOTHING)

def get_time_range_for_mode_inference(user_id):
    tq = get_time_range_for_stage(user_id, ps.PipelineStages.MODE_INFERENCE)
    tq.timeType = "data.end_ts"
    return tq

def mark_mode_inference_complete(user_id):
    if last_section_done is None:
        mark_stage_done(user_id, ps.PipelineStages.MODE_INFERENCE, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.MODE_INFERENCE,
                        last_section_done.data.end_ts + END_FUZZ_AVOID_LTE)
        
def mark_mode_inference_failed(user_id):
    mark_stage_failed(user_id, ps.PipelineStages.MODE_INFERENCE)

def get_complete_ts(user_id):
    mode_infer_state = get_current_state(user_id, ps.PipelineStages.MODE_INFERENCE)
    if mode_infer_state is not None:
        return mode_infer_state.last_processed_ts
    else:
        cleaned_state = get_current_state(user_id, ps.PipelineStages.CLEAN_RESAMPLING)
        if cleaned_state is not None:
            return cleaned_state.last_processed_ts
        else:
            return None

def get_time_range_for_clean_resampling(user_id):
    # type: (uuid.UUID) -> emission.storage.timeseries.timequery.TimeQuery
    # Returns the time range for the trips that have not yet been converted into sections.
    # Note that this is a query against the trip database, so we cannot search using the
    # "write_ts" query. Instead, we change the query to be against the trip's end_ts
    """

    :rtype: emission.storage.timeseries.timequery.TimeQuery
    """
    tq = get_time_range_for_stage(user_id, ps.PipelineStages.CLEAN_RESAMPLING)
    tq.timeType = "data.end_ts"
    return tq

def mark_clean_resampling_done(user_id, last_section_done):
    if last_section_done is None:
        mark_stage_done(user_id, ps.PipelineStages.CLEAN_RESAMPLING, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.CLEAN_RESAMPLING,
                        last_section_done.data.enter_ts + END_FUZZ_AVOID_LTE)

def mark_clean_resampling_failed(user_id):
    mark_stage_failed(user_id, ps.PipelineStages.CLEAN_RESAMPLING)

def get_time_range_for_mode_inference(user_id):
    tq = get_time_range_for_stage(user_id, ps.PipelineStages.MODE_INFERENCE)
    tq.timeType = "data.end_ts"
    return tq

def mark_mode_inference_done(user_id, last_section_done):
    if last_section_done is None:
        mark_stage_done(user_id, ps.PipelineStages.MODE_INFERENCE, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.MODE_INFERENCE,
                        last_section_done.data.end_ts + END_FUZZ_AVOID_LTE)

def mark_mode_inference_failed(user_id):    
    mark_stage_failed(user_id, ps.PipelineStages.MODE_INFERENCE)

def get_time_range_for_output_gen(user_id):
    return get_time_range_for_stage(user_id, ps.PipelineStages.OUTPUT_GEN)

def mark_output_gen_done(user_id, last_processed_ts):
    if last_processed_ts is None:
        mark_stage_done(user_id, ps.PipelineStages.OUTPUT_GEN, None)
    else:
        mark_stage_done(user_id, ps.PipelineStages.OUTPUT_GEN,
                        last_processed_ts + END_FUZZ_AVOID_LTE)

def mark_output_gen_failed(user_id):
    mark_stage_failed(user_id, ps.PipelineStages.OUTPUT_GEN)

def mark_stage_done(user_id, stage, last_processed_ts):
    # We move failed entries to the error timeseries. So usercache runs never fail.
    curr_state = get_current_state(user_id, stage)
    assert(curr_state is not None)
    assert(curr_state.curr_run_ts is not None)
    curr_state.last_ts_run = curr_state.curr_run_ts
    # It is incorrect to assume that we have processed all the data until the
    # start of the last run. In particular, due to network connectivity or
    # other issues, it is possible that there is outstanding data on phones
    # that was collected before the last run started. And if we set this, then
    # that data will simply be skipped. The same logic applies to all
    # decorators that are based on client collected data (trip start ts, etc) -
    # it is only accurate for server generated data. So for maximum generality,
    # let's allow the stage to pass in last_processed_ts.
    if last_processed_ts is not None:
        logging.info("For stage %s, last_ts_processed = %s" %
                     (stage, pydt.datetime.utcfromtimestamp(last_processed_ts).isoformat()))
        curr_state.last_processed_ts = last_processed_ts
    else:
        logging.info("For stage %s, last_ts_processed is unchanged" % stage)
    curr_state.curr_run_ts = None
    logging.debug("About to save object %s" % curr_state)
    edb.save(edb.get_pipeline_state_db(), curr_state)
    logging.debug("After saving state %s, list is %s" % (curr_state,
        list(edb.get_pipeline_state_db().find({"user_id": user_id}))))

def mark_stage_failed(user_id, stage):
    curr_state = get_current_state(user_id, stage)
    assert(curr_state is not None)
    assert(curr_state.curr_run_ts is not None)
    # last_ts_run remains unchanged since this run did not succeed
    # the next query will start from the start_ts of this run
    # we also reset the curr_run_ts to indicate that we are not currently running
    curr_state.curr_run_ts = None
    logging.debug("About to save object %s" % curr_state)
    edb.save(edb.get_pipeline_state_db(), curr_state)
    logging.debug("After saving state %s, list is %s" % (curr_state,
        list(edb.get_pipeline_state_db().find({"user_id": user_id}))))

def get_time_range_for_stage(user_id, stage):
    """
    Returns the start ts and the end ts of the entries in the stage
    """
    curr_state = get_current_state(user_id, stage)

    if curr_state is None:
        start_ts = None
        curr_state = ps.PipelineState()
        curr_state.user_id = user_id
        curr_state.pipeline_stage = stage
        curr_state.curr_run_ts = None
        curr_state.last_processed_ts = None
        curr_state.last_ts_run = None
    else:
        start_ts = curr_state.last_processed_ts

    if start_ts is None:
        logging.info("For stage %s, start_ts is None" % stage)
    else:
        logging.info("For stage %s, start_ts = %s" % (stage, pydt.datetime.utcfromtimestamp(start_ts).isoformat()))

    assert curr_state.curr_run_ts is None, "curr_state.curr_run_ts = %s" % curr_state.curr_run_ts
    # Let's pick a point 5 secs in the past. If we don't do this, then we will
    # read all entries upto the current ts and this may lead to lost data. For
    # example, let us say that the current ts is t1. At the time that we read
    # the data, we have 4 entries for t1. By the time we finish copying, we
    # have 6 entries for t1, we will end up deleting all 6, which will lose 2
    # entries.
    end_ts = time.time() - END_FUZZ_AVOID_LTE

    ret_query = estt.TimeQuery("metadata.write_ts", start_ts, end_ts)

    curr_state.curr_run_ts = end_ts
    logging.debug("About to save object %s" % curr_state)
    edb.save(edb.get_pipeline_state_db(), curr_state)
    logging.debug("After saving state %s, list is %s" % (curr_state,
        list(edb.get_pipeline_state_db().find({"user_id": user_id}))))
    return ret_query

def get_current_state(user_id, stage):
    curr_state_doc = edb.get_pipeline_state_db().find_one({"user_id": user_id,
                                                            "pipeline_stage": stage.value})
    #logging.debug("returning curr_state_doc  %s for stage %s " % (curr_state_doc, stage))
    if curr_state_doc is not None:
        return ps.PipelineState(curr_state_doc)
    else:
        return None

