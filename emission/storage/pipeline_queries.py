import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ps
import emission.net.usercache.abstract_usercache as enua
import time

def mark_usercache_done():
    mark_stage_done(ps.PipelineStages.USERCACHE)

def get_time_range_for_usercache():
    get_time_range_for_stage(ps.PipelineStages.USERCACHE)

def mark_stage_done(stage):
    # We move failed entries to the error timeseries. So usercache runs never fail.
    curr_state = get_current_state(stage)
    assert(curr_state is not None)
    assert(curr_state.curr_run_ts is not None)
    curr_state.last_ts_run = curr_state.curr_run_ts
    curr_state.curr_run_ts = None
    edb.get_pipeline_state_db().save(curr_state)

def get_time_range_for_stage(stage):
    """
    Returns the start ts and the end ts of the entries in the stage
    """
    curr_state = get_current_state(stage)

    if curr_state is None:
        start_ts = None
        curr_state = ps.PipelineState()
        curr_state.pipeline_stage = stage
        curr_state.curr_run_ts = None
    else:
        start_ts = curr_state.last_ts_run

    assert(curr_state.curr_run_ts is None)

    end_ts = time.time() - 5 # Let's pick a point 5 secs in the past to avoid race conditions

    ret_query = enua.UserCache.TimeQuery("write_ts", start_ts, end_ts)

    curr_state.curr_run_ts = end_ts
    edb.get_pipeline_state_db().save(curr_state)
    return ret_query

def get_current_state(stage):
    curr_state_doc = edb.get_pipeline_state_db().find_one({"pipeline_stage": stage.value})
    if curr_state_doc is not None:
        return ps.PipelineState(curr_state_doc)
    else:
        return None
