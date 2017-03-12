import logging

import emission.core.wrapper.transition as et
import emission.net.usercache.formatters.common as fc
import attrdict as ad

state_map = {
    "unknown": et.State.UNKNOWN,
    "local.state.start": et.State.START,
    "local.state.waiting_for_trip_start": et.State.WAITING_FOR_TRIP_START,
    "local.state.ongoing_trip": et.State.ONGOING_TRIP,
    "local.state.tracking_stopped": et.State.TRACKING_STOPPED
}

transition_map = {
    "booted": et.TransitionType.BOOTED,
    "local.transition.initialize": et.TransitionType.INITIALIZE,
    "local.transition.exited_geofence": et.TransitionType.EXITED_GEOFENCE,
    "local.transition.stopped_moving": et.TransitionType.STOPPED_MOVING,
    "local.transition.stop_tracking": et.TransitionType.STOP_TRACKING,
    "local.transition.start_tracking": et.TransitionType.START_TRACKING,
    "local.transition.tracking_error": et.TransitionType.TRACKING_ERROR
}

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id
    
    m = entry.metadata
    if "time_zone" not in m:
        m.time_zone = "America/Los_Angeles" 
    logging.debug("Timestamp conversion: %s -> %s done" % (entry.metadata.write_ts, m.write_ts))
    fc.expand_metadata_times(m)
    formatted_entry.metadata = m

    data = ad.AttrDict()
    data.curr_state = state_map[entry.data.currState].value
    logging.debug("Mapped %s -> %s" % (entry.data.currState, data.curr_state))
    data.transition = transition_map[entry.data.transition].value
    if "ts" not in data:
        data.ts = formatted_entry.metadata.write_ts
        logging.debug("No existing timestamp, copyied from metadata%s" % data.ts)
        data.local_dt = formatted_entry.metadata.write_local_dt
        data.fmt_time = formatted_entry.metadata.write_fmt_time
    else:
        logging.debug("Retaining existing timestamp %s" % data.ts)
        fc.expand_data_times(data, metadata)

    formatted_entry.data = data

    return formatted_entry
