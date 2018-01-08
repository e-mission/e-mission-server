from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.core.wrapper.transition as et
import emission.net.usercache.formatters.common as fc
import attrdict as ad

state_map = {
    "STATE_START": et.State.START,
    "STATE_WAITING_FOR_TRIP_START": et.State.WAITING_FOR_TRIP_START,
    "STATE_ONGOING_TRIP": et.State.ONGOING_TRIP,
    "STATE_TRACKING_STOPPED": et.State.TRACKING_STOPPED
}

transition_map = {
    "booted": et.TransitionType.BOOTED,
    "T_INITIALIZE": et.TransitionType.INITIALIZE,
    "T_INIT_COMPLETE": et.TransitionType.INIT_COMPLETE,
    "T_EXITED_GEOFENCE": et.TransitionType.EXITED_GEOFENCE,
    "T_TRIP_STARTED": et.TransitionType.TRIP_STARTED,
    "T_RECEIVED_SILENT_PUSH": et.TransitionType.RECEIVED_SILENT_PUSH,
    "T_TRIP_END_DETECTED": et.TransitionType.TRIP_END_DETECTED,
    "T_TRIP_RESTARTED": et.TransitionType.TRIP_RESTARTED,
    "T_END_TRIP_TRACKING": et.TransitionType.END_TRIP_TRACKING,
    "T_DATA_PUSHED": et.TransitionType.DATA_PUSHED,
    "T_TRIP_ENDED": et.TransitionType.STOPPED_MOVING,
    "T_FORCE_STOP_TRACKING": et.TransitionType.STOP_TRACKING,
    "T_TRACKING_STOPPED": et.TransitionType.TRACKING_STOPPED,
    "T_VISIT_STARTED": et.TransitionType.VISIT_STARTED,
    "T_VISIT_ENDED": et.TransitionType.VISIT_ENDED,
    "T_NOP": et.TransitionType.NOP,
    "T_START_TRACKING": et.TransitionType.START_TRACKING
}

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id
    
    m = entry.metadata
    fc.expand_metadata_times(m)
    formatted_entry.metadata = m

    data = ad.AttrDict()
    data.curr_state = state_map[entry.data.currState].value
    logging.debug("Mapped %s -> %s" % (entry.data.currState, data.curr_state))
    
    # The iOS state diagram is significantly more complex than the android state diagram
    # So there are a lot more transitions. But some of the intermediate states are 
    # not interesting, so it seems like it should be possible to collapse them to the 
    # simple 2-state android state machine. But that requires looking at a window of
    # transitions, which we don't have here. Let's focus on simply mapping here and 
    # deal with collapsing later
    # data.transition_raw = entry.data.transition
    
    data.transition = transition_map[entry.data.transition].value
    logging.debug("Mapped %s -> %s" % (entry.data.transition, data.transition))
    
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
