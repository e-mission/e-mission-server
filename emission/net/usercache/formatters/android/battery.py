import logging
import copy

import emission.core.wrapper.battery as ecwb
import emission.net.usercache.formatters.common as fc
import attrdict as ad

status_map = {
    1: ecwb.BatteryStatus.UNKNOWN,
    2: ecwb.BatteryStatus.CHARGING,
    3: ecwb.BatteryStatus.DISCHARGING,
    4: ecwb.BatteryStatus.NOT_CHARGING,
    5: ecwb.BatteryStatus.FULL
}

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    # adds the python datetime and fmt_time entries. important for future searches!
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = ad.AttrDict()
    # There are lots of fields incoming on android, so instead of copying each
    # one over, let's just copy the whole thing
    data = copy.copy(entry.data)

    data.battery_status = status_map[entry.data.battery_status].value
    logging.debug("Mapped %s -> %s" % (entry.data.battery_status, data.battery_status))

    data.ts = formatted_entry.metadata.write_ts
    data.local_dt = formatted_entry.metadata.write_local_dt
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    formatted_entry.data = data


    return formatted_entry
