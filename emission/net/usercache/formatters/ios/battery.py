import logging

import emission.core.wrapper.battery as ecwb
import emission.net.usercache.formatters.common as fc
import attrdict as ad

status_map = {
    0: ecwb.BatteryStatus.UNKNOWN,
    1: ecwb.BatteryStatus.DISCHARGING,
    2: ecwb.BatteryStatus.CHARGING,
    3: ecwb.BatteryStatus.FULL
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
    # ios sets data in a ratio between 0 and 1, so let's convert to percent to be consistent
    # with android
    data.battery_level_pct = entry.data.battery_level_ratio * 100

    data.battery_status = status_map[entry.data.battery_status].value
    logging.debug("Mapped %s -> %s" % (entry.data.battery_status, data.battery_status))

    data.ts = formatted_entry.metadata.write_ts
    data.local_dt = formatted_entry.metadata.write_local_dt
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    formatted_entry.data = data

    return formatted_entry
