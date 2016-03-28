import emission.core.wrapper.battery as ecwb
import emission.net.usercache.formatters.common as fc
import attrdict as ad

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

    if entry.data.battery_status == 0:
        data.battery_status = ecwb.BatteryStatus.UNKNOWN
    elif entry.data.battery_status == 1:
        data.battery_status = ecwb.BatteryStatus.DISCHARGING
    elif entry.data.battery_status == 2:
        data.battery_status = ecwb.BatteryStatus.CHARGING
    elif entry.data.battery_status == 3:
        data.battery_status = ecwb.BatteryStatus.FULL

    data.ts = formatted_entry.metadata.write_ts
    data.local_dt = formatted_entry.metadata.write_local_dt
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    formatted_entry.data = data

    return formatted_entry
