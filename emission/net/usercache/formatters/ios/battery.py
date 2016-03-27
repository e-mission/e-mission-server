import emission.core.wrapper.motionactivity as ecwa
import emission.net.usercache.formatters.common as fc
import attrdict as ad

def format(entry):
    formatted_entry = entry
    formatted_entry.data.ts = formatted_entry.metadata.write_ts
    formatted_entry.data.local_dt = formatted_entry.metadata.write_local_dt
    formatted_entry.data.fmt_time = formatted_entry.metadata.write_fmt_time
