from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.core.wrapper.motionactivity as ecwa
import emission.net.usercache.formatters.common as fc
import attrdict as ad

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    if "time_zone" not in metadata:
        metadata.time_zone = "America/Los_Angeles" 
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    #logging.info('*** Motion Data write_ts: %d' % metadata.write_ts)
   
    if 'ts' not in entry.data:
        # old style entries
        data = ad.AttrDict()
    else:
        data = entry.data

    if 'agb' in entry.data:
        data.type = ecwa.MotionTypes(entry.data.agb).value
    elif 'zzaEg' in entry.data:
        data.type = ecwa.MotionTypes(entry.data.zzaEg).value
    elif 'zzbjA' in entry.data:
        data.type = ecwa.MotionTypes(entry.data.zzbjA).value
    elif 'ajO' in entry.data:
        data.type = ecwa.MotionTypes(entry.data.ajO).value
    elif 'zzaKM' in entry.data:
        data.type = ecwa.MotionTypes(entry.data.zzaKM).value
    elif 'zzbhB' in entry.data:
        data.type = ecwa.MotionTypes(entry.data.zzbhB).value


    if 'agc' in entry.data:
        data.confidence = entry.data.agc
    elif 'zzaEh' in entry.data:
        data.confidence = entry.data.zzaEh
    elif 'zzbjB' in entry.data:
        data.confidence = entry.data.zzbjB
    elif 'ajP' in entry.data:
        data.confidence = entry.data.ajP
    elif 'zzaKN' in entry.data:
        data.confidence = entry.data.zzaKN
    elif 'zzbhC' in entry.data:
        data.confidence = entry.data.zzbhC

    if 'ts' not in entry.data:
        data.ts = formatted_entry.metadata.write_ts

    data.local_dt = formatted_entry.metadata.write_local_dt
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    formatted_entry.data = data

    return formatted_entry
