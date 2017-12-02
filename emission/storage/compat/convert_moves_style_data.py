import json
import attrdict as ad
from dateutil import parser
import logging
import time
import pandas as pd

to_ts = lambda dt: time.mktime(dt.timetuple())

def convert_track_point(tp):
    point = ad.AttrDict()
    tpDict = ad.AttrDict(tp)
    point.mTime = to_ts(parser.parse(tpDict.time))
    point.mLatitude = tpDict.track_location.coordinates[1]
    point.mLongitude = tpDict.track_location.coordinates[0]
    point.mAccuracy = 0
    return point

def convert_track_point_array(tp_array):
    return [convert_track_point(tp) for tp in tp_array]

def convert_track_point_array_to_df(tp_array):
    point_array = convert_track_point_array(tp_array)
    return pd.DataFrame(point_array)
