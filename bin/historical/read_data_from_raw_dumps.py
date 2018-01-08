from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Read data from a temporary set of "dump" files that were stored on the
# mission server in the first half of 2015. These files were identical to the
# inputs from moves, except that they also had accuracy values for the points.
# The data was processed to generate the regular sections, but the regular
# sections did not store the accuracy, so the raw data was dumped into files as well.
# This was stopped once we switched our data collection to the new "sequence of
# values" format, which stored accuracies and everything else.

# The temporary dump files are now used to evaluate the efficiency of various
# smoothing algorithms. This script is not useful otherwise.

# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import json
from attrdict import AttrDict
from pymongo import MongoClient
import logging
from dateutil import parser
import time
import emission.core.get_database as edb

to_ts = lambda dt: time.mktime(dt.timetuple()) * 1000
logging.basicConfig(level=logging.DEBUG)

reconstructedTimeSeriesDb = edb.get_usercache_db()
reconstructedTripsDb = edb.get_section_db()

def load_file(curr_list):
    prevSection = None
    for entryJSON in curr_list:
        entryDict = AttrDict(entryJSON)
        if entryDict.type == "move":
            trip_id = entryDict.startTime
            for i, activity in enumerate(entryDict.activities):
                print("For trip id = %s, activity %s starts at %s" % (trip_id, i, activity.startTime))
                section = AttrDict()
                section.id = trip_id + "_"+ str(i)
                section.filter = "time"
                section.source = "raw_auto"
                section.start_time = activity.startTime
                section.end_time = activity.endTime
                if prevSection is None:
                    prevSection = section
                else:
                    section.prev_section = prevSection.id
                    prevSection.next_section = section.id
                    prevSection = section
                try:
                    section.start_ts = to_ts(parser.parse(activity.startTime))
                    section.end_ts = to_ts(parser.parse(activity.endTime))
                except ValueError:
                    if len(entryDict.activities) == 1:
                        print("One section case: Error parsing times %s or %s, using start and end points of the trip %s %s instead" % 
                              (activity.startTime, activity.endTime, entryDict.startTime, entryDict.endTime))
                        section.start_ts = to_ts(parser.parse(entryDict.startTime))
                        section.end_ts = to_ts(parser.parse(entryDict.endTime))
                    else:
                        if (i < (len(entryDict.activities) - 1)):
                            # This is not the last activity
                            print("Multi-section, not last section case: Error parsing times %s or %s, using start times of this and next section %s %s instead" %
                                  (activity.startTime, activity.endTime, activity.trackPoints[0].time,
                                   entryDict.activities[i+1].trackPoints[0].time))
                            section.start_ts = to_ts(parser.parse(activity.trackPoints[0].time))
                            section.end_ts = to_ts(parser.parse(entryDict.activities[i+1].trackPoints[0].time))
                        else:
                            # This is the last activity
                            print("Multi-section, last section case: Error parsing times %s or %s, using start times of this and next section %s %s instead" %
                                  (activity.startTime, activity.endTime, activity.trackPoints[0].time,
                                   entryDict.endTime))
                            section.start_ts = to_ts(parser.parse(activity.trackPoints[0].time))
                            section.end_ts = to_ts(parser.parse(entryDict.endTime))

                print("For section %s, inserting track points %s" % (section, len(activity.trackPoints)))
                reconstructedTripsDb.insert(section)
                for i, tp in enumerate(activity.trackPoints):
                    if "accuracy" not in tp:
                        print("Skipping point %d of section %s because it has no accuracy" % (i, section.id))
                        continue
                    point = AttrDict()
                    point.user_id = section.id
                    point.section_id = section.id

                    pointMetadata = AttrDict()
                    pointMetadata.key = "background/location"
                    pointMetadata.filter = "time"
                    point.metadata = pointMetadata

                    pointData = AttrDict()
                    pointData.idx = i
                    pointData.mLatitude = tp.lat
                    pointData.mLongitude = tp.lon
                    pointData.formatted_time = tp.time
                    pointData.mTime = to_ts(parser.parse(tp.time))
                    pointData.mAccuracy = tp.accuracy
                    point.data = pointData
                    # print "Got track point %s" % point
                    reconstructedTimeSeriesDb.insert(point)

if __name__ == '__main__':
    import os

    for dirname, dirnames, filenames in os.walk('/Users/shankari/e-mission/e-mission-data/our_collection_data/android/raw_save_our_data/'):
        # print dirname
        for filename in filenames:
            print("Loading %s%s" % (dirname, filename))
            load_file(json.load(open("%s%s" % (dirname, filename))))
