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
import json
from attrdict import AttrDict
from pymongo import MongoClient
import logging
from dateutil import parser
import time

to_ts = lambda(dt): time.mktime(dt.timetuple())
logging.basicConfig(level=logging.DEBUG)

reconstructedTimeSeriesDb = MongoClient().Stage_database.Stage_Reconstructed_Timeseries
reconstructedTripsDb = MongoClient().Stage_database.Stage_Trips_db

def load_file(curr_list):
    for entryJSON in curr_list:
        entryDict = AttrDict(entryJSON)
        if entryDict.type == "move":
            trip_id = entryDict.startTime
            for i, activity in enumerate(entryDict.activities):
                print("For trip id = %s, activity %s starts at %s" % (trip_id, i, activity.startTime))
                section = AttrDict()
                section.id = trip_id + "_"+ str(i)
                try:
                    section.startTs = to_ts(parser.parse(activity.startTime))
                    section.endTs = to_ts(parser.parse(activity.endTime))
                except ValueError:
                    if len(entryDict.activities) == 1:
                        print("One section case: Error parsing times %s or %s, using start and end points of the trip %s %s instead" % 
                              (activity.startTime, activity.endTime, entryDict.startTime, entryDict.endTime))
                        section.startTs = to_ts(parser.parse(entryDict.startTime))
                        section.endTs = to_ts(parser.parse(entryDict.endTime))
                    else:
                        if (i < (len(entryDict.activities) - 1)):
                            # This is not the last activity
                            print("Multi-section, not last section case: Error parsing times %s or %s, using start times of this and next section %s %s instead" %
                                  (activity.startTime, activity.endTime, activity.trackPoints[0].time,
                                   entryDict.activities[i+1].trackPoints[0].time))
                            section.startTs = to_ts(parser.parse(activity.trackPoints[0].time))
                            section.endTs = to_ts(parser.parse(entryDict.activities[i+1].trackPoints[0].time))
                        else:
                            # This is the last activity
                            print("Multi-section, last section case: Error parsing times %s or %s, using start times of this and next section %s %s instead" %
                                  (activity.startTime, activity.endTime, activity.trackPoints[0].time,
                                   entryDict.endTime))
                            section.startTs = to_ts(parser.parse(activity.trackPoints[0].time))
                            section.endTs = to_ts(parser.parse(entryDict.endTime))

                section.startTime = activity.startTime
                section.endTime = activity.endTime
                print("For section %s, inserting track points %s" % (section, len(activity.trackPoints)))
                reconstructedTripsDb.insert(section)
                for i, tp in enumerate(activity.trackPoints):
                    if "accuracy" not in tp:
                        print "Skipping point %d of section %s because it has no accuracy" % (i, section.id)
                        continue
                    point = AttrDict()
                    point.idx = i
                    point.mLatitude = tp.lat
                    point.mLongitude = tp.lon
                    point.formatted_time = tp.time
                    point.mTime = to_ts(parser.parse(tp.time))
                    point.mAccuracy = tp.accuracy
                    # print "Got track point %s" % point
                    reconstructedTimeSeriesDb.insert(point)

if __name__ == '__main__':
    import os

    for dirname, dirnames, filenames in os.walk('/Users/shankari/e-mission/e-mission-data/our_collection_data/android/raw_save_our_data/'):
        # print dirname
        for filename in filenames:
            print("Loading %s%s" % (dirname, filename))
            load_file(json.load(open("%s%s" % (dirname, filename))))
