# Regenerate score and footprint information for a set of users. In the
# production system, we only calculate the score for users who were part of the
# "game" group and the footprint for users who were part of the "data" group.
# When we want to analyse their engagement, though, we want to compare data
# across all groups. This fills in the gaps for the results that were not
# calculated initially.
# NOTE: The following values are hardcoded here. If this script is used on an
# ongoing basis, we should make them available as parameters somehow.
#

CFC_WEBAPP_PATH = "/Users/shankari/e-mission/e-mission-server/CFC_WebApp/"
START_DATE = "2014/12/1"
END_DATE = "2014/12/31"
UUID_GROUP_CSV = "/Users/shankari/e-mission/e-mission-data/client_stats/EMissions_Survey_i232_Fall_2014.csv_with_group"

import pandas as pd
from uuid import UUID

import sys
sys.path.append(CFC_WEBAPP_PATH)

from get_database import get_section_db, get_result_stats_db

from clients.data import data
from clients.gamified import gamified
from clients.choice import choice

from main import stats

from dao.user import User

import datetime as pydt
import time as pytime

def fixClientTimestamps(origDate):
    """
        Note that all the stats will have the current timestamp in them. But
        in order to run analyses, we need to fix them so that the timestamps
        are from the correct date. This method fixes all timestamps from the past hour
        (recently created and not yet fixed) to be from the origDate specified
    """
    utctimestamp = lambda date: (date - pydt.datetime.utcfromtimestamp(0)).total_seconds()

    origTs = utctimestamp(origDate)
    now = pydt.datetime.utcnow()
    hourago = now + pydt.timedelta(hours = -1)
    get_result_stats_db().update({"ts": {"$gt": utctimestamp(hourago), "$lt": utctimestamp(now)}}, {"$set": {"ts": origTs}})

def regenerateData():
    dec_days = pd.date_range(START_DATE, END_DATE)
    uuidDF = pd.read_csv(UUID_GROUP_CSV)

    for date in dec_days:
        print "Generating scores for day %s" % date
        for uuidStr in uuidDF.uuid:
            uuid = UUID(uuidStr)
            print "Generating scores for uuid %s " % uuid
            choice.runBackgroundTasksForDay(uuid, date)

            # Get the number of trips for a day
            dateQuery = {"section_start_datetime": {"$gt": date, "$lt": date + pydt.timedelta(days=1)}}
            tripsForDay = get_section_db().find({"$and": [{"user_id" : uuid}, dateQuery]}).count()
            stats.storeServerEntry(uuid, stats.STAT_TRIP_MGR_TRIPS_FOR_DAY, pytime.time(), tripsForDay)
        fixClientTimestamps(date)

if __name__ == '__main__':
    import sys
    regenerateData()
