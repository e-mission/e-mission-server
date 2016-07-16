# Standard imports
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta
import logging
import pytz
import json
from dateutil import parser
from time import sleep
import numpy as np
from sklearn import linear_model
import math

# Our imports
from sdk import Moves
from emission.core.get_database import get_mode_db, get_section_db, get_trip_db, get_moves_db

def collect():
  key_file = open('keys.json')
  key_data = json.load(key_file)

  m = Moves(client_id = key_data["client_id"],
            client_secret = key_data["client_secret"],
            redirect_url = key_data["redirect_url"])

  for (i, user) in enumerate(get_moves_db().find()):
    if i != 0 and i % 10 == 0:
        logging.info("Finished 10 users, sleeping for two minutes...")
        sleep(2 * 60)
    access_token = user['access_token']
    if access_token == "Ignore_me":
      logging.info("Found entry to ignore for user %s" % user)
      continue
    logging.info("%s: Getting data for user %s with token %s" % (datetime.now(), user['our_uuid'], user['access_token']))
    user_uuid=user['our_uuid']
    # logging.debug(user_uuid)
    profile = m.get_profile(token = access_token)
    # logging.debug(profile)
    tzStr = getTimezone(profile)
    tz = pytz.timezone(tzStr)

    # So the documentation both here:
    # http://www.enricozini.org/2009/debian/using-python-datetime/
    # and here:
    # http://pytz.sourceforge.net/
    # says that we should use utcnow() because now() is not reliable.
    # unfortunately, using utcnow() on my desktop, where the timezone is set, causes the localized time to be wrong
    # now() works
    # testing with now() on the server as well
    # now() didn't work on the server either
    # in fact, pytz localize() (i.e. tz.localize(datetime.now()) seems broken
    # fortunately, astimezone() seems to work, so we are using it here

    dateFormat = "%Y-%m-%d"
    # endPoint = "user/storyline/daily/%s?trackPoints=true" % ('20140303')
    endPoint_today = "user/storyline/daily/%s?trackPoints=true" % (datetime.now(pytz.utc).astimezone(tz).strftime(dateFormat))
    endPoint_yesterday="user/storyline/daily/%s?trackPoints=true" % ((datetime.now(pytz.utc).astimezone(tz)-timedelta(1)).strftime(dateFormat))
    result_today = m.get(token=access_token, endpoint = endPoint_today)
    result_yesterday=m.get(token=access_token, endpoint = endPoint_yesterday)
    logging.debug("result_today.type = %s, result_yesterday = %s" % (type(result_today), type(result_yesterday)))
    # logging.debug(json.dumps(result_yesterday))

    # result=result_today+result_yesterday
    processResult(user_uuid, result_today)
    processResult(user_uuid, result_yesterday)

def getTimezone(profileObj):
  if profileObj == None:
    return "America/Los_Angeles"
  else:
    return profileObj["profile"]["currentTimeZone"]["id"]

# The new section will have some fields already filled in, notably, the ones
# that we get from the trip information. We fill in the other information here.
# Note that the function does not return anything since the new_sec has been created
# elsewhere and passed in
def fillSectionWithMovesData(sec_from_moves, newSec):
   newSec['manual'] = sec_from_moves["manual"] if "manual" in sec_from_moves else None
   newSec['section_start_time'] = sec_from_moves["startTime"] if "startTime" in sec_from_moves else ''
   newSec['section_end_time'] = sec_from_moves["endTime"] if "endTime" in sec_from_moves else ''
   newSec['section_start_datetime'] = parser.parse(sec_from_moves["startTime"]) if "startTime" in sec_from_moves else None
   newSec['section_end_datetime'] = parser.parse(sec_from_moves["endTime"]) if "endTime" in sec_from_moves else None
   # it's easy to do the query with this:
   # start = datetime(2014, 3, 20)
   # end = datetime(2014, 3, 21)
   # for section in Stage_Sections.find({"date": {"$gte": start, "$lt": end}}):
   #     print(section)
   newSec['duration'] = sec_from_moves["duration"] if "duration" in sec_from_moves else None
   newSec['distance'] = sec_from_moves["distance"] if "distance" in sec_from_moves else None
   newSec['original_points'] = [{'track_location':{'type':'Point', 'coordinates':[point["lon"],point["lat"]]}, 'time':point["time"]}for point in sec_from_moves["trackPoints"]] if "trackPoints" in sec_from_moves else []
   newSec['track_points'] = _cleanGPSData(newSec['original_points'])
   newSec['section_start_point'] = {'type':'Point', 'coordinates':[sec_from_moves['trackPoints'][0]["lon"],sec_from_moves['trackPoints'][0]["lat"]]} if ("trackPoints" in sec_from_moves and len(sec_from_moves['trackPoints'])>0) else None
   newSec['section_end_point'] = {'type':'Point', 'coordinates':[sec_from_moves['trackPoints'][-1]["lon"],sec_from_moves['trackPoints'][-1]["lat"]]} if ("trackPoints" in sec_from_moves and len(sec_from_moves['trackPoints'])>0) else None

def calDistance(point1, point2):

    earthRadius = 6371000
    # SHANKARI: Why do we have two calDistance() functions?
    # Need to combine into one
    # points are now in geojson format (lng,lat)
    dLat = math.radians(point1[1]-point2[1])
    dLon = math.radians(point1[0]-point2[0])
    lat1 = math.radians(point1[1])
    lat2 = math.radians(point2[1])

    a = (math.sin(dLat/2) ** 2) + ((math.sin(dLon/2) ** 2) * math.cos(lat1) * math.cos(lat2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = earthRadius * c

    return d

def max_Distance(points):
    # 'track_points':[{'track_location':{'type':'Point', 'coordinates':[point["lat"],point["lon"]]}, 'time':point["time"]}for point in seg_act_note["trackPoints"]] if "trackPoints" in seg_act_note else []}
    num_pts=len(points)
    max_d=0
    for i in range(num_pts):
        for j in range(i+1,num_pts):
            max_d=max(max_d,calDistance(points[i]['track_location']['coordinates'], points[j]['track_location']['coordinates']))
    return max_d

def travel_time(time1,time2):
    start_time=parser.parse(time1)
    end_time=parser.parse(time2)
    travel_time = end_time-start_time
    return travel_time.seconds

def label_filtered_section(section):
    minimum_travel_time=120
    minimum_travel_distance=200
    Modes=get_mode_db()
    Sections=get_section_db()

    is_retained = False
    # logging.debug("Appending %s" % json.dumps(section))
    if section['section_start_time']!=''and section['section_end_time']!=''and len(section['track_points'])>=2:
        if travel_time(section['section_start_time'],section['section_end_time']) >= minimum_travel_time and \
                        max_Distance(section['track_points']) >= minimum_travel_distance:
            section['mode']=''.join(mode['mode_name'] for mode in Modes.find({"mode_id":section['mode']})) \
                if type(section['mode'])!=type('aa') else section['mode']
            is_retained =  True
        else:
            section['type'] ='not a trip'
    elif section['section_start_time']!=''and section['section_end_time']!=''and len(section['track_points'])<2:
        if travel_time(section['section_start_time'],section['section_end_time']) >= minimum_travel_time:
            section['mode']=''.join(mode['mode_name'] for mode in Modes.find({"mode_id":section['mode']})) \
                if type(section['mode'])!=type('aa') else section['mode']
            is_retained =  True
        else:
            section['type'] ='not a trip'
    elif (section['section_start_time']==''or section['section_end_time']=='') and len(section['track_points'])>=2:
        if max_Distance(section['track_points']) >= minimum_travel_distance:
            section['mode']=''.join(mode['mode_name'] for mode in Modes.find({"mode_id":section['mode']})) \
                if type(section['mode'])!=type('aa') else section['mode']
            is_retained =  True
        else:
            section['type'] ='not a trip'
    else:
        section['type'] ='not complete information'
    section['retained'] = is_retained

# The new trip will have some fields already filled in, notably, the ones
# that we get from the trip information. We fill in the other information here.
# Note that the function does not return anything since the new_trip has been created
# elsewhere and passed in SHANKARI: There are numerous nulll checks missing
# from this section (type, startTime, endTime), etc. However, they don't seem
# to occur in practice, so it doesn't appear to be so critical that we are able
# to fix it
def fillTripWithMovesData(trip_from_moves, new_trip):
  # logging.debug("trip_from_moves = %s" % trip_from_moves)
  new_trip['type'] = trip_from_moves["type"] if 'type' in trip_from_moves else "unknown"
  new_trip['trip_start_time'] = trip_from_moves["startTime"] if "startTime" in trip_from_moves else ""
  new_trip['trip_end_time'] = trip_from_moves["endTime"] if "endTime" in trip_from_moves else "" 
  new_trip['trip_start_datetime'] = parser.parse(trip_from_moves["startTime"]) if "startTime" in trip_from_moves else None
  new_trip['trip_end_datetime'] = parser.parse(trip_from_moves["endTime"]) if "endTime" in trip_from_moves else None
  new_trip['place'] = {'place_id':    trip_from_moves["place"]["id"],
                      'place_type':   trip_from_moves["place"]["type"],
                      'place_location':{'type':'Point',
                                        'coordinates':[trip_from_moves["place"]["location"]["lon"],
                                                       trip_from_moves["place"]["location"]["lat"]]}} \
                      if "place"in trip_from_moves else {}
  new_trip['last_update_time'] = trip_from_moves["lastUpdate"] if "lastUpdate" in trip_from_moves else ""

# Returns the integer version of the modeName, i.e.
# "walk" = 1, "run" = 2, "bike" = 3, etc
# SHANKARI: Think about whether we should continue converting to int or whether
# we should have a string representation. It looks like we support both int and
# string anyway now. How will we allow user editable modes going forward?

def convertModeNameToIndex(ModeDb, modeName):
  return int(''.join(map(str, [mode['mode_id'] for mode in ModeDb.find({'mode_name':modeName})]))) \
    if ModeDb.find({'mode_name':modeName}).count()!=0 else modeName

def processResult(user_uuid, result):
  # logging.debug(json.dumps(result))

  # It turns out that if a user just signed up, then they don't have any
  # trips for yesterday, and it is a dict instead of a list. In that case, we
  # just return early
  if type(result) is dict:
    logging.info("result is a dict, returning early")
    return

  if (len(result) == 0):
    logging.debug("result has length 0, returning early")
    return

  trip_array = result[0]["segments"]
  if trip_array != None:
    processTripArray(user_uuid, trip_array)
  else:
    logging.warning("result[0] = %s, does not contain any segments" % result[0])
    # # logging.debug(json.dumps(result[0]["segments"][0]["activities"][0]))
    # # logging.debug(json.dumps(result))

def processTripArray(user_uuid, trip_array):
  # First, we open a connection to the database
  Stage_Trips=get_trip_db()
  Stage_Sections=get_section_db()
  Modes=get_mode_db()

  number_of_trips=len(trip_array)
  logging.info("number of trips = %s" % number_of_trips)
  for trip in range(number_of_trips):
      seg_note=trip_array[trip]
      trip_id=seg_note["startTime"]
      _id_trip=str(user_uuid)+'_'+seg_note["startTime"]
      #logging.debug(json.dumps(seg_note))
      if "activities" in seg_note:
          number_of_sections=len(seg_note["activities"])
          logging.debug("number of sections = %s" % number_of_sections)
          for sectionindex in range(number_of_sections):
              seg_act_note=seg_note["activities"][sectionindex]
              # if the section is missing some data that we access later, then we skip it
              if Stage_Sections.find({"$and":[ {"user_id":user_uuid},{"trip_id": trip_id},{"section_id": sectionindex}]}).count()==0:
                  try:
                      _id_section = str(user_uuid)+'_'+seg_act_note["startTime"]+'_'+str(sectionindex)
                      _mode = convertModeNameToIndex(Modes, seg_act_note["activity"])
                      isManual = seg_act_note["manual"] if "manual" in seg_act_note else False
                      sections_todo={'source':'Shankari',
                                     '_id':_id_section,
                                     'user_id': user_uuid,
                                     'trip_id':trip_id,
                                     'type':seg_note["type"],
                                     'section_id':sectionindex,
                                     'mode' : _mode,
                                      # SHANKARI: what does seg_act_note["manual"] mean?
                                     'confirmed_mode' :_mode if isManual else '',
                                     # 'group':int(''.join(map(str, [group['group_id'] for group in Groups.find({'group_name':seg_act_note["group"]})])))
                                     # if "group" in seg_act_note else '',
                                    }
                      fillSectionWithMovesData(seg_act_note, sections_todo)
                      label_filtered_section(sections_todo)
                      # Now that we have created this section, let's insert it into the database
                      try:
                        logging.info("About to insert section with trip_id = %s,p section_id = %s, section_start_time = %s, type = %s and mode = %s " %
                            (trip_id, sectionindex, sections_todo['section_start_time'], seg_note["type"], seg_act_note["activity"]))
                        Stage_Sections.insert(sections_todo)
                      except DuplicateKeyError:
                        logging.warning("DuplicateKeyError, skipping insert %s" % sections_todo)
                        logging.warning("Existing section is %s" % Stage_Sections.find_one({"_id": _id_section}))

                  except KeyError, e:
                    logging.warning("Missing key %s, skipping section insert %s" % (e, seg_act_note))

                    insertedSectionCount = Stage_Sections.find({"$and" : [{"user_id": user_uuid},
                                                                          {"trip_id": trip_id},
                                                                          {"section_id": sectionindex}]}).count()
                    if insertedSectionCount == 0:
                         logging.error("Insert appears to have FAILED. No entry for %s, %s, %s found" %
                                (user_uuid, trip_id, sectionindex))
              else:
                 logging.debug("Found existing matching entry for %s, %s, %s, skipping entry" %
                        (user_uuid, trip_id, sectionindex))

      # Insert a trip if it doesn't already exist
      # SHANKARI: What if we get other sections for a trip later? When do we update the trip?
      # Do we even need to keep this linkage, with the concomittant
      # management cost if we can just find all sections by trip_id
      # instead? How expensive is the query?
      if Stage_Trips.find({"$and":[ {"user_id":user_uuid},{"trip_id": trip_id}]}).count()==0:
          trips_todo={ 'source':'Shankari',
                       '_id':_id_trip,
                       'user_id': user_uuid,
                       'trip_id':trip_id,
                       'sections':[sections['section_id'] for sections in Stage_Sections.find({"$and":[{"user_id":user_uuid}, {"trip_id":trip_id}]})]}
          fillTripWithMovesData(seg_note, trips_todo)
          logging.info("About to insert trip with trip_id = %s " % (trip_id))
          Stage_Trips.insert(trips_todo)
      else:
          logging.debug("Found existing trip with trip_id = %s " % (trip_id))

def _cleanGPSData(old_points):
    if len(old_points) > 10:
        points = np.array([[(datetime.strptime(point['time'].split('-')[0], "%Y%m%dT%H%M%S") - datetime(1970,1,1)).total_seconds(), 
            point["track_location"]["coordinates"][0], point["track_location"]["coordinates"][1]] for point in old_points])
        time_stamp = points[:,0].reshape(len(points[:,0]), 1)
        lon_lat = points[:,1:]
        model_ransac = linear_model.RANSACRegressor(linear_model.LinearRegression(), min_samples = 10)
        model_ransac.fit(lon_lat, time_stamp)
        inlier_mask = model_ransac.inlier_mask_
        outlier_mask = np.logical_not(inlier_mask)
        remove = [index for index,v in enumerate(outlier_mask) if v]
        return [v for j,v in enumerate(old_points) if j not in frozenset(remove)]
    return old_points

if __name__ == "__main__":
    config_data = json.load(open('conf/net/api/webserver.conf'))
    log_base_dir = config_data['paths']['log_base_dir']
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    filename="%s/moves_collect.log" % log_base_dir, level=logging.DEBUG)

    collect()

