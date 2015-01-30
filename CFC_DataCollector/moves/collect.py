from moves import Moves
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timedelta
import logging
import pytz
import json
from dateutil import parser
from get_database import get_mode_db, get_section_db, get_trip_db, get_moves_db
from time import sleep

config_data = json.load(open('config.json'))
log_base_dir = config_data['paths']['log_base_dir']
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                    filename="%s/moves_collect.log" % log_base_dir, level=logging.DEBUG)

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
   newSec['track_points'] = [{'track_location':{'type':'Point', 'coordinates':[point["lon"],point["lat"]]}, 'time':point["time"]}for point in sec_from_moves["trackPoints"]] if "trackPoints" in sec_from_moves else []
   newSec['section_start_point'] = {'type':'Point', 'coordinates':[sec_from_moves['trackPoints'][0]["lon"],sec_from_moves['trackPoints'][0]["lat"]]} if ("trackPoints" in sec_from_moves and len(sec_from_moves['trackPoints'])>0) else None
   newSec['section_end_point'] = {'type':'Point', 'coordinates':[sec_from_moves['trackPoints'][-1]["lon"],sec_from_moves['trackPoints'][-1]["lat"]]} if ("trackPoints" in sec_from_moves and len(sec_from_moves['trackPoints'])>0) else None

# The new trip will have some fields already filled in, notably, the ones
# that we get from the trip information. We fill in the other information here.
# Note that the function does not return anything since the new_trip has been created
# elsewhere and passed in SHANKARI: There are numerous nulll checks missing
# from this section (type, startTime, endTime), etc. However, they don't seem
# to occur in practice, so it doesn't appear to be so critical that we are able
# to fix it
def fillTripWithMovesData(trip_from_moves, new_trip):
  new_trip['type'] = trip_from_moves["type"]
  new_trip['trip_start_time'] = trip_from_moves["startTime"]
  new_trip['trip_end_time'] = trip_from_moves["endTime"]
  new_trip['trip_start_datetime'] = parser.parse(trip_from_moves["startTime"])
  new_trip['trip_end_datetime'] = parser.parse(trip_from_moves["endTime"])
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
  # First, we open a connection to the database
  Stage_Trips=get_trip_db()
  Stage_Sections=get_section_db()
  Modes=get_mode_db()

  #logging.debug(json.dumps(result))

  # It turns out that if a user just signed up, then they don't have any
  # trips for yesterday, and it is a dict instead of a list. In that case, we
  # just return early
  if type(result) is dict:
    logging.info("result is a dict, returning early")
    return

  if (len(result) == 0):
    logging.debug("result has length 0, returning early")
    return

  if result[0]["segments"] != None:
      number_of_trips=len(result[0]["segments"])
      logging.info("number of trips = %s" % number_of_trips)
      for trip in range(number_of_trips):
          seg_note=result[0]["segments"][trip]
          trip_id=seg_note["startTime"]
          _id_trip=str(user_uuid)+'_'+seg_note["startTime"]
          #logging.debug(json.dumps(seg_note))
          if "activities" in seg_note:
              number_of_sections=len(seg_note["activities"])
              for sectionindex in range(number_of_sections):
                  seg_act_note=seg_note["activities"][sectionindex]
                  # if the section is missing some data that we access later, then we skip it
                  if Stage_Sections.find({"$and":[ {"user_id":user_uuid},{"trip_id": trip_id},{"section_id": sectionindex}]}).count()==0:
                      try:
                          _id_section = str(user_uuid)+'_'+seg_act_note["startTime"]+'_'+str(sectionindex)
                          _mode = convertModeNameToIndex(Modes, seg_act_note["activity"])
                          sections_todo={'source':'Shankari',
                                         '_id':_id_section,
                                         'user_id': user_uuid,
                                         'trip_id':trip_id,
                                         'type':seg_note["type"],
                                         'section_id':sectionindex,
                                         'mode' : _mode,
                                          # SHANKARI: what does seg_act_note["manual"] mean?
                                         'confirmed_mode' :_mode if seg_act_note["manual"]==True else '',
                                         # 'group':int(''.join(map(str, [group['group_id'] for group in Groups.find({'group_name':seg_act_note["group"]})])))
                                         # if "group" in seg_act_note else '',
                                        }
                          fillSectionWithMovesData(seg_act_note, sections_todo)
                          # Now that we have created this section, let's insert it into the database
                          try:
                            logging.info("About to insert section with trip_id = %s,p section_id = %s, section_start_time = %s, type = %s and mode = %s " %
                                (trip_id, sectionindex, sections_todo['section_start_time'], seg_note["type"], seg_act_note["activity"]))
                            Stage_Sections.insert(sections_todo)
                          except DuplicateKeyError:
                            logging.warning("DuplicateKeyError, skipping insert %s" % sections_todo)
                            logging.warning("Existing section is %s" % Stage_Sections.find_one({"_id": _id_section}))
                      except KeyError:
                        logging.warning("Missing key, skipping section insert %s" % seg_act_note)

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
              logging.info("About to insert trip with trip_id = %s " % (trip_id))
              Stage_Trips.insert(trips_todo)
  else:
    logging.warning("result[0] = %s, does not contain any segments" % result[0])
    # # logging.debug(json.dumps(result[0]["segments"][0]["activities"][0]))
    # # logging.debug(json.dumps(result))

if __name__ == "__main__":
  collect()

