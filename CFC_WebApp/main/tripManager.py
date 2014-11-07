import logging
from dateutil import parser
import math
import json
from get_database import get_mode_db, get_section_db
from datetime import datetime, timedelta

def travel_time(time1,time2):
    start_time=parser.parse(time1)
    end_time=parser.parse(time2)
    travel_time = end_time-start_time
    return travel_time.seconds

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

def filter_unclassifiedSections(UnclassifiedSections):
    minimum_travel_time=120
    minimum_travel_distance=200
    Modes=get_mode_db()
    Sections=get_section_db()
    filtered_Sections=[]
    for section in UnclassifiedSections:
        # logging.debug("Appending %s" % json.dumps(section))
        if section['section_start_time']!=''and section['section_end_time']!=''and len(section['track_points'])>=2:
            if travel_time(section['section_start_time'],section['section_end_time']) >= minimum_travel_time and \
                            max_Distance(section['track_points']) >= minimum_travel_distance:
                section['mode']=''.join(mode['mode_name'] for mode in Modes.find({"mode_id":section['mode']})) \
                    if type(section['mode'])!=type('aa') else section['mode']
                filtered_Sections.append(section)
            else:
                Sections.update({"$and":[{'source':'Shankari'},
                                     {'user_id':section['user_id']},
                                     {'trip_id': section['trip_id']},
                                     {'section_id': section['section_id']}]},{"$set":{'type':'not a trip'}})
        elif section['section_start_time']!=''and section['section_end_time']!=''and len(section['track_points'])<2:
            if travel_time(section['section_start_time'],section['section_end_time']) >= minimum_travel_time:
                section['mode']=''.join(mode['mode_name'] for mode in Modes.find({"mode_id":section['mode']})) \
                    if type(section['mode'])!=type('aa') else section['mode']
                filtered_Sections.append(section)
            else:
                Sections.update({"$and":[{'source':'Shankari'},
                                     {'user_id':section['user_id']},
                                     {'trip_id': section['trip_id']},
                                     {'section_id': section['section_id']}]},{"$set":{'type':'not a trip'}})
        elif (section['section_start_time']==''or section['section_end_time']=='') and len(section['track_points'])>=2:
            if max_Distance(section['track_points']) >= minimum_travel_distance:
                section['mode']=''.join(mode['mode_name'] for mode in Modes.find({"mode_id":section['mode']})) \
                    if type(section['mode'])!=type('aa') else section['mode']
                filtered_Sections.append(section)
            else:
                Sections.update({"$and":[{'source':'Shankari'},
                                     {'user_id':section['user_id']},
                                     {'trip_id': section['trip_id']},
                                     {'section_id': section['section_id']}]},{"$set":{'type':'not a trip'}})
        else:
            Sections.update({"$and":[{'source':'Shankari'},
                                     {'user_id':section['user_id']},
                                     {'trip_id': section['trip_id']},
                                     {'section_id': section['section_id']}]},{"$set":{'type':'not complete information'}})
    return filtered_Sections

# TODO: Mogeng fix me the right way
def stripoutNonSerializable(sectionList):
    strippedList = []
    for section in sectionList:
        del section['section_start_datetime']
        del section['section_end_datetime']
        del section['section_start_point']
        del section['section_end_point']
        del section['user_id']
        strippedList.append(section)
    return strippedList

def queryUnclassifiedSections(uuid):
    now = datetime.now()
    weekago = now - timedelta(weeks = 1)

    user_uuid=uuid
    clientSpecificQuery = getClientSpecificQueryFilter(user_uuid)
    Sections=get_section_db()
    logging.debug('section.count = %s' % Sections.count())
    # Theoretically, we shouldn't need the 'predicted_mode' code because we
    # predict values right after reading the trips from moves. However, there
    # is still a small race window in which we are reading trips for other
    # users and haven't yet run the classifier. As we get more users, this
    # window can only grow, and it is easy to handle it, so let's just do so now.
    defaultQueryList = [ {'source':'Shankari'},
                         {'user_id':user_uuid},
                         {'predicted_mode': { '$exists' : True } },
                         {'confirmed_mode': ''},
                         { 'type': 'move' },
                         {'section_end_datetime': {"$gt": weekago}}]
    completeQueryList = defaultQueryList + clientSpecificQuery
    unclassifiedSections=Sections.find({"$and": completeQueryList})

    # totalUnclassifiedSections are for debugging only, can remove after we know that this works well
    totalUnclassifiedSections=Sections.find({"$and":[ {'source':'Shankari'},
                                                 {'user_id':user_uuid},
                                                 {'confirmed_mode': ''},
                                                 { 'type': 'move' }]})
    logging.debug('Unsec.count = %s' % unclassifiedSections.count())
    logging.debug('Total Unsec.count = %s' % totalUnclassifiedSections.count())
    return unclassifiedSections

def getClientSpecificQueryFilter(uuid):
    from dao.user import User
    from dao.client import Client

    studyList = User.fromUUID(uuid).getStudy()
    if len(studyList) > 0:
      assert(len(studyList) == 1)
      study = studyList[0]
      client = Client(study)
      return client.getSectionFilter(uuid)
    else:
      # User is not part of any study, so no additional filtering is needed
      return []

def getUnclassifiedSections(uuid):
    return_dict={}
    unclassifiedSections = queryUnclassifiedSections(uuid)
    filtered_UnclassifiedSections=filter_unclassifiedSections(unclassifiedSections)
    logging.debug("filtered_UnclassifiedSections = %s" % len(filtered_UnclassifiedSections))
    stripped_filtered_UnclassifiedSections = stripoutNonSerializable(filtered_UnclassifiedSections)
    logging.debug("stripped_filtered_UnclassifiedSections = %s" % len(stripped_filtered_UnclassifiedSections))
    return_dict["sections"]=stripped_filtered_UnclassifiedSections
    return return_dict

def setSectionClassification(uuid, userClassifications):
    number_class_sec=len(userClassifications)
    user_uuid=uuid
    Sections=get_section_db()
    Modes=get_mode_db()
    logging.debug("userClassifications = %s" % userClassifications)
    logging.debug("number_class_sec = %s" % number_class_sec)
    if number_class_sec!=0:
        logging.debug("number_class_sec = %s" % number_class_sec)
        for sectionindex in range(number_class_sec):
            if userClassifications[sectionindex]['userMode']=='not a trip':
                logging.debug("usermode = %s" % userClassifications[sectionindex]['userMode'])
                Sections.update({"$and":[{'source':'Shankari'},
                                     {'user_id': user_uuid},
                                     {'trip_id': userClassifications[sectionindex]['trip_id']},
                                     {'section_id': int(userClassifications[sectionindex]['section_id'])}]},
                                                        {"$set":{'type':userClassifications[sectionindex]['userMode']}})
                logging.debug("update done" )
            else:
                Sections.update({"$and":[{'source':'Shankari'},
                                         {'user_id': user_uuid},
                                         {'trip_id': userClassifications[sectionindex]['trip_id']},
                                         {'section_id': int(userClassifications[sectionindex]['section_id'])}]},
                                                            {"$set":{'confirmed_mode':int(''.join(map(str, [mode['mode_id']
                                                                                                  for mode in Modes.find({'mode_name':userClassifications[sectionindex]['userMode']})])))
                                                            if Modes.find({'mode_name':userClassifications[sectionindex]['userMode']}).count()!=0
                                                            else userClassifications[sectionindex]['userMode']}})
                logging.debug("update done" )

def getModeOptions():
    Modes=get_mode_db()
    return_dict = {}
    modes=[]
    for mode in Modes.find():
        modes.append(mode['mode_name'])
    return_dict['modelist'] = modes
    return return_dict
