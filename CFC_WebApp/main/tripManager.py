import logging
from dateutil import parser
import math
import json
from get_database import get_mode_db, get_section_db
from datetime import datetime, timedelta
from userclient import getClientSpecificQueryFilter
from common import calDistance, travel_time
import stats
import time

# TODO: Argh! Until now, we just had the data collector import the webapp.
# Now we have the webapp import the data collector. This badly needs
# restructuring.
import sys
import os

sys.path.append("%s" % os.getcwd())
sys.path.append("%s/../CFC_DataCollector/moves" % os.getcwd())

import collect

def max_Distance(points):
    # 'track_points':[{'track_location':{'type':'Point', 'coordinates':[point["lat"],point["lon"]]}, 'time':point["time"]}for point in seg_act_note["trackPoints"]] if "trackPoints" in seg_act_note else []}
    num_pts=len(points)
    max_d=0
    for i in range(num_pts):
        for j in range(i+1,num_pts):
            max_d=max(max_d,calDistance(points[i]['track_location']['coordinates'], points[j]['track_location']['coordinates']))
    return max_d

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

# def filter_unclassifiedSections(UnclassifiedSections):
#     filtered_Sections=[]
#     for section in UnclassifiedSections:
#         if section['filtered']:
#             filtered_Sections.append(section)
#     return filtered_Sections

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
                         {'predicted_mode':{ '$exists' : True }},
                         {'confirmed_mode':''},
                         {'retained':True},
                         { 'type': 'move' },
                         {'section_end_datetime':{"$gt": weekago}}]
    completeQueryList = defaultQueryList + clientSpecificQuery
    unclassifiedSections=Sections.find({"$and": completeQueryList})

    # totalUnclassifiedSections are for debugging only, can remove after we know that this works well
    totalUnclassifiedSections=Sections.find({"$and":[ {'source':'Shankari'},
                                                 {'user_id':user_uuid},
                                                 {'confirmed_mode': ''},
                                                 { 'type': 'move' }]})

    unclassifiedSectionCount = unclassifiedSections.count()
    totalUnclassifiedSectionCount = totalUnclassifiedSections.count()

    logging.debug('Unsec.count = %s' % unclassifiedSectionCount)
    logging.debug('Total Unsec.count = %s' % totalUnclassifiedSectionCount)
    # Keep track of what percent of sections are stripped out.
    # Sections can be stripped out for various reasons:
    # - they are too old
    # - they have enough confidence that above the magic threshold (90%) AND
    # the client has requested stripping out
    # - they have already been identified as being too short by the filter label
    stats.storeServerEntry(user_uuid, stats.STAT_TRIP_MGR_PCT_SHOWN, time.time(),
            0 if totalUnclassifiedSectionCount == 0 else float(unclassifiedSectionCount)/totalUnclassifiedSectionCount)
    return unclassifiedSections

def getUnclassifiedSections(uuid):
    return_dict={}
    unclassifiedSections = list(queryUnclassifiedSections(uuid))
    logging.debug("filtered_UnclassifiedSections = %s" % len(unclassifiedSections))
    stripped_filtered_UnclassifiedSections = stripoutNonSerializable(unclassifiedSections)
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

def storeSensedTrips(user_uuid, sections):
    logging.debug("invoked storing sensed trips")
    collect.processTripArray(user_uuid, sections)
    logging.debug("done storing sensed trips")

def getModeOptions():
    Modes=get_mode_db()
    return_dict = {}
    modes=[]
    for mode in Modes.find():
        modes.append(mode['mode_name'])
    return_dict['modelist'] = modes
    return return_dict
