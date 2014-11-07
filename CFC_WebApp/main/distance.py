import logging
from random import randrange
import math
from common import getDistance
# from commute import get_morning_commute_sections
from tripManager import calDistance
from get_database import get_section_db,get_worktime_db

# dis_list = [[0,1],[1,2],[2,3], [3,5], [5,10], [10,20], [20,30], [30,50], [50,100],[100,200],[200,500]]
dis_list = [[0,1],[1,2],[2,3], [3,5], [5,10], [10,20], [20,30], [30,50], [50,99]]


# def getAllDistances():
#     logging.debug("getting the list of distances")
#     return getDistance(get_section_db().find())



def get_morning_commute_distance(user,start,end):
    # day should be from 1 to 5
    # get a list of work starttime for Mon, or ...
    Sections=get_section_db()
    Worktimes=get_worktime_db()
    totalDist = 0
    projection = {'distance': True, '_id': False}
    for section in Sections.find({"$and":[{'user_id':user},{'commute':'to'},{"section_start_datetime": {"$gte": start, "$lt": end}}]}, projection):
#     logging.debug("found section with %s %s %s %s %s" %
#       (section['trip_id'], section['section_id'], section['confirmed_mode'],
#           section['user_id'], section['type']))
        if section['distance']!=None:
            totalDist = totalDist + section['distance']
        else:
            logging.warning("distance key not found in projection, returning zero...")
      # returning zero for the distance
            pass
  # logging.debug("for sectionList %s, distanceList = %s" % (len(sectionList), distanceList))
    num_commute=Worktimes.find({"$and":[{'user_id':user},{'arr_hour':{ "$exists": True}},{"date": {"$gte": start, "$lt": end}}]}).count()
    if num_commute==0:
        return 'N/A'
    else:
        return totalDist/num_commute


def get_evening_commute_distance(user,start,end):
    # day should be from 1 to 5
    # get a list of work starttime for Mon, or ...
    Sections=get_section_db()
    Worktimes=get_worktime_db()
    totalDist = 0
    projection = {'distance': True, '_id': False}
    for section in Sections.find({"$and":[{'user_id':user},{'commute':'from'},{"section_start_datetime": {"$gte": start, "$lt": end}}]}, projection):
#     logging.debug("found section with %s %s %s %s %s" %
#       (section['trip_id'], section['section_id'], section['confirmed_mode'],
#           section['user_id'], section['type']))
        if section['distance']!=None:
            totalDist = totalDist + section['distance']
        else:
            logging.warning("distance key not found in projection, returning zero...")
      # returning zero for the distance
            pass
  # logging.debug("for sectionList %s, distanceList = %s" % (len(sectionList), distanceList))
    num_commute=Worktimes.find({"$and":[{'user_id':user},{'dep_hour':{ "$exists": True}},{"date": {"$gte": start, "$lt": end}}]}).count()
    if num_commute==0:
        return 'N/A'
    else:
        return totalDist/num_commute

def get_morning_commute_distance_pie(start,end):
    Sections=get_section_db()
    disCountMap = {}
    for user in Sections.distinct('user_id'):
        dis=get_morning_commute_distance(user,start,end)
        if dis!='N/A':
            for dissection in dis_list:
                if dis/1609 > dissection[0] and dis/1609 <= dissection[1]:
                    key=str(dissection[0]).zfill(2)+' - '+str(dissection[1]).zfill(2)
                    if key not in disCountMap:
                        disCountMap[key] =1
                    else:
                        disCountMap[key] +=1
    return disCountMap

def get_evening_commute_distance_pie(start,end):
    Sections=get_section_db()
    disCountMap = {}
    for user in Sections.distinct('user_id'):
        dis=get_evening_commute_distance(user,start,end)
        if dis!='N/A':
            for dissection in dis_list:
                if dis/1609 > dissection[0] and dis/1609 <= dissection[1]:
                    key=str(dissection[0]).zfill(2)+' - '+str(dissection[1]).zfill(2)
                    if key not in disCountMap:
                        disCountMap[key] =1
                    else:
                        disCountMap[key] +=1
    return disCountMap
