__author__ = 'Yin'
from pymongo import MongoClient
from common import Is_date, Is_place, get_mode_share_by_distance, berkeley_area
from tripManager import travel_time
from get_database import get_section_db,get_profile_db
# from commute import get_morning_commute_sections
from dateutil import parser

def get_Alluser_mode_share_by_distance(flag,start,end):
    # start = datetime(2014, 3, 20)
    # end = datetime(2014, 3, 21)
    if flag=='all':
        spec={'section_start_datetime': {"$gte": start, "$lt": end}}
    elif flag=='commute':
        spec={"$and":[{"section_start_datetime": {"$gte": start, "$lt": end}},{"$or":[{'commute': 'to'},{'commute': 'from'}]}]}
    return get_mode_share_by_distance(spec)


def get_user_mode_share_by_distance(user,flag,start,end):
    # start = datetime(2014, 3, 20)
    # end = datetime(2014, 3, 21)
    if flag=='all':
        spec={"$and":[{'user_id':user},{"section_start_datetime": {"$gte": start, "$lt": end}},{"$or":[{'commute': 'to'},{'commute': 'from'}]}]}
    elif flag=='commute':
        spec={"$and":[{'user_id':user},{"section_start_datetime": {"$gte": start, "$lt": end}},{"$or":[{'commute': 'to'},{'commute': 'from'}]}]}
    return get_mode_share_by_distance(spec)
