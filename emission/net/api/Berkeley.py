__author__ = 'Yin'
from pymongo import MongoClient
from common import get_mode_share_by_distance, berkeley_area, Inside_polygon
from get_database import get_section_db
from pymongo import GEOSPHERE
from dateutil import parser


def get_berkeley_sections():
    Sections = get_section_db()
    list_of_sections=[]
    for section in Sections.find():
        if section['section_start_point']!=None and section['section_end_point']!=None and \
            Inside_polygon(section['section_start_point'],berkeley_area()) and \
                Inside_polygon(section['section_end_point'],berkeley_area()):
            list_of_sections.append(section)
    return list_of_sections

def get_berkeley_mode_share_by_distance(start,end):
    # start = datetime(2014, 3, 20)
    # end = datetime(2014, 3, 21)
    spec={"$and":[{"section_start_datetime": {"$gte": start, "$lt": end}},{ "In_UCB" :True}]}
    return get_mode_share_by_distance(spec)

