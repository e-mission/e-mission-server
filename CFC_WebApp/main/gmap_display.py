from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import json
from datetime import tzinfo, datetime, timedelta
import pytz
import math
import logging
from pygeocoder import Geocoder
from pytz import timezone
from dateutil import parser
from home import detect_home, detect_home_from_db
from work_place import  detect_work_office, detect_daily_work_office,detect_work_office_from_db,detect_daily_work_office_from_db
from commute import get_daily_morning_commute_sections
from work_time import get_work_start_time, get_work_end_time,get_Alluser_work_start_time_pie, get_Alluser_work_start_time, get_user_work_start_time_pie,\
    get_Alluser_work_end_time_pie,get_Alluser_work_end_time
from common import most_common, get_first_daily_point, Include_place,berkeley_area,getModeShare
from distance import get_morning_commute_distance_pie,get_evening_commute_distance_pie
from get_database import get_mode_db, get_section_db, get_trip_db, get_test_db
from Berkeley import get_berkeley_mode_share_by_distance
from carbon import getModeCarbonFootprint,getFootprintCompare
# from zipcode import getZipcode,get_mode_share_by_Zipcode,get_period_mode_share_by_Zipcode
from modeshare import get_Alluser_mode_share_by_distance,get_user_mode_share_by_distance
import math
import pygmaps
from datetime import date, timedelta
from visualize import Berkeley_pop_route,carbon_by_zip
from uuid import *
from tripManager import calDistance
from pygeocoder import Geocoder

USER = '1cc03940-57f5-3e35-a189-55d067dc6460'
USER_B = '5322f635-a82c-3677-a1a4-5c26804a90b7'
POINTS = 'points'
PATH = 'path'
ALL = 'all'
COLOR = {0:"#00FFFF",
         1:"#0000FF",
         2:"#00FF00",
         3:"#FFFF00",
         4:"#FF0000",
        }

STARTPOINT = 'soda hall, berkeley'
ENDPOINT = '2610 warring st. berkeley'


def display_home(): 
        mymap_home = pygmaps.maps(37.8656475757, -122.258774009,14)
        for user in get_section_db().distinct('user_id'):
            print(user)
            user_home=detect_home(user)
            print(user_home)
            if user_home!='N/A':
                mymap_home.addpoint(user_home[0], user_home[1], "#FF0000")
            mymap_home.draw('mymap_home.html')

def Date(date):
    start = datetime(2000+int(date[6:8]), int(date[:2]),int(date[3:5]))
    if len(date) == 8:
        end = start + timedelta(days=1)
    else:
        end = datetime(2000+int(date[15:17]), int(date[9:11]),int(date[12:14]))
    return start, end


#Description: Display on google map, this user's trip during the specified date
#             Look for html file in gmap_display folder, file is named as "date
#             _user.html" eg. '05-12-2014_1cc03940-57f5-3e35-a189-55d067dc6460.h#             tml'
#user: a string user id, eg. '1cc03940-57f5-3e35-a189-55d067dc6460'
#date: a string specifying the date, eg. '05/30/14'
#option: a string indicating how to display the trip
#        eg. 'points' for point
#            'path'   for line
#            'all'    for point and line
#
#Color scheme for the gmap display:  Red    - mode 4
#                                    Blue   - mode 1                   
#                                    Yellow - mode 3 
#                                    Lime   - mode 2


def display_trip(user, date, option):
    user_id = UUID(user)
    user_home = detect_home(user_id)
    gmap = pygmaps.maps(user_home[0], user_home[1], 14)
    start, end = Date(date)
    for section in get_section_db().find({"$and":[{'user_id':user_id},{"section_start_datetime": {"$gte": start, "$lt": end}}]}):
        drawSection(section, option, gmap)
    gmap.draw('gmap_display/' + str(start)[:10] + '_' + user + '.html')

def searchTrip(user, period, startpoint, endpoint, mode, option):
    user_id = UUID(user)
    user_home = detect_home(user_id)
    gmap = pygmaps.maps(user_home[0], user_home[1], 14)
    start, end = Date(period)
    sectionList = []
    startpoint = Geocoder.geocode(startpoint)[0].coordinates
    endpoint = Geocoder.geocode(endpoint)[0].coordinates
    #gmap.addpoint(startpoint[0], startpoint[1], COLOR[4])
    #gmap.addpoint(endpoint[0], endpoint[1], COLOR[1])

    for section in get_section_db().find({"$and":[
        #{'user_id':user_id},
        #{"section_start_datetime": {"$gte": start, "$lt":end}},
        {"mode": mode},
        {"mode": {"$ne": 'airplane'}},
        {"mode": {"$ne":7}},
        {"section_start_point": {"$ne": None}},
        {"section_end_point": {"$ne": None}}]}):
        point_start = section['section_start_point']['coordinates']
        point_end = section['section_end_point']['coordinates']
        if calDistance(startpoint, point_start) < 100 and calDistance(endpoint, point_end) < 100:
            sectionList.append(section['_id'])
            gmap.addpoint(point_end[0], point_end[1], COLOR[1])
            gmap.addpoint(point_start[0], point_start[1], COLOR[1])
        drawSection(section, option, gmap)
    gmap.draw('gmap_display/' + 'SearchResult' + str(start)[:10] + '-' + str(end)[:10] + '_' + user + '.html')
    print sectionList

    
def drawSection(section, option, gmap, Color = 'default'):
    track_points = section['track_points']
    if track_points != []:
        path = []
        if Color != 'default':
            color = Color
        else:
            color = COLOR[section['mode']]
        for point in track_points:
            coordinate = point['track_location']['coordinates']
            coordinate_tuple = tuple(coordinate)
            path.append(coordinate_tuple)
            if option == POINTS or option == ALL:
                # coordinates are in GeoJSON format, ie lng, lat
                gmap.addpoint(coordinate[1], coordinate[0], color)
        if option == PATH or option == ALL:
            gmap.addpath(path, color)

def drawSections(sections,option, gmap, Color = 'default'):
    for section in sections:
        drawSection(section,option, gmap, Color)

def compareTrips(section1, section2):
    startPoint = section1['section_start_point']['coordinates']
    # coordinates are in GeoJSON format, ie lng, lat
    gmap = pygmaps.maps(startPoint[1], startPoint[0], 14)
    drawSection(section1, PATH,gmap,COLOR[1])
    drawSection(section2, PATH,gmap,COLOR[4])
    gmap.draw('gmap_display/compare.html')

def test():
    start,end = Date('05/25/14')
    sectionList = []
    for section in get_section_db().find({"$and":[{'user_id':UUID(USER)},{"section_start_datetime": {"$gte": start, "$lt": end}}]}):
        track_points = section['track_points']
        if track_points != []:
            sectionList.append(section)
        #print section
    #print len(sectionList)
    compareTrips(sectionList[0], sectionList[3])


    

