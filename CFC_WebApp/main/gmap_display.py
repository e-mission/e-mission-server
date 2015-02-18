"""
#########################################################
#Google Map Display Tool for E-Mission Trip/Section Data#
#########################################################
Author: En (Ryan) Lei
A library of functions that allows the user to plot GPS coordinates data via Google Maps API(pygmap). 
Plots are stored in HTML format and can be open using Chrome or Firefox web browser. For more inform
ation please refer to the comments within the function.

Color - Transportation Mode Mapping:

walking - blue
running - green
cycling - yellow
transport - red
bus - aqua
train - darkOrange
car - grey 
mixed - olive
air - skyBlue
"""

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
from common import most_common, get_first_daily_point, Include_place,berkeley_area,getModeShare
from distance import get_morning_commute_distance_pie,get_evening_commute_distance_pie
from get_database import get_mode_db, get_section_db, get_trip_db, get_test_db
import math
import pygmaps
from datetime import date, timedelta
from uuid import *
from common import calDistance
from pygeocoder import Geocoder



POINTS = 'points'
PATH = 'path'
ALL = 'all'
COLOR = {1:"#0000FF",       #walking - blue
         2:"#00FF00",       #running - green
         3:"#FFFF00",       #cycling - yellow
         4:"#FF0000",       #transport - red
         5:"#00FFFF",       #bus - aqua
         6:"#FF8C00",       #train - darkOrange
         7:"#778899",       #car - grey 
         8:"#808000",       #mixed - olive
         9:"#87CEFA"        #air - skyBlue
        }



def display_home(): 
        mymap_home = pygmaps.maps(37.8556475757, -122.248774009,14)
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
#             _user.html" eg. '1234567890.html'
#user: a string user id, eg. '1234567890'
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

    for section in get_section_db().find({"$and":[
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
            
            coordinate_tuple = tuple([coordinate[1], coordinate[0]])
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



def drawTrip(trip_id, db, gmap):
    #Given trip_id, database where the trip is stored, and a pygmap.map object
    #drawTrip plots all sections associated with the trip_id on the map object
    #and returns the modified map object
    trip = None
    trip_cursor = db.Stage_Trips.find({'trip_id': trip_id})

    if trip_cursor.count() == 1:
        trip = trip_cursor[0]
    else:
        print "Duplicated trip_id: " + trip_id
        exit()
    sections = db.Stage_Sections.find({'trip_id': trip_id})
    drawSections(sections, ALL, gmap)
    return gmap
    

