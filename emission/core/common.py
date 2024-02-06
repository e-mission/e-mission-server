# Standard imports
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import map
from builtins import *
from random import randrange
import logging
import copy
from datetime import datetime, timedelta
from dateutil import parser
from pytz import timezone
import math

def isMillisecs(ts):
  return not (ts < 10 ** 11)

def Is_place_2(place1,place,radius):
    # print(place)
    if calDistance(place1,place)<radius:
        return True
    else:
        return False

def Include_place_2(lst,place,radius):
    # list of tracking points
    count=0
    for pnt in lst:
        count=count+(1 if calDistance(pnt,place)<=radius else 0)
    if count>0:
        return True
    else:
        return False

def travel_date_time(time1,time2):
    travel_time = time2-time1
    return travel_time.seconds

def calDistance(point1, point2, coordinates=False):
    """haversine distance

    :param point1: a coordinate in degrees WGS84
    :param point2: another coordinate in degrees WGS84
    :param coordinates: if false, expect a list of coordinates, defaults to False
    :return: distance approximately in meters
    """
    earthRadius = 6371000  # meters
    # SHANKARI: Why do we have two calDistance() functions?
    # Need to combine into one
    # points are now in geojson format (lng,lat)
    if coordinates:
        dLat = math.radians(point1.lat-point2.lat)
        dLon = math.radians(point1.lon-point2.lon)
        lat1 = math.radians(point1.lat)
        lat2 = math.radians(point2.lat)
    else:
        dLat = math.radians(point1[1]-point2[1])
        dLon = math.radians(point1[0]-point2[0])
        lat1 = math.radians(point1[1])
        lat2 = math.radians(point2[1])


    a = (math.sin(dLat/2) ** 2) + ((math.sin(dLon/2) ** 2) * math.cos(lat1) * math.cos(lat2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = earthRadius * c

    return d

def compare_rounded_arrays(arr1, arr2, digits):
    round2n = lambda x: round(x, digits)
    return list(map(round2n, arr1)) == list(map(round2n, arr2))

