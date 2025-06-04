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
import numpy as np

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

def haversine_numpy(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between points
    using numpy for vectorized calculations.
    
    :param lon1: Longitude of first point(s) in decimal degrees
    :param lat1: Latitude of first point(s) in decimal degrees
    :param lon2: Longitude of second point(s) in decimal degrees
    :param lat2: Latitude of second point(s) in decimal degrees
    :return: Distance in meters
    """
    earth_radius = 6371000  # meters
    
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    distance = earth_radius * c
    
    return distance

def calDistance(point1, point2, coordinates=False):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    
    :param point1: Point in [longitude, latitude] format or object with .lon and .lat attributes
    :param point2: Point in [longitude, latitude] format or object with .lon and .lat attributes
    :param coordinates: If True, points are objects with .lon and .lat attributes. 
                        If False, points are [lon, lat] lists
    :return: Distance in meters
    """
    # Extract coordinates based on the input format
    if coordinates:
        lon1, lat1 = point1.lon, point1.lat
        lon2, lat2 = point2.lon, point2.lat
    else:
        lon1, lat1 = point1[0], point1[1]
        lon2, lat2 = point2[0], point2[1]
    
    # Use vectorized function for the calculation
    return haversine_numpy(lon1, lat1, lon2, lat2)

def compare_rounded_arrays(arr1, arr2, digits):
    round2n = lambda x: round(x, digits)
    return list(map(round2n, arr1)) == list(map(round2n, arr2))

def calHeading(point1, point2, coordinates=False):
    """
    Calculate the heading angle between two points on the earth 
    (specified in decimal degrees)
    
    :param point1: Point in [longitude, latitude] format or object with .lon and .lat attributes
    :param point2: Point in [longitude, latitude] format or object with .lon and .lat attributes
    :param coordinates: If True, points are objects with .lon and .lat attributes.
                        If False, points are [lon, lat] lists
    :return: Heading angle in degrees (0-360)
    """
    # Extract coordinates based on the input format
    if coordinates:
        lon1, lat1 = point1.lon, point1.lat
        lon2, lat2 = point2.lon, point2.lat
    else:
        lon1, lat1 = point1[0], point1[1]
        lon2, lat2 = point2[0], point2[1]
    
    # Use calHeading_numpy for the calculation
    return calHeading_numpy(lon1, lat1, lon2, lat2)

def calHeading_numpy(lon1, lat1, lon2, lat2):
    """
    Calculate the heading angle between two points
    using numpy for vectorized calculations.
    
    :param lon1: Longitude of first point(s) in decimal degrees
    :param lat1: Latitude of first point(s) in decimal degrees
    :param lon2: Longitude of second point(s) in decimal degrees
    :param lat2: Latitude of second point(s) in decimal degrees
    :return: Heading angle in degrees (0-360)
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    
    # Calculate heading
    dlon = lon2 - lon1
    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    
    # Convert to degrees and normalize to 0-360
    heading = np.degrees(np.arctan2(x, y))
    heading = (heading + 360) % 360
    
    return heading

