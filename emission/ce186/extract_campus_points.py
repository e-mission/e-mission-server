import emission.net.ext_service.otp.otp as otp
import emission.core.our_geocoder as geo
import emission.core.common as cm

import datetime
import random
import json
import googlemaps


BOUNDING_BOX = ( (37.87540, -122.26637),  (37.86938, -122.25251) )

def in_bounding_box(lat, lon, bounding_box):
    return bounding_box[1][0] <= lat and lat <= bounding_box[0][0] and bounding_box[0][1] <= lon and lon <= bounding_box[1][1]

def grab_loc_points_from_line(lne):
    lne = lne.split()
    if lne[0] != "<node":
        return
    lat = float(lne[2][5:-1])
    lon = float(lne[3][5:-1])
    return (lat, lon)

def extract_points():
    osm_file = open("sf-bay-area.osm")
    points_file = open("points.csv", "w")
    points = set()
    for l in osm_file:
        pnt = grab_loc_points_from_line(l)
        if pnt and in_bounding_box(pnt[0], pnt[1], BOUNDING_BOX):
            points.add(pnt)
            
    for pnt in points:
        points_file.write(str(pnt) + ",\n")

# def test():
#     assert in_bounding_box(37.871307, -122.259149)

def get_times_between_address(addr1, addr2, mode):
    coder = geo.Geocoder()
    loc1, loc2 = coder.geocode(addr1), coder.geocode(addr2)
    return get_times_between_point(loc1, loc2, mode)


def get_times_between_point(pnt1, pnt2, mode):
    curr_time = datetime.datetime.now()
    curr_month = curr_time.month
    curr_year = curr_time.year
    curr_minute = curr_time.minute
    curr_day = curr_time.day
    curr_hour = curr_time.hour
    trip = otp.OTP(pnt1, pnt2, mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), True)
    t = trip.turn_into_trip(0,0,0)
    print t.end_time - t.start_time

def get_dist_between_points(pnt1, pnt2):
    curr_time = datetime.datetime.now()
    curr_month = curr_time.month
    curr_year = curr_time.year
    curr_minute = curr_time.minute
    curr_day = curr_time.day
    curr_hour = curr_time.hour
    trip = otp.OTP(pnt1, pnt2, "WALK", write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), True)
    t = trip.turn_into_trip(0,0,0)


def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute)


def get_ele_change_loc(addr1, addr2):
    coder = geo.Geocoder()
    loc1 = coder.geocode(addr1).to_tuple()
    loc2 = coder.geocode(addr2).to_tuple()
    print loc1, loc2
    print type(loc1)
    return get_elevation_change(loc1, loc2)    

def get_elevation_change(pnt1, pnt2):
    c = googlemaps.client.Client('AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c')
    jsn = googlemaps.elevation.elevation_along_path(c, (pnt1, pnt2), 10)
    up, down = 0, 0
    prev = None
    for item in jsn:
        if prev and item["elevation"] > prev:
            up += item["elevation"] - prev
        elif prev and item["elevation"] < prev:
            down += prev - item["elevation"]
        prev = item['elevation']
    return (up, down)


def get_distances():
    places = [( (37.873508, -122.256512), (37.873745, -122.255493) ),  ( (37.873940, -122.257553), (37.873381, -122.257274) ), ((37.873326, -122.256791), (37.872246, -122.256485)) ]
    for p in places:
        print get_elevation_change(p[0], p[1])
        print cm.calDistance(p[0], p[1], False)



def demo():
    print "time is %s" % get_times_between_point((37.8691323,-122.2549288), (37.8755814,-122.2589025), "BICYCLE")
    print "Loc change is %s" % str(get_ele_change_loc("Cafe Strada", "Soda Hall"))
                  
#get_times_between_point((-122.27303, 37.81412), (-122.41173, 37.75497, ), "CAR")
#get_times_between_point((37.8753995, -122.25987323), (37.8734016, -122.2570006), "BICYCLE")