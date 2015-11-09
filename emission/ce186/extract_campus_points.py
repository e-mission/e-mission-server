import emission.net.ext_service.otp.otp as otp
import emission.core.our_geocoder as geo
import datetime
import random
import json
import googlemaps


bounding_box = ( (37.87540, -122.26637),  (37.86938, -122.25251) )

def in_bounding_box(lat, lon):
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
        if pnt and in_bounding_box(pnt[0], pnt[1]):
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




def demo():
    print "time is %s" % get_times_between_address("Cafe Strada", "Soda Hall", "BICYCLE")
    print "Loc change is %s" % str(get_ele_change_loc("Cafe Strada", "Soda Hall"))
                  
#get_times_between_point((-122.27303, 37.81412), (-122.41173, 37.75497, ), "CAR")
#get_times_between_point((37.8753995, -122.25987323), (37.8734016, -122.2570006), "BICYCLE")