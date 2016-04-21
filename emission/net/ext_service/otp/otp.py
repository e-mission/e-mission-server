## Library to make calls to our Open Trip Planner server
## Hopefully similiar to googlemaps.py

# Standard imports
import urllib, urllib2, datetime, time, random
import geojson as gj
import arrow
# from traffic import get_travel_time

# Our imports
from emission.core.wrapper.trip_old import Coordinate, Alternative_Trip, Section, Fake_Trip, Trip
import emission.core.our_geocoder as our_geo
import emission.storage.decorations.trip_queries as ecsdtq
import emission.storage.decorations.section_queries as ecsdsq
import emission.storage.decorations.place_queries as ecsdpq
import emission.storage.decorations.local_date_queries as ecsdlq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.section as ecws

try:
    import json
except ImportError:
    import simplejson as json   

class PathNotFoundException(Exception):
    def __init__(self, parsedMessageObj):
        self.message = parsedMessageObj

    def __str__(self):
        return json.dumps(self.message)

class OTP:

    """ A class that exists to create an alternative trip object out of a call to our OTP server"""

    def __init__(self, start_point, end_point, mode, date, time, bike, max_walk_distance=10000000000000000000000000000000000):
        self.accepted_modes = {"CAR", "WALK", "BICYCLE", "TRANSIT", "BICYCLE_RENT"}
        self.start_point = start_point
        self.end_point = end_point
        if mode not in self.accepted_modes:
            print "mode is %s" % mode
            raise Exception("You are using a mode that doesnt exist")
        if mode == "TRANSIT" and bike:
            mode = "BICYCLE,TRANSIT"
        elif mode == "TRANSIT":
            mode = "WALK,TRANSIT"
        self.mode = mode
        self.date = date
        self.time = time
        self.max_walk_distance = max_walk_distance

    def make_url(self):
        """Returns the url for request """
        params = {

            "fromPlace" : self.start_point,
            "toPlace" : self.end_point,
            "time" : self.time,
            "mode" : self.mode,
            "date" : self.date,
            "maxWalkDistance" : self.max_walk_distance,
            "initIndex" : "0",
            "showIntermediateStops" : "true",
            "arriveBy" : "false"
        }

        add_file = open("emission/net/ext_service/otp/planner.json")
        add_file_1 = json.loads(add_file.read())
        address = add_file_1["open_trip_planner_instance_address"]
        query_url = "%s/otp/routers/default/plan?" % address
        encoded_params = urllib.urlencode(params)
        url = query_url + encoded_params
        print url
        return url

    def get_json(self):
        request = urllib2.Request(self.make_url())
        response = urllib2.urlopen(request)
        return json.loads(response.read())

    def get_all_trips(self, _id, user_id, trip_id):
        trps = [ ]
        our_json = self.get_json()
        num_its = len(our_json["plan"]["itineraries"])
        for itin in range(num_its):
            trps.append(self.turn_into_trip(_id, user_id, trip_id, False, itin))
        return trps

    def turn_into_new_trip(self, user_id):
        print "new trip"
        ts = esta.TimeSeries.get_time_series(user_id)
        trip = ecwrt.Rawtrip()
        sections = []
        our_json = self.get_json()
        mode_list = set ( )


        if "plan" not in our_json:
            print("While querying alternatives from %s to %s" % (self.start_point, self.end_point))
            print("query URL is %s" % self.make_url())
            print("Response %s does not have a plan " % our_json)
            raise PathNotFoundException(our_json['debugOutput'])

        trip.start_loc = gj.Point( (float(our_json["plan"]["from"]["lat"]), float(our_json["plan"]["from"]["lon"])) ) 
        trip.end_loc = gj.Point( (float(our_json["plan"]["to"]["lat"]), float(our_json["plan"]["to"]["lon"])) ) 
        trip.start_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
            our_json['plan']['itineraries'][0]["startTime"]).timestamp, "UTC")
        trip.end_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
            our_json['plan']['itineraries'][0]["endTime"]).timestamp, "UTC")
        trip_id = ts.insert(ecwe.Entry.create_entry(user_id, "segmentation/raw_trip", trip))

        for leg in our_json["plan"]["itineraries"][0]['legs']:
            section = ecws.Section()
            section.trip_id = trip_id
            section.start_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
                leg["startTime"]).timestamp, "UTC")
            section.end_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
                leg["endTime"]).timestamp, "UTC")
            section.distance = float(leg["distance"])
            section.start_loc = gj.Point( (float(leg["from"]["lat"]), float(leg["from"]["lon"])) )
            section.end_loc = gj.Point( (float(leg["to"]["lat"]), float(leg["to"]["lon"])) )
            ts.insert_data(user_id, "segmentation/raw_section", section)
 
    def turn_into_trip(self, _id, user_id, trip_id, is_fake=False, itinerary=0):
        sections = [ ]
        our_json = self.get_json()
        mode_list = set()
        car_dist = 0
        if "plan" not in our_json:
            print("While querying alternatives from %s to %s" % (self.start_point, self.end_point))
            print("query URL is %s" % self.make_url())
            print("Response %s does not have a plan " % our_json)
            raise PathNotFoundException(our_json['debugOutput'])

        for leg in our_json["plan"]["itineraries"][itinerary]['legs']:
            coords = [ ]
            var = 'steps'
            if leg['mode'] == 'RAIL' or leg['mode'] == 'SUBWAY':
                var = 'intermediateStops'
                for step in leg[var]:
                    coords.append(Coordinate(step['lat'], step['lon'])) 

            start_time = otp_time_to_ours(leg["startTime"])
            end_time = otp_time_to_ours(leg["endTime"])
            distance = float(leg['distance'])
            start_loc = Coordinate(float(leg["from"]["lat"]), float(leg["from"]["lon"]))
            end_loc = Coordinate(float(leg["to"]["lat"]), float(leg["to"]["lon"]))
            coords.insert(0, start_loc)
            coords.append(end_loc)
            mode = leg["mode"]
            mode_list.add(mode)
            fake_id = random.random()
            points = [ ]
            for step in leg['steps']:
                c = Coordinate(step["lat"], step['lon'])
                #print c
                points.append(c)
            #print "len of points is %s" % len(points)
            section = Section(str(fake_id), user_id, trip_id, distance, "move", start_time, end_time, start_loc, end_loc, mode, mode, points)
            #section.points = coords
            sections.append(section)
            if mode == 'CAR':
                car_dist = distance
                car_start_coordinates = Coordinate(float(leg["from"]["lat"]), float(leg["from"]["lon"]))    
                car_end_coordinates = Coordinate(float(leg["to"]["lat"]), float(leg["to"]["lon"]))
        
        print "len(sections) = %s" % len(sections)
        final_start_loc = Coordinate(float(our_json["plan"]["from"]["lat"]), float(our_json["plan"]["from"]["lon"]))         
        final_end_loc = Coordinate(float(our_json["plan"]["to"]["lat"]), float(our_json["plan"]["to"]["lon"]))
        final_start_time = otp_time_to_ours(our_json['plan']['itineraries'][0]["startTime"])
        final_end_time = otp_time_to_ours(our_json['plan']['itineraries'][0]["endTime"])
        cost = 0
        if "RAIL" in mode_list or "SUBWAY" in mode_list:
            try:
                cost = float(our_json['plan']['itineraries'][0]['fare']['fare']['regular']['cents']) / 100.0   #gives fare in cents 
            except:
                cost = 0
        elif "CAR" in mode_list:
            # TODO calculate car cost
            cost = 0
        mode_list = list(mode_list)
        if is_fake:
            return Trip(_id, user_id, trip_id, sections, final_start_time, final_end_time, final_start_loc, final_end_loc)
        return Alternative_Trip(_id, user_id, trip_id, sections, final_start_time, final_end_time, final_start_loc, final_end_loc, 0, cost, mode_list)

def otp_time_to_ours(otp_str):
    return arrow.get(int(otp_str)/1000)

