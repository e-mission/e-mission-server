## Library to make calls to our Open Trip Planner server
## Hopefully similiar to googlemaps.py

# Standard imports
import urllib, urllib2, datetime, time, random
# from traffic import get_travel_time
from pygeocoder import Geocoder

# Our imports
from emission.core.wrapper.trip import Coordinate, Alternative_Trip, Section, E_Mission_Trip

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
        self.accepted_modes = {"CAR", "WALK", "BICYCLE", "TRANSIT"}
        self.start_point = start_point
        self.end_point = end_point
        if mode not in self.accepted_modes:
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

        query_url = "http://50.17.58.240:8080/otp/routers/default/plan?"
        encoded_params = urllib.urlencode(params)
        url = query_url + encoded_params
        print url
        return url

    def get_json(self):
        request = urllib2.Request(self.make_url())
        response = urllib2.urlopen(request)
        return json.loads(response.read())

    def turn_into_trip(self, _id, user_id, trip_id, is_fake=None):
        sections = [ ]
        our_json = self.get_json()
        mode_list = set()
        car_dist = 0
        #our_json = json.dumps(our_json)
        print type(our_json)
        if "plan" not in our_json:
            print("While querying alternatives from %s to %s" % (self.start_point, self.end_point))
            print("query URL is %s" % self.make_url())
            print("Response %s does not have a plan " % our_json)
            raise PathNotFoundException(our_json['debugOutput'])

        # print our_json["plan"]["itineraries"]
        for leg in our_json["plan"]["itineraries"][0]['legs']:
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
            section = Section(fake_id, user_id, trip_id, distance, "move", start_time, end_time, start_loc, end_loc, mode, mode)
            section.points = coords
            #print section.points
            sections.append(section)
            if mode == 'CAR':
                car_dist = distance
                car_start_coordinates = Coordinate(float(leg["from"]["lat"]), float(leg["from"]["lon"]))    
                car_end_coordinates = Coordinate(float(leg["to"]["lat"]), float(leg["to"]["lon"]))
        
        final_start_loc = Coordinate(float(our_json["plan"]["from"]["lat"]), float(our_json["plan"]["from"]["lon"]))         
        final_end_loc = Coordinate(float(our_json["plan"]["to"]["lat"]), float(our_json["plan"]["to"]["lon"]))
        final_start_time = otp_time_to_ours(our_json['plan']['itineraries'][0]["startTime"])
        final_end_time = otp_time_to_ours(our_json['plan']['itineraries'][0]["endTime"])
        cost = 0

        #print start_city, end_city
        
        if "RAIL" in mode_list or "SUBWAY" in mode_list:#
            #print our_json
            try:
                print our_json['plan']['itineraries'][0]['fare'].keys()
                cost = float(our_json['plan']['itineraries'][0]['fare']['fare']['regular']['cents']) / 100.0   #gives fare in cents 
            except:
                cost = 0
        elif "CAR" in mode_list:
            #start_city_car = str(Geocoder().reverse_geocode(car_start_coordinates.get_lat(),car_start_coordinates.get_lon())).split(',')[1]
            #end_city_car = str(Geocoder().reverse_geocode(car_end_coordinates.get_lat(), car_end_coordinates.get_lon())).split(',')[1]
            #traffic_time = get_travel_time(start_city_car, end_city_car)
            cost = 0
            #if traffic_time > final_end_time - final_start_time:
            #    print "Driving is bad: " + traffic_time
            #    final_end_time = final_start_time + traffic_time

        mode_list = list(mode_list)
        if is_fake:
            return E_Mission_Trip(_id, user_id, trip_id, sections, final_start_time, final_end_time, final_start_loc, final_end_loc, 0, 0, mode_list, mode_list)
        return Alternative_Trip(_id, user_id, trip_id, sections, final_start_time, final_end_time, final_start_loc, final_end_loc, 0, cost, mode_list)

def otp_time_to_ours(otp_str):
    t = time.gmtime(int(otp_str)/1000)
    #datetime.datetime.fromtimestamp(int(otp_str)/1000)
    return datetime.datetime(*t[:6])    


def basic_test():
    r = OTP("37.866678, -122.263224", "37.5114478, -122.3191384", "WALK", "4-25-15", "4:45pm")
    f = open("sample.json", "w")
    print f.write(r.get_json())

def multi_modal():
    otp = OTP("37.866678, -122.263224", "37.5114478, -122.3191384", "TRANSIT", "4-25-15", "4:45pm", True)
    new_trip = otp.turn_into_trip(0,0,0)
    print "type: " + str(type(new_trip))    
    print "mode list: " + str(new_trip.mode_list)
    print "start location: " + str(new_trip.trip_start_location)
    print "end location: " + str(new_trip.trip_end_location)
    #print "start time: " + str(new_trip.start_time)
    #print "end time: "  + str(new_trip.end_time)
    print "duration : " + str(new_trip.end_time - new_trip.start_time)
    print "cost: " + str(new_trip.cost)
    for sec in new_trip.sections:
        print "mode: " + str(sec.mode)
        print "corrds: " + str(sec.points)
    
def simple_driving():
    otp =  OTP("37.866678, -122.263224", "37.5114478, -122.3191384", "CAR", "4-25-15", "4:45pm", False)
    new_trip = otp.turn_into_trip(0,0,0)
    print "type: " + str(type(new_trip))
    print "mode list: " + str(new_trip.mode_list)
    print "start location: " + str(new_trip.trip_start_location)
    print "end location: " + str(new_trip.trip_end_location)
    #print "start time: " + str(new_trip.start_time)
    #print "end time: "  + str(new_trip.end_time)
    print "duration: " + str(new_trip.end_time - new_trip.start_time)
    print "cost: " + str(new_trip.cost)
    for sec in new_trip.sections:
        print "mode: " + str(sec.mode)
        print "corrds: " + str(sec.points)
    

def just_train():
    otp = OTP("37.866678, -122.263224", "37.5114478, -122.3191384", "TRANSIT", "4-25-15", "4:45pm", False)
    new_trip = otp.turn_into_trip(0,0,0)
    print "type: " + str(type(new_trip))
    print "mode list: " + str(new_trip.mode_list)
    print "start location: " + str(new_trip.trip_start_location)
    print "end location: " + str(new_trip.trip_end_location)
    #print "start time: " + str(new_trip.start_time)
    #print "end time: "  + str(new_trip.end_time)
    print "duration : " + str(new_trip.end_time - new_trip.start_time)
    print "cost: " + str(new_trip.cost)

if __name__ == "__main__":
    print "Simple Drive:"
    simple_driving()
    
    print
    print "Walk to train: "
    just_train()
    
    print 
    print "You have a bike: "
    multi_modal()



# def google_maps_to_our_trip(google_maps_json, _id, user_id, trip_id, mode, org_start_time):
#         sections = [ ]
#         time = org_start_time
#         for leg in google_maps_json['routes'][0]['legs']:
#                 td = coerce_gmaps_time(leg['duration']['text'])
#                 distance = leg['distance']
#                 start_location = Coordinate(leg['start_location']['lat'], leg['start_location']['lng'])
#                 end_location = Coordinate(leg['end_location']['lat'], leg['end_location']['lng'])
#                 end_time = time + td
#                 section = Section(0, trip_id, distance, time, end_time, start_location, end_location, mode, mode)
#                 sections.append(section)
#                 time = end_time
#         start_trip = sections[0].section_start_location
#         end_trip = sections[-1].section_end_location
#         #TODO: actually calculate cost
#         cost = 0
#         parent_id = trip_id
#         mode_list = [str(mode)]
#         return Alternative_Trip(_id, user_id, trip_id, sections, org_start_time, end_time, start_trip, end_trip, parent_id, cost, mode_list)


