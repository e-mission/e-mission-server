from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import

## Library to make calls to our Open Trip Planner server
## Hopefully similiar to googlemaps.py

# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import *
from builtins import object
from past.utils import old_div
import urllib.request, urllib.parse, urllib.error, urllib.request, urllib.error, urllib.parse, datetime, time, random
import geojson as gj
import arrow
# from traffic import get_travel_time

# Our imports
from emission.core.wrapper.trip_old import Coordinate, Alternative_Trip, Section, Fake_Trip, Trip
import emission.net.ext_service.geocoder.nominatim as our_geo
import emission.storage.decorations.trip_queries as ecsdtq
import emission.storage.decorations.section_queries as ecsdsq
import emission.storage.decorations.place_queries as ecsdpq
import emission.storage.decorations.local_date_queries as ecsdlq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.section as ecws
# new imports
import emission.core.wrapper.transition as ecwt
import emission.core.wrapper.location as ecwl
import emission.core.wrapper.rawplace as ecwrp
import emission.core.wrapper.stop as ecwrs
import emission.core.wrapper.motionactivity as ecwm

import emission.analysis.intake.segmentation.trip_segmentation as eaist 
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.storage.decorations.analysis_timeseries_queries as esda

try:
    import json
except ImportError:
    import simplejson as json   

class PathNotFoundException(Exception):
    def __init__(self, parsedMessageObj):
        self.message = parsedMessageObj

    def __str__(self):
        return json.dumps(self.message)

class OTP(object):

    """ A class that exists to create an alternative trip object out of a call to our OTP server"""

    def __init__(self, start_point, end_point, mode, date, time, bike, max_walk_distance=10000000000000000000000000000000000):
        self.accepted_modes = {"CAR", "WALK", "BICYCLE", "TRANSIT", "BICYCLE_RENT"}
        self.start_point = start_point
        self.end_point = end_point
        if mode not in self.accepted_modes:
            print("mode is %s" % mode)
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
        encoded_params = urllib.parse.urlencode(params)
        url = query_url + encoded_params
        print(url)
        return url

    def get_json(self):
        request = urllib.request.Request(self.make_url())
        response = urllib.request.urlopen(request)
        return json.loads(response.read())

    def get_all_trips(self, _id, user_id, trip_id):
        trps = [ ]
        our_json = self.get_json()
        num_its = len(our_json["plan"]["itineraries"])
        for itin in range(num_its):
            trps.append(self.turn_into_trip(_id, user_id, trip_id, False, itin))
        return trps

    def turn_into_new_trip(self, user_id):
        #TODO: This function does not work with the new data format. 
        # The way sections are created is wrong. Look at intake pielpline to figure out 
        # how to properly build sections
        our_json = self.get_json()
        print("new trip")
        if "plan" not in our_json:
            print("While querying alternatives from %s to %s" % (self.start_point, self.end_point))
            print("query URL is %s" % self.make_url())
            print("Response %s does not have a plan " % our_json)
            raise PathNotFoundException(our_json['debugOutput'])

        #Initialize some values.
        ts = esta.TimeSeries.get_time_series(user_id)
        #Create start place entry 
        start_place = eaist.start_new_chain(user_id)
        start_place.source = 'Fake'
        start_place_entry = ecwe.Entry.create_entry(user_id,
                                "segmentation/raw_place", start_place, create_id = True)
        #Set the start location TODO: should we save this to longterm storage.
        trip_start_loc = create_start_location_from_trip_plan(our_json['plan'])
        #set end location 
        trip_end_loc = create_end_location_from_trip_plan(our_json['plan']) 
        #Create a curr trip object
        trip = ecwrt.Rawtrip()
        trip.source = 'Fake'  
        trip_entry = ecwe.Entry.create_entry(user_id,
                            "segmentation/raw_trip", trip, create_id = True)
        #Create end_place
        end_place = ecwrp.Rawplace()
        end_place.source = 'Fake' 
        end_place_entry = ecwe.Entry.create_entry(user_id,
                            "segmentation/raw_place", end_place, create_id = True)
                            
        ## Link together the start place entry, the trip entry and endplace entry 
        eaist._link_and_save(ts, start_place_entry, trip_entry, end_place_entry, trip_start_loc, trip_end_loc)
       # trip.start_loc = gj.Point( (float(our_json["plan"]["from"]["lon"]), float(our_json["plan"]["from"]["lat"])) ) 
       # trip.end_loc = gj.Point( (float(our_json["plan"]["to"]["lon"]), float(our_json["plan"]["to"]["lat"]) ) ) 
       # trip.start_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
       #     our_json['plan']['itineraries'][0]["startTime"]).timestamp, "UTC")
       # trip.end_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
       #     our_json['plan']['itineraries'][0]["endTime"]).timestamp, "UTC")
       # trip_id = ts.insert(ecwe.Entry.create_entry(user_id, "segmentation/raw_trip", trip))
        prev_section_entry = None
        for i, leg in enumerate(our_json["plan"]["itineraries"][0]['legs']):
            #Fill section entry. TODO: break this into its own function
            section_start_loc = create_start_location_from_leg(leg)
            section_end_loc  = create_end_location_from_leg(leg)

            section = ecws.Section()
            section.trip_id = trip_entry.get_id()
            if prev_section_entry is None:
            # This is the first point, so we want to start from the start of the trip, not the start of this segment
                section_start_loc = trip_start_loc
            if i == len(our_json["plan"]["itineraries"][0]['legs']) - 1:
                # This is the last point, so we want to end at the end of the trip, not at the end of this segment
                # Particularly in this case, if we don't do this, then the trip end may overshoot the section end
                section_end_loc = trip_end_loc

            eaiss.fill_section(section, section_start_loc, section_end_loc, opt_mode_to_motiontype(leg["mode"]) )
            ## TODO: We need to create a secion entry first. Create section entry here
            section_entry = ecwe.Entry.create_entry(user_id, esda.RAW_SECTION_KEY,
                                                section, create_id=True)
            if prev_section_entry is not None:
            # If this is not the first section, create a stop to link the two sections together
            # The expectation is prev_section -> stop -> curr_section
                stop = ecwrs.Stop()
                stop.trip_id = trip_entry.get_id()
                stop_entry = ecwe.Entry.create_entry(user_id,
                                                    esda.RAW_STOP_KEY,
                                                    stop, create_id=True)
                eaiss.stitch_together(prev_section_entry, stop_entry, section_entry)
                ts.insert(stop_entry)
                ts.update(prev_section_entry)

            # After we go through the loop, we will be left with the last section,
            # which does not have an ending stop. We insert that too.
            ts.insert(section_entry)
            prev_section_entry = section_entry
            #section.trip_id = trip_entry.get_id()
            #section.start_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
            #    leg["startTime"]).timestamp, "UTC")
            #section.end_local_dt = ecsdlq.get_local_date(otp_time_to_ours(
            #    leg["endTime"]).timestamp, "UTC")
            #section.distance = float(leg["distance"])
            #section.start_loc = gj.Point( (float(leg["from"]["lat"]), float(leg["from"]["lon"])) )
            #section.end_loc = gj.Point( (float(leg["to"]["lat"]), float(leg["to"]["lon"])) )
 
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
        
        print("len(sections) = %s" % len(sections))
        final_start_loc = Coordinate(float(our_json["plan"]["from"]["lat"]), float(our_json["plan"]["from"]["lon"]))         
        final_end_loc = Coordinate(float(our_json["plan"]["to"]["lat"]), float(our_json["plan"]["to"]["lon"]))
        final_start_time = otp_time_to_ours(our_json['plan']['itineraries'][0]["startTime"])
        final_end_time = otp_time_to_ours(our_json['plan']['itineraries'][0]["endTime"])
        cost = 0
        if "RAIL" in mode_list or "SUBWAY" in mode_list:
            try:
                cost = old_div(float(our_json['plan']['itineraries'][0]['fare']['fare']['regular']['cents']), 100.0)   #gives fare in cents 
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
    return arrow.get(old_div(int(otp_str),1000))


def create_start_location_from_trip_plan(plan):
    converted_time = otp_time_to_ours(plan['itineraries'][0]["startTime"])
    time_stamp = converted_time.timestamp
    local_dt = ecsdlq.get_local_date(time_stamp, 'UTC')
    fmt_time = converted_time.to("UTC").format()
    loc = gj.Point( (float(plan["from"]["lon"]), float(plan["from"]["lat"])) )
    start_loc = ecwl.Location(
        ts =time_stamp, 
        local_dt =local_dt,
        fmt_time= fmt_time,
        loc = loc
    )
    return start_loc

def create_end_location_from_trip_plan(plan):
    converted_time = otp_time_to_ours(plan['itineraries'][0]["endTime"])
    time_stamp = converted_time.timestamp
    local_dt = ecsdlq.get_local_date(time_stamp, 'UTC')
    fmt_time = converted_time.to("UTC").format()
    loc = gj.Point( (float(plan["to"]["lon"]), float(plan["to"]["lat"])) )
    end_loc = ecwl.Location(
        ts =time_stamp, 
        local_dt =local_dt,
        fmt_time= fmt_time,
        loc = loc
    )
    return end_loc


def create_start_location_from_leg(leg):
    converted_time = otp_time_to_ours(leg['startTime'])
    time_stamp = converted_time.timestamp
    local_dt = ecsdlq.get_local_date(time_stamp, 'UTC')
    fmt_time = converted_time.to("UTC").format()
    loc = gj.Point( (float(leg["from"]["lon"]), float(leg["from"]["lat"])) )
    start_loc = ecwl.Location(
        ts =time_stamp, 
        local_dt =local_dt,
        fmt_time= fmt_time,
        loc = loc
    )
    return start_loc

def create_end_location_from_leg(leg):
    converted_time = otp_time_to_ours(leg['endTime'])
    time_stamp = converted_time.timestamp
    local_dt = ecsdlq.get_local_date(time_stamp, 'UTC')
    fmt_time = converted_time.to("UTC").format()
    loc = gj.Point( (float(leg["to"]["lon"]), float(leg["to"]["lat"])) )
    end_loc = ecwl.Location(
        ts =time_stamp, 
        local_dt =local_dt,
        fmt_time= fmt_time,
        loc = loc
    )
    return end_loc

def opt_mode_to_motiontype(opt_mode):
    mapping = {
        'CAR': ecwm.MotionTypes.IN_VEHICLE,
        'RAIL': ecwm.MotionTypes.IN_VEHICLE,
        'WALK': ecwm.MotionTypes.WALKING
    }
    if opt_mode in mapping.keys():
        return mapping[opt_mode]
    else:
        return ecwm.MotionTypes.UNKNOWN

