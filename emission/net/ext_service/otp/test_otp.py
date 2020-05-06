import unittest
import random
import datetime 
import emission.net.ext_service.otp.otp as otp
import emission.core.wrapper.location as ecwl
import emission.core.wrapper.localdate as ecwld
import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.cache_series as estcs
import emission.storage.timeseries.abstract_timeseries as esta
from past.utils import old_div
import arrow
import geocoder
import requests


class TestOTPMethods(unittest.TestCase):
    def setUp(self):
        start_point_1 = (37.77264255,-122.399714854263)
        end_point_1 = (37.42870635,-122.140926605802)
        start_point_2 = (37.42870635,-122.140926605802)
        end_point_2 = (37.76624, -122.43456)
        start_point_3 = (37.76976, -122.43422)
        end_point_3 = (37.87119, -122.27388)
        mode_1 = "TRANSIT"
        mode_2 = 'CAR'
        mode_3 = 'CAR'
        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = random.randint(1, 9)
        hour_1 = random.randint(0, 10)
        hour_2 = hour_1 + 5
        hour_3 = hour_2 + 5
        date = "%s-%s-%s" % (curr_month, curr_day, curr_year)
        time_1 = "%s:%s" % (hour_1, curr_minute) 
        time_2 = "%s:%s" % (hour_2, curr_minute) 
        time_3 = "%s:%s" % (hour_3, curr_minute) 

        self.opt_trip_1 = otp.OTP(start_point_1, end_point_1, mode_1, date, time_1, bike=True)
        self.opt_trip_2 = otp.OTP(start_point_2, end_point_2, mode_2, date, time_2, bike=False)
        self.opt_trip_3 = otp.OTP(start_point_2, end_point_2, mode_2, date, time_3, bike=False)

    def test_create_start_location_form_leg(self):
        legs = self.opt_trip_1.get_json()["plan"]["itineraries"][0]['legs']
        first_leg = legs[0]
        start_loc = otp.create_start_location_from_leg(first_leg)
        self.assertEqual(start_loc.ts,otp.otp_time_to_ours(first_leg['startTime']).timestamp )
        self.assertEqual(start_loc.local_dt, ecwld.LocalDate.get_local_date(start_loc.ts, 'UTC'))
        #print(start_loc)

    def test_create_start_location_form_trip_plan(self):
        trip_plan = self.opt_trip_1.get_json()["plan"]
        start_loc = otp.create_start_location_from_trip_plan(trip_plan)

    def test_legs_json(self):
       #legs = self.opt_trip_1.get_json()["plan"]["itineraries"][0]['legs']
       pass 
    
    def test_turn_into_new_trip(self):
        #fake_user_email = 'test_otp_insert'
        #user = ecwu.User.register(fake_user_email)
        #override_uuid = user.uuid
        #self.opt_trip_1.turn_into_new_trip(override_uuid)
        pass 
    
    def test_get_measurements_along_route(self):
        ##Test that the last 
        #fake_user_email = 'test_time_delta'
        #user = ecwu.User.register(fake_user_email)
        #locations = self.opt_trip_1.get_locations_along_route(user.uuid)
        #print(locations[-1], locations[-2])
        #time_delta = arrow.get(locations[-1].data.ts) - arrow.get(locations[-2].data.ts)
        #self.assertGreater(time_delta.total_seconds(), 300)
        pass 
    def test_get_average_velocity(self):
        start_time = arrow.utcnow().timestamp 
        end_time  = arrow.utcnow().shift(seconds=+200).timestamp
        distance = 500
        velocity = otp.get_average_velocity(start_time, end_time, distance)
        self.assertAlmostEquals(velocity, 2.5)

    def test_get_time_at_next_location(self):
        prev_loc, next_loc, time_at_prev, velocity = (37.77264255,-122.399714854263), (37.42870635,-122.140926605802), arrow.utcnow().timestamp, 10 
        time_at_next_loc = otp.get_time_at_next_location(prev_loc, next_loc, time_at_prev, velocity)
        #print(arrow.get(time_at_prev).humanize())
        #print(arrow.get(time_at_next_loc).humanize())
        self.assertGreater(time_at_next_loc, time_at_prev)

    def test_create_measurement_obj(self):
        coorindate = (37.77264255,-122.399714854263)
        time_stamp = arrow.utcnow().timestamp
        user_id = 123
        velocity = 5
        altitude = 0.1
        new_measurement = otp.create_measurement(coorindate, time_stamp, velocity, altitude, user_id)
        #print(new_measurement)

    def test_save_entries_to_db(self):
        fake_user_email = 'test_insert_fake_data_84'
        user = ecwu.User.register(fake_user_email)
        override_uuid = user.uuid
        location_entries = self.opt_trip_1.get_measurements_along_route(override_uuid)
        ts = esta.TimeSeries.get_time_series(override_uuid)
        #result = ts.bulk_insert(location_entries)
        #print(type(location_entries[-1].data.ts))
        (tsdb_count, ucdb_count) = estcs.insert_entries(override_uuid, location_entries)
        print("Finished loading %d entries into the usercache and %d entries into the timeseries" %
        (ucdb_count, tsdb_count))
        
        #Trip 2
        location_entries = self.opt_trip_2.get_measurements_along_route(override_uuid)
        (tsdb_count, ucdb_count) = estcs.insert_entries(override_uuid, location_entries)
        print("Finished loading %d entries into the usercache and %d entries into the timeseries" %
        (ucdb_count, tsdb_count))

        # Trip 3
        location_entries = self.opt_trip_3.get_measurements_along_route(override_uuid)
        (tsdb_count, ucdb_count) = estcs.insert_entries(override_uuid, location_entries)
        print("Finished loading %d entries into the usercache and %d entries into the timeseries" %
        (ucdb_count, tsdb_count))

    def test_create_motion_entry(self):
        time_stamp = arrow.utcnow().timestamp
        user_id = 123
        start_time = arrow.utcnow().timestamp
        leg = {
            'mode': "BICYCLE",
            'startTime' : start_time,
            'endTime':arrow.utcnow().shift(minutes=+60).timestamp 
        }
        new_motion_entry = otp.create_motion_entry_from_leg(leg, user_id)
        #print(new_motion_entry)
        #self.assertEqual(start_time, new_motion_entry.data.ts)

    def test_get_elevation(self):
        coordinate = (37.77264255,-122.399714854263)
        #print('Elevation',otp.get_elevation(coordinate))


if __name__ == '__main__':
    unittest.main()


