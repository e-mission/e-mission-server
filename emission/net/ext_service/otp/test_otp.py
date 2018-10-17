import unittest
import random
import datetime 
import emission.net.ext_service.otp.otp as otp
import emission.core.wrapper.location as ecwl
import emission.storage.decorations.local_date_queries as ecsdlq
import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.cache_series as estcs
from past.utils import old_div
import arrow


class TestOTPMethods(unittest.TestCase):
    def setUp(self):
        start_point = (37.77264255,-122.399714854263)
        end_point = (37.42870635,-122.140926605802)
        mode = "TRANSIT"
        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = random.randint(1, 28)
        curr_hour = random.randint(0, 23)
        date = "%s-%s-%s" % (curr_month, curr_day, curr_year)
        time = "%s:%s" % (curr_hour, curr_minute) 
        self.opt_trip = otp.OTP(start_point, end_point, mode, date, time, bike=True)

    def test_create_start_location_form_leg(self):
        legs = self.opt_trip.get_json()["plan"]["itineraries"][0]['legs']
        first_leg = legs[0]
        start_loc = otp.create_start_location_from_leg(first_leg)
        self.assertEqual(start_loc.ts,otp.otp_time_to_ours(first_leg['startTime']).timestamp )
        self.assertEqual(start_loc.local_dt, ecsdlq.get_local_date(start_loc.ts, 'UTC'))
        #print(start_loc)

    def test_create_start_location_form_trip_plan(self):
        trip_plan = self.opt_trip.get_json()["plan"]
        start_loc = otp.create_start_location_from_trip_plan(trip_plan)

    def test_get_json(self):
        pass

    def test_legs_json(self):
       #legs = self.opt_trip.get_json()["plan"]["itineraries"][0]['legs']
       pass 
    
    def test_turn_into_new_trip(self):
        fake_user_email = 'test_otp_insert'
        user = ecwu.User.register(fake_user_email)
        override_uuid = user.uuid
        #self.opt_trip.turn_into_new_trip(override_uuid)
    
    def test_make_url(self):
        pass
       #print(self.opt_trip.make_url())
    
    def test_get_locations_along_route(self):
        pass
        #locations = self.opt_trip.get_locations_along_route()
        #print(locations)
        #self.assertNotEqual(len(locations), 0)
        #pass

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
        new_measurement = otp.create_measurement(coorindate, time_stamp, velocity, user_id)

    def test_save_entries_to_db(self):
        fake_user_email = 'test_insert_fake_data'
        user = ecwu.User.register(fake_user_email)
        override_uuid = user.uuid
        location_entries = self.opt_trip.get_locations_along_route(override_uuid)
        (tsdb_count, ucdb_count) = estcs.insert_entries(override_uuid, location_entries)
        print("Finished loading %d entries into the usercache and %d entries into the timeseries" %
        (ucdb_count, tsdb_count))

if __name__ == '__main__':
    unittest.main()


