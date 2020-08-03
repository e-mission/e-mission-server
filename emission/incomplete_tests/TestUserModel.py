from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import emission.user_model_josh.utility_model as eum
import emission.net.ext_service.otp.otp as otp
import datetime
import os


class UserModelTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDelta(self):
        base = eum.UserBase()
        josh = eum.UserModel(base)
        josh.increase_utility_by_n("scenery", 100)
        josh.increase_utility_by_n("noise", 10)
        start, end = (1, 1), (2, 2)  ## GPS points
        josh.add_to_last(start, end)

        ## would find top score for a trip here

        self.assertFalse(josh.delta(start, end))
        josh.increase_utility_by_n("scenery", 1)
        self.assertTrue(josh.delta(start, end))
        josh.add_to_last(start, end)
        self.assertFalse(josh.delta(start, end))

    def testGetTopScore(self):
        t1 = eum.CampusTrip((1, 2, 3, 4), 60, 1, "fake")
        t2 = eum.CampusTrip((10, 20, 30, 40), 120, 2, "fake")
        t3 = eum.CampusTrip((100, 200, 300, 400), 180, 3, "fake")
        base = eum.UserBase()
        josh = eum.UserModel()
        self.assertEqual(josh.get_top_n((t1, t2, t3), 1), [t3])
        self.assertEqual(josh.get_top_n((t1, t2, t3), 2), [t3, t2])


    def testE2E(self):
        base = eum.UserBase()
        josh = eum.UserModel()
        josh.increase_utility_by_n("time", 10)
        josh.increase_utility_by_n("noise", 10)
        start, end = (37.504712, -122.314189), (37.509580, -122.321560)   ## GPS points
        josh.add_to_last(start, end)

        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = curr_time.day
        curr_hour = curr_time.hour

        self.base_url = os.environ("OTP_SERVER")
        walk_otp = otp.OTP(self.base_url).route(start, end, "WALK", eum.write_day(curr_month, curr_day, curr_year), eum.write_time(curr_hour, curr_minute), False)
        bike_otp = otp.OTP(self.base_url).route(start, end, "BICYCLE", eum.write_day(curr_month, curr_day, curr_year), eum.write_time(curr_hour, curr_minute), True)

        
        choices = walk_otp.get_all_trips(0,0,0) + bike_otp.get_all_trips(0,0,0)

        trips = josh.get_top_choices_lat_lng(start, end, tot_trips=choices)

        
        end = (37.512048, -122.316614)
        self.assertTrue(josh.delta(start, end))
        josh.add_to_last(start, end)
        self.assertFalse(josh.delta(start, end))
        josh.increase_utility_by_n("time", -200)
        josh.add_to_last(start, end)


        new_choices = josh.get_top_choices_lat_lng(start, end, tot_trips=choices)




    def testNormalizationTimes(self):
        base = eum.UserBase()
        josh = eum.UserModel()
        josh.increase_utility_by_n("scenery", 100)
        josh.increase_utility_by_n("noise", 10)
        start, end = (37.504712, -122.314189), (37.509580, -122.321560)   ## GPS points
        josh.add_to_last(start, end)

        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = curr_time.day
        curr_hour = curr_time.hour

        self.base_url = os.environ("OTP_SERVER")
        walk_otp = otp.OTP(self.base_url).route(start, end, "WALK", eum.write_day(curr_month, curr_day, curr_year), eum.write_time(curr_hour, curr_minute), False)
        bike_otp = otp.OTP(self.base_url).route(start, end, "BICYCLE", eum.write_day(curr_month, curr_day, curr_year), eum.write_time(curr_hour, curr_minute), False)

        choices = walk_otp.get_all_trips(0,0,0) + bike_otp.get_all_trips(0,0,0)



        normal_times = eum.get_normalized_times(choices)
        normal_sweat = eum.get_normalized_sweat(choices, True)
        normal_beauty = eum.get_normalized_beauty(choices)

        print("normal times = %s" % normal_times)
        print("normal sweat = %s" % normal_sweat)
        print("normal beauty = %s" % normal_beauty)

        self.assertAlmostEqual(sum(normal_times), 1)
        self.assertAlmostEqual(sum(normal_sweat), 1)
        self.assertAlmostEqual(sum(normal_beauty), 1)








if __name__ == "__main__":
    unittest.main()

