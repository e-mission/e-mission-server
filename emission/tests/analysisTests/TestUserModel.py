import unittest
import emission.user_model.utility_model as eum
import googlemaps
import emission.net.ext_service.otp.otp as otp
import emission.net.ext_service.gmaps.googlemaps as gmaps
import emission.net.ext_service.gmaps.common as gmcommon
import datetime


class UserModelTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDelta(self):
        base = eum.UserBase()
        josh = eum.UserModel("josh", base)
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
        josh = eum.UserModel("josh", base)
        self.assertEqual(josh.get_top_n((t1, t2, t3), 1), [t3])
        self.assertEqual(josh.get_top_n((t1, t2, t3), 2), [t3, t2])


    def testE2E(self):
        base = eum.UserBase()
        josh = eum.UserModel("josh", base)
        josh.increase_utility_by_n("scenery", 100)
        josh.increase_utility_by_n("noise", 10)
        start, end = (37.504712, -122.314189), (37.509580, -122.321560)   ## GPS points
        josh.add_to_last(start, end)

        choices = josh.get_top_choices_lat_lng(start, end)
        
        end = (37.512048, -122.316614)
        self.assertTrue(josh.delta(start, end))
        josh.add_to_last(start, end)
        self.assertFalse(josh.delta(start, end))
        josh.increase_utility_by_n("noise", 1000000)
        josh.increase_utility_by_n("scenery", -100)
        josh.add_to_last(start, end)

        new_choices = josh.get_top_choices_lat_lng(start, end)
        self.assertFalse(new_choices == choices)


    def testNormalizationScores(self):
        bs = eum.parse_beauty()
        normal = eum.normalize_scores(bs)
        
        tot = 0
        for area in normal:
             tot += area.beauty
        print normal
        self.assertEqual(tot, 1)

    def testNormalizationTimes(self):
        start = (37.870637,-122.259722)
        end = (37.872277,-122.25639)
        base = eum.UserBase()
        josh = eum.UserModel("josh", base)
        all_trips = josh.get_all_trips(start, end)

        normal_times = eum.get_normalized_times(all_trips)
        normal_sweat = eum.get_normalized_sweat(all_trips)
        normal_beauty = eum.get_normalized_beauty(all_trips)

        print "normal times = %s" % normal_times
        print "normal sweat = %s" % normal_sweat
        print "normal beauty = %s" % normal_beauty

    def test_points(self):
        start = (37.870637,-122.259722)
        end = (37.872277,-122.25639)

        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = curr_time.day
        curr_hour = curr_time.hour
        our_gmaps = gmaps.GoogleMaps("AIzaSyAFsQeO3Xj60s0nBVRcAS-I9FLw6KZPV-E") 

        jsn = our_gmaps.directions(start, end, 'walking')
        gmaps_options = gmcommon.google_maps_to_our_trip(jsn, 0, 0, 0, "walking", curr_time)
        # self.assertEqual(gmaps_options[0].trip_start_location.to_tuple(), start)
        # self.assertEqual(gmaps_options[0].trip_end_location.to_tuple(), end) 

        walk_otp = otp.OTP(start, end, "WALK", eum.write_day(curr_month, curr_day, curr_year), eum.write_time(curr_hour, curr_minute), False)
        lst_of_trips = walk_otp.get_all_trips(0, 0, 0)

        # self.assertEqual(lst_of_trips[0].trip_start_location.to_tuple(), start)
        # self.assertEqual(lst_of_trips[0].trip_end_location.to_tuple(), end)




unittest.main()

