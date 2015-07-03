import unittest 
from trip_generator.trip_gen import *
from math import radians, cos, sin, asin, sqrt
from OurGeocoder import ReverseGeocode, Geocode
from 



def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    Copied from 
    	https://stackoverflow.com/questions/4913349/haversine-formula-in-python-bearing-and-distance-between-two-gps-points
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r


class TestFakeTrips(unittest.TestCase):

	def test_get_random_point(self):
	    a = "2703 Hallmark Dr Belmont CA"
	    gc = Geocode(a)
	    coords = a.get_coords()
	    p1 = get_one_random_point_in_radius(a, 1)
	    p2 = get_one_random_point_in_radius(a, 2)
	    # self.assertTrue(p1.get_lon(), pt1.get_lat(), coords.get_lon(), coords.get_lat() < 2)
	    # self.assertTrue(p2.get_lon(), pt2.get_lat(), coords.get_lon(), coords.get_lat() < 3)
	    self.assertTrue(p1.get_lat())
	    self.assertTrue(p1.get_lon())
	    self.assertTrue(p2.get_lat())
	    self.assertTrue(p2.get_lon())

	def sanity_check():
		c = create_fake_trips()
		assertTrue(len(c.starting_points) > 0)
        assertTrue(len(c.ending_points) > 0)
        assertTrue(len(c.a_to_b) > 0)

