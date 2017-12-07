from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
from pymongo import MongoClient
import pygmaps
from get_database import get_section_db
from main import home
from pygeocoder import Geocoder
import webbrowser
import os



class TestHomeDetection(unittest.TestCase):
    def setUp(self):
        print('### START DETECTING USER HOME ADDRESS ###')
        print('...')
        self.db = MongoClient().Test_database
        self.Test_Sections=self.db.Test_Sections
        self.Test_Trips=self.db.Test_Trips


    def tearDown(self):
        self.Test_Sections.remove()
        self.Test_Trips.remove()
        print('...')
        print('### END OF HOME DETECTION TEST ###')


    def test(self):
        homeDetection = pygmaps.maps(37.8656475757, -122.258774009,14)
        for user in get_section_db().distinct('user_id'):
            user_home=home.detect_home(user)
            print(user)
            if user_home == 'N/A' or '':
                print('--- USER HOME ADDRESS UNAVAILABLE ---')
            else:
                print(Geocoder.reverse_geocode(user_home[0], user_home[1])[0])
                homeDetection.addpoint(user_home[0], user_home[1], "#FF0000")
            print('...')
        if not os.path.exists('gmap_display'):
            os.makedirs('gmap_display')
        homeDetection.draw('gmap_display/homeDetection.html')


if __name__ == '__main__':
    unittest.main()
