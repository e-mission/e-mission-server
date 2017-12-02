from __future__ import print_function
import unittest
from pymongo import MongoClient
import pygmaps
from get_database import get_section_db
from main import work_place
from pygeocoder import Geocoder
import os


class TestWorkDetection(unittest.TestCase):
    def setUp(self):
        print('### START DETECTING USER WORK ADDRESS ###')
        print('...')
        self.db = MongoClient().Test_database
        self.Test_Sections=self.db.Test_Sections
        self.Test_Trips=self.db.Test_Trips

    def tearDown(self):
        self.Test_Sections.remove()
        self.Test_Trips.remove()
        print('...')
        print('### END OF WORK DETECTION TEST ###')

    def test(self):
        workDetection = pygmaps.maps(37.8656475757, -122.258774009,14)
        for user in get_section_db().distinct('user_id'):
            user_work=work_place.detect_work_office(user)
            print(user)
            if user_work == 'N/A' or '':
                print('--- USER WORK ADDRESS UNAVAILABLE ---')
            else:
                print(Geocoder.reverse_geocode(user_work[0], user_work[1])[0])
                workDetection.addpoint(user_work[0], user_work[1], "#FF0000")
            print('...')
        if not os.path.exists('gmap_display'):
            os.makedirs('gmap_display')
        workDetection.draw('gmap_display/workDetection.html')


if __name__ == '__main__':
    unittest.main()

