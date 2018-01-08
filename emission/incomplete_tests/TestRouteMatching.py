from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import *
from pymongo import MongoClient
import pygmaps
from get_database import get_section_db
from main import route_matching
import unittest


class TestRouteMatching(unittest.TestCase):
    def setUp(self):
        print('### START TESTING ROUTE MATCHING ###')
        print('...')
        self.Sections = MongoClient().Stage_database.Stage_Sections
        self.sampleRoutes = MongoClient().Routes.Similar_Routes

    def tearDown(self):
        print('...')
        print('### END OF ROUTE MATCHING TEST ###')

    
    def test_realRoutes_positive(self):
        pAccu = 0
        fNeg = 0
        for List in self.sampleRoutes.find({'type':'real_match'}):
            ID = List['list']
            for n in range(len(ID)):
                lst1 = self.Sections.find({'_id':ID[n]})[0]['track_points']
                for m in range(n+1,len(ID)):
                    lst2 = self.Sections.find({'_id':ID[m]})[0]['track_points']
                    if route_matching.route_matching(lst1,lst2,150,1400,0.8,0.9):
                        pAccu += 1
                    else:
                        fNeg += 1
        
        print('Number of Runs = ' + str(pAccu + fNeg))
        print('Positive Accuracy = ' + str(pAccu*100/(pAccu+fNeg)) + '%')
        print('False Negative = ' + str(fNeg*100/(pAccu+fNeg)) + '%')
    

    
    def test_realRoutes_negative(self):
        nAccu = 0
        fPos = 0
        for List in self.sampleRoutes.find({'type':'real_mismatch'}):
            ID = List['list']
            for n in range(len(ID)):
                lst1 = self.Sections.find({'_id':ID[n]})[0]['track_points']
                for m in range(n+1,len(ID)):
                    lst2 = self.Sections.find({'_id':ID[m]})[0]['track_points']
                    if route_matching.route_matching(lst1,lst2,150,1400,0.8,0.9):
                        fPos += 1
                    else:
                        nAccu += 1
        
        print('Number of Runs = ' + str(nAccu + fPos))
        print('Negative Accuracy = ' + str(nAccu*100/(nAccu+fPos)) + '%')
        print('False Positive = ' + str(fPos*100/(nAccu+fPos)) + '%')
    
    

    def test_artificialRoutes_positive(self):
        print('### Testing Artificial Routes ###')

        pAccu = 0
        fNeg = 0
        for List in self.sampleRoutes.find({'type':'artificial_match'}):
            ID = List['list']
            for n in range(len(ID)):
                lst1 = ID[n]['track_points']
                for m in range(n+1,len(ID)):
                    lst2 = ID[m]['track_points']
                    if route_matching.route_matching(lst1,lst2,150,1400,0.8,0.9):
                        pAccu += 1
                    else:
                        fNeg += 1
        
        print('Number of Runs = ' + str(pAccu + fNeg))
        print('Positive Accuracy = ' + str(pAccu*100/(pAccu+fNeg)) + '%')
        print('False Negative = ' + str(fNeg*100/(pAccu+fNeg)) + '%')


if __name__ == '__main__':
    unittest.main()
