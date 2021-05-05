from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from past.utils import old_div
from main import gmap_display
from pymongo import MongoClient
from main import pygmaps_modified as pygmaps
import webbrowser
import sys
import os

db = MongoClient().Stage_database


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("USAGE: %s trip_id\n" % sys.argv[0])
        print("option: -o , immediately open file in default browser")
        print("OUTPUT: plot will be stored at ./plots/trip_id.html")
        print("COLOR SCHEME FOR THE PLOT:\n")
        print("walking - blue\nrunning - green\ncycling - yellow\ntransport - red\nbus - aqua\ntrain - darkOrange\ncar - grey\nmixed - olive\nair -skyBlue\n")
        exit(1)
    else:
        trip_id = sys.argv[1]
        sections =db.Stage_Sections.find({'trip_id': trip_id})
        if db.Stage_Sections.count_documents({'trip_id': trip_id}) == 0:
            print("\nTRIP NOT FOUND\n")
            exit(1)
        startpoint = sections[0]['section_start_point']
        distance = sections[0]['distance']
        gmap = None
        startCoord = None
        if startpoint == None:
            print("start_point missing corodinates")
        else:
            startCoord = startpoint['coordinates']
            gmap = pygmaps.maps(startCoord[1], startCoord[0], min(15, old_div(165000.0,distance)))
            gmap_display.drawTrip(trip_id, db, gmap)
            if not os.path.exists('plots'):
                os.makedirs('plots')
            gmap.draw('plots/' + trip_id + '.html')
            try: 
                if sys.argv[2] == '-o':
                    webbrowser.open('plots/' + trip_id + '.html')
            except:
                exit(1)
