from dao.user import User
import json 

import sys, os, random

# This is in here so the pygmaps associated functions can be imported 
# from the webapp
# sys.path.append("%s/../../CFC_WebApp/" % os.getcwd())
# This is in here so the getCanonicalTrips function can be imported 
# from the recommender pipeline
sys.path.append("%s/../CFC_DataCollector/" % os.getcwd())
from uuid import UUID 
    
def getUserTour(user_uuid):
    """
    Gets a users "tour"
    """
    from recommender.filter_modules import getCanonicalTrips
    canonical_trips = getCanonicalTrips(user_uuid)
    print "Number of canonical_trips: %i" % len(canonical_trips)
    return canonical_trips
            
def generate_tour_map(user_uuid):
    """
    Generates tour map and saves it to result_template.html 
    """
    import main.pygmaps_modified as pygmaps
    from main.gmap_display import drawSection
    from get_database import get_section_db, get_routeCluster_db
    gmap = pygmaps.maps(37.8717, -122.2728, 14)
    r = lambda: random.randint(0,255)
    color = '#%02X%02X%02X' % (r(),r(),r())
    canonical_trips = getUserTour(user_uuid)
    section = canonical_trips[0]
    drawSection(section, 'path', gmap, color)
    os.remove('clients/commontrips/result_template.html')
    gmap.draw('clients/commontrips/result_template.html')

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  generate_tour_map(user_uuid)
  renderedTemplate = template("clients/commontrips/result_template.html")
  return renderedTemplate
