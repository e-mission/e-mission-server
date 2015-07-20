# Standard imports
import json 
import sys, os, random
from uuid import UUID 

# Our imports
import emission.core.wrapper.user import User
    
def getUserTour(user_uuid):
    """
    Gets a users "tour"
    """
    # This is in here so the getCanonicalTrips function can be imported 
    # from the recommender pipeline
    sys.path.append("%s/../CFC_DataCollector/" % os.getcwd())
    from recommender.filter_modules import getCanonicalTrips
    import recommender.tripiterator as ti
    from recommender.trip import Canonical_E_Mission_Trip
    canonical_trips = list(ti.TripIterator(user_uuid,["recommender", "get_improve"], Canonical_E_Mission_Trip))
    for cluster in canonical_trips:
        # print(cluster.start_point_distr)
        # print(cluster.end_point_distr)
        # print(cluster.start_time_distr)
        # print(cluster.end_time_distr)
        print cluster._id
    # representative_trips = map(lambda x: x.trip_id, canonical_trips)
    sys.path.remove("%s/../CFC_DataCollector/" % os.getcwd())

    # print "Number of canonical_trips: %i" % len(canonical_trips)
    return canonical_trips
            
def generate_tour_map(user_uuid):
    """
    Generates tour map and saves it to result_template.html 
    """
    sys.path.append("%s/../../CFC_WebApp/" % os.getcwd())
    import main.pygmaps_modified as pygmaps
    from main.gmap_display import drawSection
    from get_database import get_section_db, get_routeCluster_db
    gmap = pygmaps.maps(37.8717, -122.2728, 14)
    r = lambda: random.randint(0,255)
    canonical_trips = getUserTour(user_uuid)
    for section in canonical_trips:        
        color = '#%02X%02X%02X' % (r(),r(),r())
        # print section._id
        section_json = get_section_db().find_one({'_id': section._id})
	drawSection(section_json, 'path', gmap, color)
    try:
        os.remove('clients/commontrips/result_template.html')
    except OSError, e:
        print "Result of removing the result template is %s" % e
    gmap.draw('clients/commontrips/result_template.html')
    sys.path.remove("%s/../../CFC_WebApp/" % os.getcwd())

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template
  print "common trips getResult UUID: %s" % user_uuid
  generate_tour_map(user_uuid)
  renderedTemplate = template("clients/commontrips/result_template.html")
  return renderedTemplate

