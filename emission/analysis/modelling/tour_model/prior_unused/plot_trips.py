import sys, os
sys.path.append("%s/../" % os.getcwd())
from get_database import get_section_db, get_routeCluster_db
from gmap_display import drawTripsForUser
import pygmaps
from uuid import UUID


if __name__ == "__main__":
    Sections = get_section_db()
    gmap = pygmaps.maps(37.8717, -122.2728, 14)
    print "plotting"
    drawTripsForUser(UUID('b0d937d0-70ef-305e-9563-440369012b39'), Sections, gmap)
    gmap.draw("all_trips.html")
