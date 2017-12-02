from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from get_database import get_section_db, get_routeCluster_db
from uuid import UUID
import random

# Our imports
from emission.analysis.modelling.tour_model.trajectory_matching.route_matching import get_common_routes_for_user, getRoute
from . import pygmaps_modified as pygmaps
from .gmap_display import drawSection

def plot_each_route_cluster_for_user(user_id,method='lcs'):
    i=0
    Sections = get_section_db()
    user_route_clusters = get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':method}]})
    # plot each cluster as a file.
    for idx in user_route_clusters['clusters'].keys():
        print(idx)
        gmap = pygmaps.maps(37.8717, -122.2728, 14)
        # gmap = pygmaps.maps(getRoute(idx)[0][0], getRoute(idx)[0][1], 14)
        section=Sections.find_one({'_id': idx})
        r = lambda: random.randint(0,255)
        color = '#%02X%02X%02X' % (r(),r(),r())
        drawSection(section, 'path', gmap,color)
        print(len(user_route_clusters['clusters'][idx]))
        first = True
        for idi in user_route_clusters['clusters'][idx]:
            # print(Sections.find({'_id': idi}).count())
            section=Sections.find_one({'_id': idi})
            if first:
                print(section)
                first = False
            color = '#%02X%02X%02X' % (r(),r(),r())
            drawSection(section, 'path', gmap,color)
        gmap.draw(str(user_id) + '_'+ method+ '_'+str(i) + '.html')
        i+=1
        break


def plot_common_routes_for_user(user_id,method='lcs'):
    Sections = get_section_db()
    idxs = get_common_routes_for_user(user_id,method)
    gmap = pygmaps.maps(37.8717, -122.2728, 14)
    for idx in idxs:
        r = lambda: random.randint(0,255)
        color = '#%02X%02X%02X' % (r(),r(),r())
        section = Sections.find_one({'_id': idx})
        print(section)
        drawSection(section,'path',gmap,color)
    gmap.draw(str(user_id) + '_'+ method + '.html')

if __name__ == "__main__":
    plot_each_route_cluster_for_user(UUID("0763de67-f61e-3f5d-90e7-518e69793954"),method="dtw")   
    # plot_common_routes_for_user(UUID("0763de67-f61e-3f5d-90e7-518e69793954"),method="dtw")   

