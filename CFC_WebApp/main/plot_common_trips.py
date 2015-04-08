from get_database import get_section_db, get_routeCluster_db
from route_matching import get_common_routes_for_user
import pygmaps
from gmap_display import drawSection


def plot_each_route_cluster_for_user(user_id,method='lcs'):
    i=0
    Sections = get_section_db()
    user_route_clusters = get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':method}]})
    # plot each cluster as a file.
    for idx in user_route_clusters.keys():
        gmap = pygmaps.maps(getRoute(idx)[0][0], getRoute(idx)[0][1], 14)
        section=Sections.find_one({'_id': idx})
        drawSection(section, 'path', gmap)
        for idi in user_route_clusters[idx]:
            # print(Sections.find({'_id': idi}).count())
            section=Sections.find_one({'_id': idi})
            drawSection(section, 'path', gmap)
        gmap.draw(str(user_id) + '_'+ method+ '_'+str(i) + '.html')
        i+=1


def plot_common_routes_for_user(user_id,method='lcs'):
    Sections = get_section_db()
    idxs = get_common_routes_for_user(user_id,method)
    gmap = pygmaps.maps(37.8717, -122.2728, 14)
    for idx in idxs:
        section = Sections.find_one({'_id': idx})
        drawSection(section,'path',gmap)
    gmap.draw(str(user_id) + '_'+ method + '.html')
