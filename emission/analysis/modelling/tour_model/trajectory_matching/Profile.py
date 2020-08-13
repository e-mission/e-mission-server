from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
__author__ = 'Yin'
import logging
from pygeocoder import Geocoder
import math

# Our imports
from emission.analysis.modelling.home import detect_home, detect_home_from_db
from zipcode import get_userZipcode
from emission.analysis.modelling.work_place import detect_work_office, detect_daily_work_office
from emission.core.get_database import get_trip_db, get_section_db,get_profile_db
from emission.core.common import calDistance
from emission.analysis.modelling.tour_model.trajectory_matching.route_matching import update_user_routeDistanceMatrix, update_user_routeClusters
from emission.analysis.modelling.tour_model.K_medoid import kmedoids, user_route_data
import emission.analysis.modelling.tour_model.cluster_pipeline as cp

TOLERANCE = 200 #How much movement we allow before updating zip codes again. Should be pretty large.. this is conservative
Profiles=get_profile_db()

def update_profiles(dummy_users=False):
    if dummy_users:
        user_list = ['1']
    else:
        user_list = get_section_db().distinct('user_id')
    for user in user_list:
        generate_route_clusters(user)

def generate_route_clusters(user):
    print("In profile, generating route clusters for %s" % user)
    data = cp.read_data(uuid=user)
    data, bins = cp.remove_noise(data, 300)
    num_clusters, labels, data = cp.cluster(data, len(bins))
    clusters = {}
    for i in range(num_clusters):
        idx = labels.index(i)
        tripid = data[idx].trip_id
        clusters[tripid] = []
        for j in range(len(labels)):
            if labels[j] == i:
                clusters[tripid].append(data[j].trip_id)
    update_user_routeClusters(user, clusters)
# print(Profiles.count_documents())
# for profile in Profiles.find():
#     print(profile)

def _check_zip_validity(user_home, user):
    if user_home != "N/A" and detect_home_from_db(user) != "N/A" and calDistance(user_home, detect_home_from_db(user)) < TOLERANCE:
        return True
    return False

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)
    update_profiles()

