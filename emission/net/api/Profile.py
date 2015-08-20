# Standard imports
__author__ = 'Yin'
import logging
from pygeocoder import Geocoder
import math

# Our imports
from emission.analysis.modelling.home import detect_home, detect_home_from_db
from zipcode import get_userZipcode
from emission.analysis.modelling.work_place import detect_work_office, detect_daily_work_office
from emission.core.get_database import get_section_db,get_profile_db
from emission.core.common import calDistance
from emission.analysis.modelling.tour_model.trajectory_matching.route_matching import update_user_routeDistanceMatrix, update_user_routeClusters
from emission.analysis.modelling.tour_model.K_medoid import kmedoids, user_route_data

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)
Profiles=get_profile_db()
TOLERANCE = 200 #How much movement we allow before updating zip codes again. Should be pretty large.. this is conservative

def update_profiles(dummy_users=False):
    if dummy_users:
        user_list = ['1']
    else:
        user_list = get_section_db().distinct('user_id')
    for user in user_list:
        generate_user_home_work(user)
        generate_route_clusters(user, 20)

def generate_user_home_work(user):
    user_home=detect_home(user)
    # print user_home
    zip_is_valid = _check_zip_validity(user_home, user)
    logging.debug('starting for %s' % user)
    if Profiles.find({'user_id':user}).count()==0:
        profile_todo={'source':'Shankari','user_id': user,'home':user_home}
        Profiles.insert(profile_todo)
    else:
        #TODO: make detect_home return something better than a N/A string
        Profiles.update({"$and":[{'source':'Shankari'},
                                     {'user_id':user}]},{"$set":{'home':user_home}})
    user_work=detect_work_office(user)
    Profiles.update({"$and":[{'source':'Shankari'},{'user_id':user}]},{"$set":{'work_place':user_work}})
    user_zip=get_userZipcode(user, zip_is_valid)
    Profiles.update({"$and":[{'source':'Shankari'},{'user_id':user}]},{"$set":{'zip':user_zip}})
    if user_zip!='N/A':
        geoinfo= Geocoder.geocode(user_zip)
        # geocoder returns data in lat,lng format.
        # we convert to lng,lat internally, since that is the format that the
        # rest of our code is in
        zipCen=[geoinfo[0].coordinates[1],geoinfo[0].coordinates[0]]
    else:
        zipCen='N/A'
    Profiles.update({"$and":[{'source':'Shankari'},{'user_id':user}]},{"$set":{'zip_centroid':zipCen}})


    for day in range(1,6):
        key='work'+str(day)
        Profiles.update({"$and":[{'source':'Shankari'},
                                     {'user_id':user}]},{"$set":{key:detect_daily_work_office(user,day)}})

def generate_route_clusters(user, nClusters = -1):
    ## update route clusters:
    print "In profile, generating route clusters for %s" % user
    routes_user = user_route_data(user,get_section_db())
    #print(routes_user.keys())
    update_user_routeDistanceMatrix(user,routes_user,step1=100000,step2=100000,method='dtw')
    if nClusters == -1:
        nClusters = int(math.ceil(len(routes_user)/8) + 1)
    clusters_user = kmedoids(routes_user,nClusters,user,method='dtw')
    update_user_routeClusters(user,clusters_user[2],method='dtw')
# print(Profiles.find().count())
# for profile in Profiles.find():
#     print(profile)

def _check_zip_validity(user_home, user):
    if user_home != "N/A" and detect_home_from_db(user) != "N/A" and calDistance(user_home, detect_home_from_db(user)) < TOLERANCE:
        return True
    return False

if __name__ == '__main__':
    update_profiles()

