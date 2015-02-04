import logging
from pymongo import MongoClient
from home import detect_home,detect_home_from_db
from common import getModeShare
# from get_database import get_section_db
# from commute import get_morning_commute_mode_share_by_distance
from pygeocoder import Geocoder
from collections import defaultdict
from get_database import get_section_db,get_profile_db
from modeshare import get_user_mode_share_by_distance

# zipcode_list = ["other", "94720", "94709", "94705", "94706", "94703", "94704"]

def getDistinctUserCount():
    Profiles=get_profile_db()
    distinctUserCount = len(Profiles.distinct("user_id"))
    logging.debug("Found %s distinct users " % distinctUserCount)
    return distinctUserCount
   

def get_userZipcode(user):
    location = detect_home_from_db(user)
    current_db = get_profile_db()
    user_pro=current.find_one({"$and":[{'source':'Shankari'},{'user_id':user_id}]})
    if user_pro['zip']:
        return user_pro['zip']
    

    if location!='N/A':
        # Convert from our internal GeoJSON specific (lng, lat) to the (lat, lng)
        # format required by geocoder
        zipcode = Geocoder.reverse_geocode(location[1],location[0])
        zip=zipcode[0].postal_code
        user_pro['zip'] = zip
        return zip
    else:
        return 'N/A'


def getZipcode():
    Profiles=get_profile_db()
    zips = Profiles.distinct("zip")
    # print(users)
    userZipcodes = []
    ZipDictCount={}
    ZipDictUser = defaultdict(list)
    for zip in zips:
        if zip!='N/A':
            ZipDictCount[zip] = Profiles.find({'zip':zip}).count()
    # print(set(userZipcodes))
    # print(list(set(userZipcodes)))
    return ZipDictCount



def get_mode_share_by_Zipcode(zip,flag,start,end):
    Profiles=get_profile_db()
    user_list=[]
    Modeshare={}
    logging.debug("Called get_mode_share_by_Zipcode(%s)" % zip)
    for profile in Profiles.find({'zip':zip}):
        user_list.append(profile['user_id'])
    if len(user_list)!=0:
        # print(user_list)
        for user in user_list:
            user_share=get_user_mode_share_by_distance(user,flag,start,end)
            # print(user_share)
            for mode in user_share.keys():
                if mode not in Modeshare:
                    Modeshare[mode]=user_share[mode]
                else:
                    Modeshare[mode]+=user_share[mode]
        return Modeshare
    else:
        return 'N/A'
