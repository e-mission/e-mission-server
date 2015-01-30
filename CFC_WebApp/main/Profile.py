__author__ = 'Yin'
import logging
from home import detect_home
from zipcode import get_userZipcode
from work_place import detect_work_office, detect_daily_work_office
from get_database import get_section_db,get_profile_db
from pygeocoder import Geocoder
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)
Profiles=get_profile_db()

for user in get_section_db().distinct('user_id'):
    user_home=detect_home(user)
    logging.debug('starting for %s' % user)
    if Profiles.find({'user_id':user}).count()==0:
        profile_todo={'source':'Shankari','user_id': user,'home':user_home}
        Profiles.insert(profile_todo)
    else:
        Profiles.update({"$and":[{'source':'Shankari'},
                                     {'user_id':user}]},{"$set":{'home':user_home}})
    user_work=detect_work_office(user)
    Profiles.update({"$and":[{'source':'Shankari'},{'user_id':user}]},{"$set":{'work_place':user_work}})
    user_zip=get_userZipcode(user)
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
# print(Profiles.find().count())
# for profile in Profiles.find():
#     print(profile)
