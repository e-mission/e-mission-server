__author__ = 'Yin'
import emission.core.get_database as edb
import emission.core.common as ec

def detect_home(user_id):

    Sections=edb.get_section_db()
    list_first_pnt=[]
    list_home_candidate=[]
    for section in Sections.find({"$and":[{"user_id": user_id},{ "section_start_point": { "$ne": None }}]}):
            list_first_pnt.append(section['track_points'][0])
    # print(list_first_pnt)
    list_home_candidate=ec.get_first_daily_point(list_first_pnt)
    # print(type(list_home_candidate))
    # print(type(list_home_candidate[0]))
    if len(list_home_candidate)==0:
        return 'N/A'
    else:
        home_location=ec.most_common(list_home_candidate,500)
        return home_location

def detect_home_from_db(user_id):
    Profiles=edb.get_profile_db()
    user_pro=Profiles.find_one({"$and":[{'source':'Shankari'},{'user_id':user_id}]})
    if user_pro is None or 'home' not in user_pro:
        return 'N/A'
    return user_pro['home']
