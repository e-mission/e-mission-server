__author__ = 'Yin'
from pymongo import MongoClient
from home import detect_home,detect_home_from_db
from tripManager import calDistance
from common import Is_weekday, get_static_pnts, most_common, calculate_appearance_rate, Is_date,Is_place
from dateutil import parser
from get_database import get_section_db, get_profile_db


def detect_work_office(user_id):

    Sections=get_section_db()
    office_candidate=[]
    home=detect_home(user_id)

    if home == 'N/A':
      return 'N/A'

    # Else, get home locations
    # print(list_first_pnt)
    for section in Sections.find({"$and":[{"user_id": user_id},{ "section_start_point": { "$ne": None }}]}):
        section_start_pnt=section['section_start_point']
        section_end_pnt=section['section_end_point']
        if Is_weekday(section['section_start_time'])==True:
            # parameter that the distance away from home:
            away_home=400
            if not Is_place(section_start_pnt,home,away_home) and not Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
                office_candidate.append(section['track_points'][-1])
            elif Is_place(section_start_pnt,home,away_home) and not Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][-1])
            elif not Is_place(section_start_pnt,home,away_home) and Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
    if len(office_candidate)>0:
        office_candidate = sorted(office_candidate, key=lambda k: parser.parse(k['time']))
        # print(office_candidate)
        weighted_office_candidate=get_static_pnts(office_candidate)
        office_location=most_common(weighted_office_candidate,200)
        # print(len(office_candidate))
        # print(len(weighted_office_candidate))
        # print(calculate_appearance_rate(office_candidate,office_location))
        # print(calculate_appearance_rate(weighted_office_candidate,office_location))
        return office_location
    else:
        return 'N/A'

def detect_daily_work_office(user_id,day):
    # say should be from 1 to 5
    Sections=get_section_db()
    office_candidate=[]
    home=detect_home(user_id)

    if home == 'N/A':
      return 'N/A'

    # print(list_first_pnt)
    for section in Sections.find({"$and":[{"user_id": user_id},{ "section_start_point": { "$ne": None }}]}):
        section_start_pnt=section['section_start_point']
        section_end_pnt=section['section_end_point']
        if Is_date(section['section_start_time'],day)==True:
            # parameter that the distance away from home:
            away_home=400
            if not Is_place(section_start_pnt,home,away_home) and not Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
                office_candidate.append(section['track_points'][-1])
            elif Is_place(section_start_pnt,home,away_home) and not Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][-1])
            elif not Is_place(section_start_pnt,home,away_home) and Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
    if len(office_candidate)>0:
        office_candidate = sorted(office_candidate, key=lambda k: parser.parse(k['time']))
        # print(office_candidate)
        weighted_office_candidate=get_static_pnts(office_candidate)
        office_location=most_common(weighted_office_candidate,200)
        # print(len(office_candidate))
        # print(len(weighted_office_candidate))
        # print(calculate_appearance_rate(office_candidate,office_location))
        # print(calculate_appearance_rate(weighted_office_candidate,office_location))
        return office_location
    else:
        return 'N/A'

def detect_work_office_from_db(user_id):
    Profiles=get_profile_db()
    user_pro=Profiles.find_one({"$and":[{'source':'Shankari'},{'user_id':user_id}]})
    return user_pro['work_place']

def detect_daily_work_office_from_db(user_id,day):
    Profiles=get_profile_db()
    user_pro=Profiles.find_one({"$and":[{'source':'Shankari'},{'user_id':user_id}]})
    return user_pro['work'+str(day)]
