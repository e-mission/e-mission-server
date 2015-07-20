__author__ = 'Yin'
# Standard imports
from dateutil import parser

# Our imports
import home
import emission.core.common as ec
import emission.core.get_database as edb

def detect_work_office(user_id):

    Sections=edb.get_section_db()
    office_candidate=[]
    home=home.detect_home(user_id)

    if home == 'N/A':
      return 'N/A'

    # Else, get home locations
    # print(list_first_pnt)
    for section in Sections.find({"$and":[{"user_id": user_id},{ "section_start_point": { "$ne": None }}]}):
        section_start_pnt=section['section_start_point']
        section_end_pnt=section['section_end_point']
        if ec.Is_weekday(section['section_start_time'])==True:
            # parameter that the distance away from home:
            away_home=400
            if not ec.Is_place(section_start_pnt,home,away_home) and not ec.Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
                office_candidate.append(section['track_points'][-1])
            elif ec.Is_place(section_start_pnt,home,away_home) and not ec.Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][-1])
            elif not ec.Is_place(section_start_pnt,home,away_home) and ec.Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
    if len(office_candidate)>0:
        office_candidate = sorted(office_candidate, key=lambda k: parser.parse(k['time']))
        # print(office_candidate)
        weighted_office_candidate=ec.get_static_pnts(office_candidate)
        office_location=ec.most_common(weighted_office_candidate,200)
        # print(len(office_candidate))
        # print(len(weighted_office_candidate))
        # print(ec.calculate_appearance_rate(office_candidate,office_location))
        # print(ec.calculate_appearance_rate(weighted_office_candidate,office_location))
        return office_location
    else:
        return 'N/A'

def detect_daily_work_office(user_id,day):
    # say should be from 1 to 5
    Sections=edb.get_section_db()
    office_candidate=[]
    home=home.detect_home(user_id)

    if home == 'N/A':
      return 'N/A'

    # print(list_first_pnt)
    for section in Sections.find({"$and":[{"user_id": user_id},{ "section_start_point": { "$ne": None }}]}):
        section_start_pnt=section['section_start_point']
        section_end_pnt=section['section_end_point']
        if ec.Is_date(section['section_start_time'],day)==True:
            # parameter that the distance away from home:
            away_home=400
            if not ec.Is_place(section_start_pnt,home,away_home) and not ec.Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
                office_candidate.append(section['track_points'][-1])
            elif ec.Is_place(section_start_pnt,home,away_home) and not ec.Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][-1])
            elif not ec.Is_place(section_start_pnt,home,away_home) and ec.Is_place(section_end_pnt,home,away_home):
                office_candidate.append(section['track_points'][0])
    if len(office_candidate)>0:
        office_candidate = sorted(office_candidate, key=lambda k: parser.parse(k['time']))
        # print(office_candidate)
        weighted_office_candidate=ec.get_static_pnts(office_candidate)
        office_location=ec.most_common(weighted_office_candidate,200)
        # print(len(office_candidate))
        # print(len(weighted_office_candidate))
        # print(ec.calculate_appearance_rate(office_candidate,office_location))
        # print(ec.calculate_appearance_rate(weighted_office_candidate,office_location))
        return office_location
    else:
        return 'N/A'

def detect_work_office_from_db(user_id):
    Profiles=edb.get_profile_db()
    user_pro=Profiles.find_one({"$and":[{'source':'Shankari'},{'user_id':user_id}]})
    return user_pro['work_place']

def detect_daily_work_office_from_db(user_id,day):
    Profiles=edb.get_profile_db()
    user_pro=Profiles.find_one({"$and":[{'source':'Shankari'},{'user_id':user_id}]})
    return user_pro['work'+str(day)]
