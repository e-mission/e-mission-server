__author__ = 'Yin'
from pymongo import MongoClient
from home import detect_home
from work_place import detect_daily_work_office
from get_database import get_section_db
from common import Is_date, Is_place
from dateutil import parser
from common import parse_time, travel_time

########################################## morning commute ########################################################
def get_daily_morning_commute_sections(user_id,day):
    # say should be from 1 to 5
    # get a list of all the sections for Mon, or ...
    Sections=get_section_db()
    list_of_commute=[]
    candidate_sections=[]
    home=detect_home(user_id)
    work=detect_daily_work_office(user_id,day)
    if work == 'N/A':
        return []
    else:
        # print(list_first_pnt)
        for section in Sections.find({"$and":[{"user_id": user_id},{ "section_start_point": { "$ne": None }},\
                                              {'commute':{ "$exists": False }}]}):
            if Is_date(section['section_start_time'],day):
                candidate_sections.append(section)

        if len(candidate_sections)>0:
            candidate_sections = sorted(candidate_sections, key=lambda k: parser.parse(k['section_start_time']))
            max_sec=0
            for i in range(len(candidate_sections)):
                if i>=max_sec:
                    if Is_place(candidate_sections[i]['section_start_point'],home,200):
                        for j in range(i,len(candidate_sections)):
                            if Is_place(candidate_sections[j]['section_end_point'],work,200) and \
                                            travel_time(candidate_sections[i]['section_start_time'],\
                                                        candidate_sections[j]['section_end_time'])<=24*60*60:
                                sections_todo=[]
                                sections_todo.extend(candidate_sections[i:j+1])
                                list_of_commute.append(sections_todo)
                                max_sec=j+1
                                break
        return list_of_commute

def get_user_morning_commute_sections(user):
    # say should be from 1 to 5
    # get a list of all the sections for Mon, or ...
    Sections=get_section_db()
    list_of_commute=[]
    for date in range(1,6):
        list_of_commute.extend(get_daily_morning_commute_sections(user,date))
    return list_of_commute

###################################### evening commute #########################################################

def get_daily_evening_commute_sections(user_id,day):
    # say should be from 1 to 5
    # get a list of all the sections for Mon, or ...
    earlist_start=5
    earlist_end=15
    Sections=get_section_db()
    list_of_commute=[]
    candidate_sections=[]
    home=detect_home(user_id)
    work=detect_daily_work_office(user_id,day)
    if work == 'N/A':
        return []
    else:
        # print(list_first_pnt)
        for section in Sections.find({"$and":[{"user_id": user_id},{ "section_start_point": { "$ne": None }},\
                                              {'commute':{ "$exists": False }}]}):
            time2=parse_time(section['section_start_time'])
            if (Is_date(section['section_start_time'],day) and (time2 - time2.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()/3600>earlist_end) or \
                    (Is_date(section['section_start_time'],day+1) and (time2 - time2.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()/3600<earlist_start):
                candidate_sections.append(section)

        if len(candidate_sections)>0:
            candidate_sections = sorted(candidate_sections, key=lambda k: parser.parse(k['section_start_time']))
            min_sec=len(candidate_sections)
            for i in range(len(candidate_sections)-1,-1,-1):
                if i<=min_sec:
                    if Is_place(candidate_sections[i]['section_end_point'],home,200):
                        for j in range(i,-1,-1):
                            if Is_place(candidate_sections[j]['section_start_point'],work,200) and \
                                            travel_time(candidate_sections[j]['section_start_time'],\
                                                        candidate_sections[i]['section_end_time'])<=24*60*60:
                                sections_todo=[]
                                sections_todo.extend(candidate_sections[j:i+1])
                                list_of_commute.append(sections_todo)
                                min_sec=j-1
                                break
        return list_of_commute

def get_user_evening_commute_sections(user):
    # say should be from 1 to 5
    # get a list of all the sections for Mon, or ...
    Sections=get_section_db()
    list_of_commute=[]
    for date in range(1,6):
        list_of_commute.extend(get_daily_evening_commute_sections(user,date))
    return list_of_commute
