__author__ = 'Yin'
# Standard imports

# Our imports
import emission.core.get_database as edb
import work_place as wp
import emission.core.common as ec

time_list = [[0,2],[2,4],[4,6],[6,8], [8,10], [10,12], [12,14], [14,16], [16,18], [18,20],[20,22],[22,24]]
def get_work_start_time(user_id,day):
    # day should be from 1 to 5
    # get a list of work starttime for Mon, or ...
    Sections=edb.get_section_db()
    list_of_time=[]
    candidate_pnts=[]
    work=wp.detect_daily_work_office(user_id,day)

    for section in Sections.find({'$and':[{"user_id": user_id},{"commute":'to'}]}):
        if work!='N/A' and ec.Is_place(section['section_end_point'],work,200):
            list_of_time.append(section['section_end_time'])
    return list_of_time

def get_work_end_time(user_id,day):
    # day should be from 1 to 5
    # get a list of work starttime for Mon, or ...
    Sections=edb.get_section_db()
    list_of_time=[]
    candidate_pnts=[]
    work=wp.detect_daily_work_office(user_id,day)

    for section in Sections.find({'$and':[{"user_id": user_id},{"commute":'from'}]}):
        if work!='N/A' and ec.Is_place(section['section_start_point'],work,200):
            list_of_time.append(section['section_end_time'])
    return list_of_time

def get_user_work_start_time(user):
    list_of_time=[]
    for day in range(1,6):
        list_of_time.extend(get_work_start_time(user,day))
    return list_of_time

def get_user_work_end_time(user):
    list_of_time=[]
    for day in range(1,6):
        list_of_time.extend(get_work_end_time(user,day))
    return list_of_time

def get_Alluser_work_start_time():
    list_of_time=[]
    Profiles=edb.get_profile_db()
    for user in Profiles.distinct("user_id"):
        for day in range(1,6):
            list_of_time.extend(get_work_start_time(user,day))
    return list_of_time

def get_Alluser_work_end_time():
    list_of_time=[]
    Profiles=edb.get_profile_db()
    for user in Profiles.distinct("user_id"):
        for day in range(1,6):
            list_of_time.extend(get_work_end_time(user,day))
    return list_of_time
############################################## pie chart below ###############################################

def get_user_work_start_time_pie(user,start,end):
    Worktimes=edb.get_worktime_db()
    timeCountMap = {}
    for timesection in time_list:

        key=str(timesection[0]).zfill(2) +':01 - '+str(timesection[1]).zfill(2) +':00'
        timeCountMap[key] =Worktimes.find({"$and":[{'user_id':user},{'arr_hour':{"$gte": timesection[0], "$lt": timesection[1]}},\
                                                   {"date": {"$gte": start, "$lt": end}}]}).count()

    return timeCountMap

def get_user_work_end_time_pie(user,start,end):
    Worktimes=edb.get_worktime_db()
    timeCountMap = {}
    for timesection in time_list:

        key=str(timesection[0]).zfill(2) +':01 - '+str(timesection[1]).zfill(2) +':00'
        timeCountMap[key] =Worktimes.find({"$and":[{'user_id':user},{'dep_hour':{"$gte": timesection[0], "$lt": timesection[1]}},\
                                                   {"date": {"$gte": start, "$lt": end}}]}).count()

    return timeCountMap

def get_Alluser_work_start_time_pie(start,end):
    Worktimes=edb.get_worktime_db()
    timeCountMap = {}
    for timesection in time_list:

        key=str(timesection[0]).zfill(2) +':01 - '+str(timesection[1]).zfill(2) +':00'
        timeCountMap[key] =Worktimes.find({'arr_hour':{"$gte": timesection[0], "$lt": timesection[1]}},\
                                          {"date": {"$gte": start, "$lt": end}}).count()

    return timeCountMap

def get_Alluser_work_end_time_pie(start,end):
    Worktimes=edb.get_worktime_db()
    timeCountMap = {}
    for timesection in time_list:

        key=str(timesection[0]).zfill(2) +':01 - '+str(timesection[1]).zfill(2) +':00'
        timeCountMap[key] =Worktimes.find({'dep_hour':{"$gte": timesection[0], "$lt": timesection[1]}},\
                                          {"date": {"$gte": start, "$lt": end}}).count()

    return timeCountMap
