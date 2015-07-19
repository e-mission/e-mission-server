import logging
from commute import get_user_morning_commute_sections,get_user_evening_commute_sections
from work_time import get_user_work_start_time,get_user_work_end_time
from get_database import get_section_db,get_worktime_db
from datetime import datetime
from common import parse_time
from datetime import timedelta

Sections=get_section_db()
Worktimes=get_worktime_db()
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)
#### morning ######
for user in Sections.distinct('user_id'):
    Usermorningsections=get_user_morning_commute_sections(user)
    logging.debug("About to update %s morning commute sections for user %s" % (len(Usermorningsections), user))
    for sections in Usermorningsections:
        for ss in sections:
            Sections.update({"_id":ss['_id']},{"$set":{'commute': 'to'}})

##### evening ######
for user in Sections.distinct('user_id'):
    Usereveningsections=get_user_evening_commute_sections(user)
    logging.debug("About to update %s evening commute sections for user %s" % (len(Usereveningsections), user))
    for sections in Usereveningsections:
        for ss in sections:
            Sections.update({"_id":ss['_id']},{"$set":{'commute': 'from'}})
##### None #####
nonCommuteQuery = {"$and":[{'source':'Shankari'},{'commute':{ "$exists": False }}]}
logging.debug("About to update %s non-commute sections across users" % Sections.find(nonCommuteQuery).count())
for section in Sections.find(nonCommuteQuery):
    Sections.update({'_id':section['_id']},{"$set":{'commute': None}})

################# work time #######################
for user in Sections.distinct('user_id'):
    starttime=get_user_work_start_time(user)
    endtime=get_user_work_end_time(user)
    for time in starttime:
        time1=parse_time(time)
        date1=time1.replace(hour=0, minute=0, second=0, microsecond=0)
        hour1=round((time1 - time1.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()/3600,2)
        if Worktimes.find({"$and":[{'user_id':user},{'date': date1}]}).count()==0:
            time_todo={'user_id':user,'date':date1,'arr_hour':hour1}
            Worktimes.insert(time_todo)
        else:
            Worktimes.update({"$and":[{'user_id':user},{'date': date1}]},{"$set":{'arr_hour': hour1}})
    for time in endtime:
        time2=parse_time(time)
        date2=time2.replace(hour=0, minute=0, second=0, microsecond=0)
        hour2=round((time2 - time2.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()/3600,2)
        if hour2<=5:
            date2=date2-timedelta(days=1)
        if Worktimes.find({"$and":[{'user_id':user},{'date': date2}]}).count()==0:
            time_todo={'user_id':user,'date':date2,'dep_hour':hour2}
            Worktimes.insert(time_todo)
        else:
            Worktimes.update({"$and":[{'user_id':user},{'date': date2}]},{"$set":{'dep_hour': hour2}})
###### remove ###################
# Sections.update({'commute':'from'},{"$set":{'commute': None}},multi=True)
# Worktimes.remove()
############################# test ##########################################
# for section in Sections.find({"$or":[{'commute':'to'},{'commute':'from'}]}):
#     print(section)
# for section in Sections.find({'commute':'from'}):
#     print(section)

# for item in Worktimes.find():
#     print(item)
