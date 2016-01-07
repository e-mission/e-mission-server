import emission.user_model.utility_model as eum
import emission.core.get_database as edb

import json

def get_json_to_upload_to_browser():
    db = edb.get_utility_model_db()
    record = db.find({"pushing" : True}).sort({"at" : -1}).limit(1)
    return json.dumps(record)

def write_to_db_from_browser(info):
    db = edb.get_utility_model_db()
    at = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
    value_line = value_line.split(";")
    start = value_line[0]
    end = value_line[1]
    
    start = base.geocode_with_cache(start)
    end = base.geocode_with_cache(end)
    
    time_info = {}
    if value_line[2] == "leaveNow":
        time_info["leave"] = True
        time_info["when"] = datetime.datetime.now()
        print "leaveNow"
    elif value_line[2] == "leaveAt":
        time_info["leave"] = True
        time_info["when"] = str_time_to_datetme(value_line[3])
        print "leaveAt"
    elif value_line[2] == "thereBy":
        time_info["leave"] = False
        time_info["when"] = str_time_to_datetme(value_line[3])
        print "arriveAt"

    bike = get_bike_info(value_line[4])

    time = int(value_line[5])
    sweat = int(value_line[6])
    scenery = int(value_line[7])
    social = int(value_line[8])

    db.insert({"pushing" : False, "at" : at, "time" : time_info, "beauty" : scenery, "social" : social, "sweat" : sweat, "start" : start, "end" : end, "bike" : bike})


def get_bike_info(bike_str):
    if bike_str == "walk":
        return False
    return True
