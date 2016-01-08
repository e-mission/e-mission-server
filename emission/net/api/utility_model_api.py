import emission.user_model_josh.utility_model as eum
import emission.core.get_database as edb

import json

def do_utility_analysis(info):
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

    user = UserModel("scottMoura", bike)
    user.increase_utility_by_n("time", int(value_line[5]))
    user.increase_utility_by_n("sweat", int(value_line[6]))
    user.increase_utility_by_n("scenery", int(value_line[7]))
    user.increase_utility_by_n("social", int(value_line[8]))

    time = int(value_line[5])
    sweat = int(value_line[6])
    scenery = int(value_line[7])
    social = int(value_line[8])

    trips = user.get_top_choices_lat_lng(info["start"], info["end"], info["time_info"]["when"])

    for t in trips:
        ts += (str(t.make_for_browser()))
        ts += ";"

    return json.dumps({"value" : ts})

def get_bike_info(bike_str):
    if bike_str == "walk":
        return False
    return True
