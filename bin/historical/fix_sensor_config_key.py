from __future__ import print_function
import emission.core.get_database as edb


def fix_key(check_field, new_key):
    print("First entry for "+new_key+" is %s" % list(edb.get_timeseries_db().find(
                                    {"metadata.key": "config/sensor_config",
                                    check_field: {"$exists": True}}).sort(
                                        "metadata/write_ts").limit(1)))
    udb = edb.get_usercache_db()
    tdb = edb.get_timeseries_db()
    for i, entry in enumerate(edb.get_timeseries_db().find(
                                    {"metadata.key": "config/sensor_config",
                                    check_field: {"$exists": True}})):
        entry["metadata"]["key"] = new_key
        if i % 10000 == 0:
            print(udb.insert(entry))
            print(tdb.remove(entry["_id"]))
        else:
            udb.insert(entry)
            tdb.remove(entry["_id"])

fix_key("data.battery_status", "background/battery")
fix_key("data.latitude", "background/location")
fix_key("data.zzaEh", "background/motion_activity")
fix_key("data.currState", "statemachine/transition")
