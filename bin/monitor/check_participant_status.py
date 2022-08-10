import emission.core.get_database as edb

for ue in edb.get_uuid_db().find():
    trip_count = edb.get_analysis_timeseries_db().count_documents({"user_id": ue["uuid"], "metadata.key": "analysis/confirmed_trip"})
    location_count = edb.get_timeseries_db().count_documents({"user_id": ue["uuid"], "metadata.key": "background/location"})
    first_trip = list(edb.get_analysis_timeseries_db().find({"user_id": ue["uuid"], "metadata.key": "analysis/confirmed_trip"}).sort("data.end_ts", 1).limit(1))
    first_trip_time = first_trip[0]["data"]["end_fmt_time"] if len(first_trip) > 0 else None
    last_trip = list(edb.get_analysis_timeseries_db().find({"user_id": ue["uuid"], "metadata.key": "analysis/confirmed_trip"}).sort("data.end_ts", -1).limit(1))
    last_trip_time = last_trip[0]["data"]["end_fmt_time"] if len(last_trip) > 0 else None
    print(f"For {ue['user_email']}: Trip count = {trip_count}, location count = {location_count}, first trip = {first_trip_time}, last trip = {last_trip_time}")
