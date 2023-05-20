import arrow
import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.timequery as estt

for ue in edb.get_uuid_db().find():
    trip_count = edb.get_analysis_timeseries_db().count_documents({"user_id": ue["uuid"], "metadata.key": "analysis/confirmed_trip"})
    location_count = edb.get_timeseries_db().count_documents({"user_id": ue["uuid"], "metadata.key": "background/location"})
    first_trip = list(edb.get_analysis_timeseries_db().find({"user_id": ue["uuid"], "metadata.key": "analysis/confirmed_trip"}).sort("data.end_ts", 1).limit(1))
    first_trip_time = first_trip[0]["data"]["end_fmt_time"] if len(first_trip) > 0 else None
    last_trip = list(edb.get_analysis_timeseries_db().find({"user_id": ue["uuid"], "metadata.key": "analysis/confirmed_trip"}).sort("data.end_ts", -1).limit(1))
    last_trip_time = last_trip[0]["data"]["end_fmt_time"] if len(last_trip) > 0 else None
    now = arrow.now()
    month_ago = now.shift(months=-1)
    last_month_tq = estt.TimeQuery("data.start_ts", month_ago.timestamp(), now.timestamp())
    profile = edb.get_profile_db().find_one({"user_id": ue["uuid"]})
    if 'computeConfirmed' in vars(ecwu.User):
        confirmed_pct, valid_replacement_pct, score = ecwu.User.computeConfirmed(ue["uuid"], last_month_tq)
        print(f"For {ue['user_email']} on {profile['curr_platform']} app version {profile['client_app_version']}: Trip count = {trip_count}, location count = {location_count}, first trip = {first_trip_time}, last trip = {last_trip_time}, confirmed_pct ({month_ago} -> {now}) = exactly {confirmed_pct:.2f}")
    else:
        confirmed_count = edb.get_analysis_timeseries_db().count_documents({"user_id": ue["uuid"], "metadata.key": "analysis/confirmed_trip", "data.user_input": {"$ne": {}}})
        confirmed_pct = confirmed_count / trip_count if trip_count != 0 else 0
        print(f"For {ue['user_email']} on {profile['curr_platform']} app version {profile['client_app_version']}: Trip count = {trip_count}, location count = {location_count}, first trip = {first_trip_time}, last trip = {last_trip_time}, confirmed_pct  = approximately {confirmed_pct:.2f}")
