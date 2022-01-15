import emission.core.get_database as edb
import uuid
import argparse

import emission.core.wrapper.user as ecwu

parser = argparse.ArgumentParser(prog="intake_single_user")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-e", "--user_email")
group.add_argument("-u", "--user_uuid")

args = parser.parse_args()
if args.user_uuid:
    sel_uuid = uuid.UUID(args.user_uuid)
else:
    sel_uuid = ecwu.User.fromEmail(args.user_email).uuid

print("All inferred trips %s" % edb.get_analysis_timeseries_db().count_documents({"metadata.key": "analysis/inferred_trip", "user_id": sel_uuid}))

print("Inferred trips with inferences %s" % edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/inferred_trip", "user_id": sel_uuid, "data.inferred_labels": {"$ne": []}}).count())

print("All expected trips %s" % edb.get_analysis_timeseries_db().count_documents({"metadata.key": "analysis/expected_trip", "user_id": sel_uuid}))

print("Expected trips with inferences %s" % edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/expected_trip", "user_id": sel_uuid, "data.expectation": {"$exists": True}}).count())

for t in list(edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/inferred_trip", "user_id": sel_uuid})):
    if t["data"]["inferred_labels"] != []:
        confirmed_trip = edb.get_analysis_timeseries_db().find_one({"user_id": t["user_id"],
                "metadata.key": "analysis/confirmed_trip",
                "data.start_ts": t["data"]["start_ts"]})
        if confirmed_trip is None:
            print("No matching confirmed trip for %s" % t["data"]["start_fmt_time"])
            continue

        if confirmed_trip["data"]["user_input"] == {}:
            print("Found confirmed trip with matching inferred trip, without user labels")

print("all inferred trips %s" % (edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/inferred_trip", "user_id": sel_uuid}).count()))
print("all confirmed trips %s" % (edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip", "user_id": sel_uuid}).count()))
print("confirmed trips with inferred labels %s" % (edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip", "user_id": sel_uuid, "data.inferred_labels": {"$ne": []}}).count()))
print("confirmed trips without inferred labels %s" % (edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip", "user_id": sel_uuid, "data.inferred_labels": []}).count()))
print("confirmed trips with expectation %s" % (edb.get_analysis_timeseries_db().find({"metadata.key": "analysis/confirmed_trip", "user_id": sel_uuid, "data.expectation": {"$exists": True}}).count()))
