import json
import logging
import argparse
import numpy as np
import uuid

import emission.core.get_database as edb
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.core.wrapper.user as ecwu

parser = argparse.ArgumentParser(prog="reset_partial_label_testing")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-i", "--inferred", action='store_true')
group.add_argument("-c", "--confirmed", action='store_true')

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-e", "--user_email")
group.add_argument("-u", "--user_uuid")
group.add_argument("-a", "--all")

args = parser.parse_args()

if args.user_uuid:
    sel_uuid = uuid.UUID(args.user_uuid)
    base_query = {"user_id": sel_uuid}
elif args.user_email:
    sel_uuid = ecwu.User.fromEmail(args.user_email).uuid
    base_query = {"user_id": sel_uuid}
else:
    sel_uuid = None
    base_query = {}

# Using dict comprehension instead of update so that we can keep a sequence of
# one-liners for ease of understanding
# based on the first comment in https://stackoverflow.com/q/1452995/4040267

if args.inferred:
    print(edb.get_analysis_timeseries_db().delete_many(
        dict(base_query, **{"metadata.key": esda.INFERRED_TRIP_KEY})).raw_result)
    print(edb.get_analysis_timeseries_db().delete_many(
        dict(base_query, **{"metadata.key": esda.EXPECTED_TRIP_KEY})).raw_result)
    print(edb.get_analysis_timeseries_db().delete_many(
        dict(base_query, **{"metadata.key": "inference/labels"})).raw_result)
    print(edb.get_analysis_timeseries_db().delete_many(
        dict(base_query, **{"metadata.key": "analysis/inferred_labels"})).raw_result)
    print(edb.get_pipeline_state_db().delete_many(
        dict(base_query, **{"pipeline_stage": {"$in": [14,15]}})).raw_result)

if args.confirmed:
    print(edb.get_analysis_timeseries_db().delete_many(
        dict(base_query, **{"metadata.key": esda.EXPECTED_TRIP_KEY})).raw_result)
    print(edb.get_analysis_timeseries_db().delete_many(
        dict(base_query, **{"metadata.key": esda.CONFIRMED_TRIP_KEY})).raw_result)
    print(edb.get_pipeline_state_db().delete_many(
        dict(base_query, **{"pipeline_stage": {"$in": [13]}})).raw_result)

