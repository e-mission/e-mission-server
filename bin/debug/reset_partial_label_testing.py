import json
import logging
import argparse
import numpy as np
import uuid

import emission.core.get_database as edb
import emission.storage.decorations.analysis_timeseries_queries as esda


parser = argparse.ArgumentParser(prog="reset_partial_label_testing")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-i", "--inferred", action='store_true')
group.add_argument("-c", "--confirmed", action='store_true')

args = parser.parse_args()

if args.inferred:
    edb.get_analysis_timeseries_db().delete_many({"metadata.key": esda.INFERRED_TRIP_KEY})
    edb.get_analysis_timeseries_db().delete_many({"metadata.key": esda.EXPECTED_TRIP_KEY})
    edb.get_analysis_timeseries_db().delete_many({"metadata.key": "inference/labels"})
    edb.get_analysis_timeseries_db().delete_many({"metadata.key": "analysis/inferred_labels"})
    edb.get_pipeline_state_db().delete_many({"pipeline_stage": {"$in": [14,15]}})

if args.confirmed:
    edb.get_analysis_timeseries_db().delete_many({"metadata.key": esda.CONFIRMED_TRIP_KEY})
    edb.get_pipeline_state_db().delete_many({"pipeline_stage": {"$in": [13]}})

