from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pymongo
import copy
import itertools

import emission.core.get_database as edb
import emission.core.wrapper.section as ecws
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm

import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta


def get_sections_for_trip(user_id, trip_id):
    curr_query = {"user_id": user_id, "data.trip_id": trip_id}
    return _get_sections_for_query(curr_query, "data.start_ts")

def get_sections_for_trip_list(user_id, trip_list):
    curr_query = {"user_id": user_id, "data.trip_id": {"$in": trip_list}}
    return _get_sections_for_query(curr_query, "data.start_ts")

def _get_sections_for_query(section_query, sort_field):
    user_id = section_query["user_id"]
    section_query.update({"metadata.key": "segmentation/raw_section"})
    logging.debug("Returning sections for query %s" % section_query)
    
    # Replace direct database calls with TimeSeries abstraction
    ts = esta.TimeSeries.get_time_series(user_id)
    # Don't include user_id in extra_query since it's already in the user_query
    extra_query = {k: v for k, v in section_query.items() 
                  if k != "metadata.key" and k != "user_id"}
    
    # Use metadata.write_ts for TimeQuery since all entries (including test data) have this field
    # This ensures we get all entries while still leveraging MongoDB sorting
    time_query = estt.TimeQuery("metadata.write_ts", 0, 9999999999)
    section_docs = ts.find_entries(["segmentation/raw_section"], 
                                  time_query=time_query,
                                  extra_query_list=[extra_query])
    
    section_entries = [ecwe.Entry(doc) for doc in section_docs]
    
    logging.debug("result length = %d" % len(section_entries))
    return section_entries

def get_inferred_mode_entry(user_id, section_id):
    curr_prediction = _get_inference_entry_for_section(user_id, section_id, "inference/prediction", "data.section_id")
    assert curr_prediction.data.algorithm_id == ecwm.AlgorithmTypes.SEED_RANDOM_FOREST, \
        "Found algorithm_id = %s, expected %s" % (curr_prediction.data.algorithm_id,
            ecwm.AlgorithmTypes.SEED_RANDOM_FOREST)
    return curr_prediction

def cleaned2inferred_section(user_id, section_id):
    curr_predicted_entry = _get_inference_entry_for_section(user_id, section_id, "analysis/inferred_section", "data.cleaned_section")
    return curr_predicted_entry

def cleaned2inferred_section_list(section_user_list):
    curr_predicted_entries = {}
    for section_userid in section_user_list:
        section_id = section_userid.get('section')
        user_id = section_userid.get('user_id')
        # Handle case where section_id or user_id is empty
        if not section_id or not user_id:
            curr_predicted_entries[str(section_id)] = ecwm.PredictedModeTypes.UNKNOWN
            continue
            
        matching_inferred_section = cleaned2inferred_section(user_id, section_id)
        if matching_inferred_section is None:
            curr_predicted_entries[str(section_id)] = ecwm.PredictedModeTypes.UNKNOWN
        else:
            curr_predicted_entries[str(section_id)] = matching_inferred_section.data.sensed_mode # PredictedModeTypes
    return curr_predicted_entries

def _get_inference_entry_for_section(user_id, section_id, entry_key, section_id_key):
    prediction_key_query = {"metadata.key": entry_key}
    inference_query = {"user_id": user_id, section_id_key: section_id}
    combo_query = copy.copy(prediction_key_query)
    combo_query.update(inference_query)
    logging.debug("About to query %s" % combo_query)
    
    # Replace direct database calls with TimeSeries abstraction
    ts = esta.TimeSeries.get_time_series(user_id)
    # Don't include user_id in extra_query since it's already in the user_query
    extra_query = {k: v for k, v in combo_query.items() 
                  if k != "metadata.key" and k != "user_id"}
    docs = ts.find_entries([entry_key], extra_query_list=[extra_query])
    ret_list = [ecwe.Entry(doc) for doc in docs]
    
    # We currently have only one algorithm
    assert len(ret_list) <= 1, "Found len(ret_list) = %d, expected <=1" % len(ret_list)
    if len(ret_list) == 0:
        logging.debug("Found no inferred prediction, returning None")
        return None
    
    assert len(ret_list) == 1, "Found ret_list of length %d, expected 1" % len(ret_list)
    curr_prediction = ret_list[0]
    return curr_prediction

