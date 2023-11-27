from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from pymongo import MongoClient
import pymongo
import os
import json

try:
    config_file = open('conf/storage/db.conf')
except:
    print("storage not configured, falling back to sample, default configuration")
    config_file = open('conf/storage/db.conf.sample')

config_data = json.load(config_file)
url = config_data["timeseries"]["url"]
result_limit = config_data["timeseries"]["result_limit"]
config_file.close()

try:
    parsed=pymongo.uri_parser.parse_uri(url)
except:
    print("URL not formatted, defaulting to \"Stage_database\"")
    db_name = "Stage_database"
else:
    if parsed['database']:
        db_name = parsed['database']
    else:
        print("URL does not specify a DB name, defaulting to \"Stage_database\"")
        db_name = "Stage_database"

print("Connecting to database URL "+url)
_current_db = MongoClient(url, uuidRepresentation='pythonLegacy')[db_name]
#config_file.close()

def _get_current_db():
    return _current_db

def get_token_db():
    Tokens= _get_current_db().Stage_Tokens
    return Tokens

def get_mode_db():
    # #current_db = MongoClient().Stage_database
    Modes= _get_current_db().Stage_Modes
    return Modes

def get_habitica_db():
    # #current_db = MongoClient('localhost').Stage_database
    HabiticaAuth= _get_current_db().Stage_user_habitica_access
    return HabiticaAuth

def get_section_db():
    # current_db=MongoClient('localhost').Stage_database
    Sections= _get_current_db().Stage_Sections
    return Sections

def get_trip_db():
    # current_db=MongoClient().Stage_database
    Trips=_get_current_db().Stage_Trips
    return Trips

def get_profile_db():
    # current_db=MongoClient().Stage_database
    Profiles=_get_current_db().Stage_Profiles
    return Profiles

def get_prediction_db():
    Predictions=_get_current_db().Stage_Predictions
    return Predictions

"""
def get_routeDistanceMatrix_db():
    current_db=MongoClient().Stage_database
    routeDistanceMatrix=current_db.Stage_routeDistanceMatrix
    return routeDistanceMatrix
"""

def get_routeDistanceMatrix_db(user_id, method):
    if not os.path.exists('routeDistanceMatrices'):
        os.makedirs('routeDistanceMatrices')
    
    routeDistanceMatrix = {}
    if not os.path.exists('routeDistanceMatrices/' + user_id + '_' + method + '_routeDistanceMatrix.json'):
        data = {}
        f = open('routeDistanceMatrices/' + user_id + '_' + method + '_routeDistanceMatrix.json', 'w+')
        f.write(json.dumps({}))
        f.close()
    else:
        f = open('routeDistanceMatrices/' + user_id + '_' + method + '_routeDistanceMatrix.json', 'r')
        routeDistanceMatrix = json.loads(f.read())
    return routeDistanceMatrix

def update_routeDistanceMatrix_db(user_id, method, updatedMatrix):
    f = open('routeDistanceMatrices/' + user_id + '_' + method + '_routeDistanceMatrix.json', 'w+')
    f.write(json.dumps(updatedMatrix))
    f.close()   


def get_client_db():
    # current_db=MongoClient().Stage_database
    Clients = _get_current_db().Stage_clients
    return Clients

def get_routeCluster_db():
    # current_db=MongoClient().Stage_database
    routeCluster= _get_current_db().Stage_routeCluster
    return routeCluster

def get_groundClusters_db():
    # current_db=MongoClient().Stage_database
    groundClusters= _get_current_db().Stage_groundClusters
    return groundClusters

def get_uuid_db():
    # current_db=MongoClient().Stage_database
    UUIDs = _get_current_db().Stage_uuids
    return UUIDs

def get_client_stats_db_backup():
    # current_db=MongoClient().Stage_database
    ClientStats = _get_current_db().Stage_client_stats
    return ClientStats

def get_server_stats_db_backup():
    # current_db=MongoClient().Stage_database
    ServerStats = _get_current_db().Stage_server_stats
    return ServerStats

def get_result_stats_db_backup():
    # current_db=MongoClient().Stage_database
    ResultStats = _get_current_db().Stage_result_stats
    return ResultStats

def get_test_db():
    current_db=MongoClient().Test2
    Trips=_get_current_db().Test_Trips
    return Trips

def get_transit_db():
    # #current_db = MongoClient().Stage_database
    Transits=_get_current_db().Stage_Transits
    return Transits

def get_utility_model_db():
    # #current_db = MongoClient().Stage_database
    Utility_Models = _get_current_db().Stage_utility_models
    return Utility_Models

def get_alternatives_db():
    #current_db = MongoClient().Stage_database
    Alternative_trips=_get_current_db().Stage_alternative_trips
    return Alternative_trips

def get_perturbed_trips_db():
    #current_db = MongoClient().Stage_database
    Perturbed_trips=_get_current_db().Stage_alternative_trips
    return Perturbed_trips

def get_usercache_db():
    #current_db = MongoClient().Stage_database
    UserCache = _get_current_db().Stage_usercache
    UserCache.create_index([("user_id", pymongo.ASCENDING)])
    UserCache.create_index([("metadata.type", pymongo.ASCENDING)])
    UserCache.create_index([("metadata.key", pymongo.ASCENDING)])
    UserCache.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    UserCache.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)
    return UserCache

def _migrate_sparse_to_dense(collection, geo_index):
    # Hack to migrate from sparse to dense indices to gracefully handle
    # https://github.com/e-mission/e-mission-server/pull/849/commits/c65a4b209ed00db6aa3ad918fa631f9186ee3266
    # Should be safe to remove after Jun 2024
    index_info = collection.index_information()
    if geo_index in index_info and "sparse" in index_info[geo_index]:
        print("Found sparse geosphere index, dropping %s, index list before=%s" % (geo_index, collection.index_information().keys()))
        collection.drop_index(geo_index)
        print("Found sparse geosphere index, dropping %s, index list after=%s" % (geo_index, collection.index_information().keys()))

def get_timeseries_db():
    #current_db = MongoClient().Stage_database
    TimeSeries = _get_current_db().Stage_timeseries
    TimeSeries.create_index([("user_id", pymongo.ASCENDING)])
    TimeSeries.create_index([("metadata.key", pymongo.ASCENDING)])
    TimeSeries.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    TimeSeries.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)
    _migrate_sparse_to_dense(TimeSeries, "data.loc_2dsphere")
    TimeSeries.create_index([("data.loc", pymongo.GEOSPHERE)])
    return TimeSeries

def get_timeseries_error_db():
    #current_db = MongoClient().Stage_database
    TimeSeriesError = _get_current_db().Stage_timeseries_error
    return TimeSeriesError

def get_analysis_timeseries_db():
    """
    " Stores the results of the analysis performed on the raw timeseries
    """
    #current_db = MongoClient().Stage_database
    AnalysisTimeSeries = _get_current_db().Stage_analysis_timeseries
    AnalysisTimeSeries.create_index([("user_id", pymongo.ASCENDING)])
    _create_analysis_result_indices(AnalysisTimeSeries)
    return AnalysisTimeSeries

def get_non_user_timeseries_db():
    """
    " Stores the data that is not associated with a particular user
    """
    NonUserTimeSeries = _get_current_db().Stage_analysis_timeseries
    NonUserTimeSeries.create_index([("user_id", pymongo.ASCENDING)])
    _create_analysis_result_indices(NonUserTimeSeries)
    return NonUserTimeSeries

def get_model_db():
    """
    " Let's create a separate model DB to store periodically updated documents
    " This has a fundamentally different access pattern than the existing databases:
    " Timeseries_DB: - essentially read-only from an analysis perspective,
    " Analysis_DB: - essentially write-once from an analysis perspective (except for confirmed trip)
    " There are also many entries for each user in both these databases, since they represent timelines.
    "
    " The new model_db will effectively be write_many, since it will be
    " rewritten every time we retrain models. It will also have only a few entries per user.
    " Note that we may not actually rewrite the model every time, instead
    " choosing to keep a short history of a few archived models. However, we
    " will eventually delete them. This means that the elements are essentially
    " getting updated, only over time and as a log-structured filesystem.
    """
    ModelDB = _get_current_db().Stage_updateable_models
    ModelDB.create_index([("user_id", pymongo.ASCENDING)])
    ModelDB.create_index([("metadata.key", pymongo.ASCENDING)])
    ModelDB.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    return ModelDB

def _create_analysis_result_indices(tscoll):
    tscoll.create_index([("metadata.key", pymongo.ASCENDING)])

    # trips and sections
    tscoll.create_index([("data.start_ts", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.end_ts", pymongo.DESCENDING)], sparse=True)
    _migrate_sparse_to_dense(tscoll, "data.start_loc_2dsphere")
    _migrate_sparse_to_dense(tscoll, "data.end_loc_2dsphere")
    tscoll.create_index([("data.start_loc", pymongo.GEOSPHERE)])
    tscoll.create_index([("data.end_loc", pymongo.GEOSPHERE)])
    _create_local_dt_indices(tscoll, "data.start_local_dt")
    _create_local_dt_indices(tscoll, "data.end_local_dt")

    # places and stops
    tscoll.create_index([("data.enter_ts", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.exit_ts", pymongo.DESCENDING)], sparse=True)
    _create_local_dt_indices(tscoll, "data.enter_local_dt")
    _create_local_dt_indices(tscoll, "data.exit_local_dt")
    _migrate_sparse_to_dense(tscoll, "data.location_2dsphere")
    tscoll.create_index([("data.location", pymongo.GEOSPHERE)])
    tscoll.create_index([("data.duration", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.mode", pymongo.ASCENDING)], sparse=True)
    tscoll.create_index([("data.section", pymongo.ASCENDING)], sparse=True)
    tscoll.create_index([("data.cleaned_section", pymongo.ASCENDING)], sparse=True)

    # recreated location
    tscoll.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)
    _migrate_sparse_to_dense(tscoll, "data.loc_2dsphere")
    tscoll.create_index([("data.loc", pymongo.GEOSPHERE)])
    _create_local_dt_indices(tscoll, "data.local_dt") # recreated location
    return tscoll

def _create_local_dt_indices(time_series, key_prefix):
    """
    local_dt is an embedded document, but we will query it using the individual fields
    """
    time_series.create_index([("%s.year" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.month" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.day" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.hour" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.minute" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.second" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.weekday" % key_prefix, pymongo.DESCENDING)], sparse=True)

def get_pipeline_state_db():
    #current_db = MongoClient().Stage_database
    PipelineState = _get_current_db().Stage_pipeline_state
    return PipelineState

def get_push_token_mapping_db():
    PushTokenMapping = _get_current_db().Stage_push_token_mapping
    return PushTokenMapping

def get_common_place_db():
    #current_db = MongoClient().Stage_database
    CommonPlaces = _get_current_db().Stage_common_place
    return CommonPlaces

def get_common_trip_db():
    #current_db = MongoClient().Stage_database
    CommonTrips = _get_current_db().Stage_common_trips
    return CommonTrips

def get_fake_trips_db():
    #current_db = MongoClient().Stage_database
    FakeTrips = _get_current_db().Stage_fake_trips
    return FakeTrips

def get_fake_sections_db():
    #current_db = MongoClient().Stage_database
    FakeSections = _get_current_db().Stage_fake_sections
    return FakeSections

# Static utility method to save entries to a mongodb collection.  Single
# drop-in replacement for collection.save() now that it is deprecated in 
# pymongo 3.0. 
# https://github.com/e-mission/e-mission-server/issues/533#issuecomment-349430623
def save(db, entry):
    if '_id' in entry:
        result = db.replace_one({'_id': entry['_id']}, entry, upsert=True)
        # logging.debug("entry has id, calling with match, result = %s" % result.raw_result)
    else:
        result = db.insert_one(entry)
        # logging.debug("entry has id, calling without match, result = %s" % result.inserted_id)
