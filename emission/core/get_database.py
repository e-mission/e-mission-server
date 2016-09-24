from pymongo import MongoClient
import pymongo
import os
import json

_current_db = MongoClient('localhost').Stage_database

def _get_current_db():
    return _current_db

def get_mode_db():
    # #current_db = MongoClient().Stage_database
    Modes= _get_current_db().Stage_Modes
    return Modes

def get_moves_db():
    # #current_db = MongoClient('localhost').Stage_database
    MovesAuth= _get_current_db().Stage_user_moves_access
    return MovesAuth

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

def get_pending_signup_db():
    # current_db=MongoClient().Stage_database
    Pending_signups = _get_current_db().Stage_pending_signups
    return Pending_signups

def get_worktime_db():
    # current_db=MongoClient().Stage_database
    Worktimes= _get_current_db().Stage_Worktime
    return Worktimes

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

def get_db():
    current_db=MongoClient('localhost').Stage_database
    return current_db

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
    UserCache.create_index([("user_id", pymongo.ASCENDING),
                            ("metadata.type", pymongo.ASCENDING),
                            ("metadata.write_ts", pymongo.ASCENDING),
                            ("metadata.key", pymongo.ASCENDING)])
    UserCache.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    return UserCache

def get_timeseries_db():
    #current_db = MongoClient().Stage_database
    TimeSeries = _get_current_db().Stage_timeseries
    TimeSeries.create_index([("user_id", pymongo.HASHED)])
    TimeSeries.create_index([("metadata.key", pymongo.HASHED)])
    TimeSeries.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    TimeSeries.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)

    TimeSeries.create_index([("data.loc", pymongo.GEOSPHERE)], sparse=True)

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
    AnalysisTimeSeries.create_index([("user_id", pymongo.HASHED)])
    AnalysisTimeSeries.create_index([("metadata.key", pymongo.HASHED)])

    # trips and sections
    AnalysisTimeSeries.create_index([("data.start_ts", pymongo.DESCENDING)], sparse=True)
    AnalysisTimeSeries.create_index([("data.end_ts", pymongo.DESCENDING)], sparse=True)
    AnalysisTimeSeries.create_index([("data.start_loc", pymongo.GEOSPHERE)], sparse=True)
    AnalysisTimeSeries.create_index([("data.end_loc", pymongo.GEOSPHERE)], sparse=True)
    _create_local_dt_indices(AnalysisTimeSeries, "data.start_local_dt")
    _create_local_dt_indices(AnalysisTimeSeries, "data.end_local_dt")

    # places and stops
    AnalysisTimeSeries.create_index([("data.enter_ts", pymongo.DESCENDING)], sparse=True)
    AnalysisTimeSeries.create_index([("data.exit_ts", pymongo.DESCENDING)], sparse=True)
    _create_local_dt_indices(AnalysisTimeSeries, "data.enter_local_dt")
    _create_local_dt_indices(AnalysisTimeSeries, "data.exit_local_dt")
    AnalysisTimeSeries.create_index([("data.location", pymongo.GEOSPHERE)], sparse=True)
    AnalysisTimeSeries.create_index([("data.duration", pymongo.DESCENDING)], sparse=True)
    AnalysisTimeSeries.create_index([("data.mode", pymongo.HASHED)], sparse=True)
    AnalysisTimeSeries.create_index([("data.section", pymongo.HASHED)], sparse=True)

    # recreated location
    AnalysisTimeSeries.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)
    AnalysisTimeSeries.create_index([("data.loc", pymongo.GEOSPHERE)], sparse=True)
    _create_local_dt_indices(AnalysisTimeSeries, "data.local_dt") # recreated location
    return AnalysisTimeSeries

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
