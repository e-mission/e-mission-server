from pymongo import MongoClient
import pymongo
import os
import json

def get_mode_db():
    current_db = MongoClient().Stage_database
    Modes=current_db.Stage_Modes
    return Modes

def get_moves_db():
    current_db = MongoClient('localhost').Stage_database
    MovesAuth=current_db.Stage_user_moves_access
    return MovesAuth

def get_section_db():
    current_db=MongoClient('localhost').Stage_database
    Sections=current_db.Stage_Sections
    return Sections

def get_trip_db():
    current_db=MongoClient().Stage_database
    Trips=current_db.Stage_Trips
    return Trips

def get_profile_db():
    current_db=MongoClient().Stage_database
    Profiles=current_db.Stage_Profiles
    return Profiles

def get_utility_model_db():
    current_db=MongoClient().Stage_database
    Profiles=current_db.Stage_utility_models
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
    current_db=MongoClient().Stage_database
    Clients = current_db.Stage_clients
    return Clients

def get_routeCluster_db():
    current_db=MongoClient().Stage_database
    routeCluster=current_db.Stage_routeCluster
    return routeCluster

def get_groundClusters_db():
    current_db=MongoClient().Stage_database
    groundClusters=current_db.Stage_groundClusters
    return groundClusters

def get_pending_signup_db():
    current_db=MongoClient().Stage_database
    Pending_signups = current_db.Stage_pending_signups
    return Pending_signups

def get_worktime_db():
    current_db=MongoClient().Stage_database
    Worktimes=current_db.Stage_Worktime
    return Worktimes

def get_uuid_db():
    current_db=MongoClient().Stage_database
    UUIDs = current_db.Stage_uuids
    return UUIDs

def get_client_stats_db():
    current_db=MongoClient().Stage_database
    ClientStats = current_db.Stage_client_stats
    return ClientStats

def get_server_stats_db():
    current_db=MongoClient().Stage_database
    ServerStats = current_db.Stage_server_stats
    return ServerStats

def get_result_stats_db():
    current_db=MongoClient().Stage_database
    ResultStats = current_db.Stage_result_stats
    return ResultStats

def get_db():
    current_db=MongoClient('localhost').Stage_database
    return current_db

def get_test_db():
    current_db=MongoClient().Test2
    Trips=current_db.Test_Trips
    return Trips

def get_transit_db():
    current_db = MongoClient().Stage_database
    Transits=current_db.Stage_Transits
    return Transits

def get_alternatives_db():
    current_db = MongoClient().Stage_database
    Alternative_trips=current_db.Stage_alternative_trips
    return Alternative_trips

def get_perturbed_trips_db():
    current_db = MongoClient().Stage_database
    Perturbed_trips=current_db.Stage_alternative_trips
    return Perturbed_trips

def get_usercache_db():
    current_db = MongoClient().Stage_database
    UserCache = current_db.Stage_usercache
    return UserCache

def get_timeseries_db():
    current_db = MongoClient().Stage_database
    TimeSeries = current_db.Stage_timeseries
    TimeSeries.create_index([("user_id", pymongo.HASHED)])
    TimeSeries.create_index([("metadata.key", pymongo.HASHED)])
    TimeSeries.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    TimeSeries.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)
    TimeSeries.create_index([("data.start_ts", pymongo.DESCENDING)], sparse=True)
    TimeSeries.create_index([("data.end_ts", pymongo.DESCENDING)], sparse=True)
    TimeSeries.create_index([("data.enter_ts", pymongo.DESCENDING)], sparse=True)
    TimeSeries.create_index([("data.exit_ts", pymongo.DESCENDING)], sparse=True)
    TimeSeries.create_index([("data.loc", pymongo.GEOSPHERE)], sparse=True)
    return TimeSeries

def get_timeseries_error_db():
    current_db = MongoClient().Stage_database
    TimeSeriesError = current_db.Stage_timeseries_error
    return TimeSeriesError

def get_pipeline_state_db():
    current_db = MongoClient().Stage_database
    PipelineState = current_db.Stage_pipeline_state
    return PipelineState

def get_place_db():
    current_db = MongoClient().Stage_database
    Places = current_db.Stage_place
    return Places

def get_trip_new_db():
    current_db = MongoClient().Stage_database
    Trips = current_db.Stage_trip_new
    return Trips

def get_stop_db():
    current_db = MongoClient().Stage_database
    Stops = current_db.Stage_stop
    return Stops

def get_section_new_db():
    current_db = MongoClient().Stage_database
    Sections = current_db.Stage_section_new
    return Sections

def get_fake_trips_db():
    current_db = MongoClient().Stage_database
    FakeTrips = current_db.Stage_fake_trips
    return FakeTrips

def get_fake_sections_db():
    current_db = MongoClient().Stage_database
    FakeSections = current_db.Stage_fake_sections
    return FakeSections
