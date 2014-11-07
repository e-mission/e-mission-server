from pymongo import MongoClient


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

def get_routeDistanceMatrix_db():
    current_db=MongoClient().Stage_database
    routeDistanceMatrix=current_db.Stage_routeDistanceMatrix
    return routeDistanceMatrix

def get_client_db():
    current_db=MongoClient().Stage_database
    Clients = current_db.Stage_clients
    return Clients

def get_routeCluster_db():
    current_db=MongoClient().Stage_database
    routeCluster=current_db.Stage_routeCluster
    return routeCluster

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
