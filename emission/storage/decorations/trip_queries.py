import logging

import emission.core.get_database as edb
import emission.core.wrapper.trip as ecwt

def create_new_trip(user_id):
    _id = edb.get_trip_new_db().save({"user_id": user_id})
    return ecwt.Trip({"_id": _id, "user_id": user_id})

def save_trip(trip):
    edb.get_trip_new_db().save(trip)
