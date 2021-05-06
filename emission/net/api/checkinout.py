import logging
import arrow
from uuid import UUID

import emission.core.get_database as edb

def checkin(cop):
    edb.get_checkinout_db().delete_one(cop)

def checkout(cop):
    cop["ts"] = arrow.get().timestamp
    edb.get_checkinout_db().insert_one(cop)

def checkedoutlist():
    all_list = list(edb.get_checkinout_db().find())
    for co in all_list:
        del co["user_id"]
        del co["_id"]
    return all_list

def checkedoutget(user_id):
    return edb.get_checkinout_db().find_one({"user_id": user_id})
