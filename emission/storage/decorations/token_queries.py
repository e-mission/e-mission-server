import logging
import json

import emission.core.get_database as edb
import emission.storage.timeseries.aggregate_timeseries as estag
from uuid import UUID

def get_all_tokens():
    tokens_list = [e["token"] for e in edb.get_token_db().find()]
    return tokens_list

def insert(entry):
    edb.get_token_db().insert_one(entry)
