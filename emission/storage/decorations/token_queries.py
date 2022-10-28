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

def insert_from_json(jsonfile, userid=0):
    key_file = open(jsonfile)
    key_data = json.load(key_file)
    key_file.close()
    token_list_file = key_data["token_list"]
    with open(token_list_file) as tlf:
        raw_token_list = tlf.readlines()
    token_list = [t.strip() for t in raw_token_list]
    # raw_token_list = None

    for e in token_list:
        ent = {"user_id":userid, "token" : e}
        insert(ent)

