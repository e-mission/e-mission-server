import logging
import json

import emission.core.get_database as edb
import emission.storage.timeseries.aggregate_timeseries as estag

def get_all_tokens():
    tokens_list = [e["token"] for e in edb.get_token_db().find()]
    return tokens_list

def insert(entry):
    edb.get_token_db().insert_one(entry)

def insert_many_entries(entry_list):
    edb.get_token_db().insert_many(entry_list)

def insert_many_tokens(token_list):
    entry_list = [{"token":t} for t in token_list]
    insert_many_entries(entry_list)

def get_tokens_from_file(file):
    with open(file) as tlf:
        raw_token_list = tlf.readlines()
    token_list = [t.strip() for t in raw_token_list]
    return(token_list)