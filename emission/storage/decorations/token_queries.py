from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Utility class to return useful user queries
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.core.get_database as edb
import emission.storage.timeseries.aggregate_timeseries as estag
from uuid import UUID

def get_all_tokens():
    tokens_list = [e for e in edb.get_token_db().find()]
    return tokens_list

def insert(entry):
    edb.get_token_db().insert_one(entry)

