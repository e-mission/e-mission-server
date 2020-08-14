from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Fixes usercache processing
# If there are any errors in the usercache processing, fix them and reload the data
# Basic flow
# - Copy data back to user cache
# - Attempt to moveToLongTerm
# - Find errors
# - Fix errors
# - Repeat until no errors are found
from future import standard_library
standard_library.install_aliases()
from builtins import *
import sys
import logging
logging.basicConfig(level=logging.DEBUG)

import uuid
import datetime as pydt
import json
import bson.json_util as bju

import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache_handler as euah
import emission.net.usercache.abstract_usercache as enua

def fix_usercache_errors():
    copy_to_usercache()
    move_to_long_term()
    
def copy_to_usercache():       
    # Step 1: Copy data back to user cache
    error_it = edb.get_timeseries_error_db().find()
    uc = edb.get_usercache_db()
    te = edb.get_timeseries_error_db()
    logging.info("Found %d errors in this round" % edb.get_timeseries_error_db.estimate_document_count())
    for error in error_it:
        logging.debug("Copying entry %s" % error["metadata"])
        save_result = uc.save(error)
        remove_result = te.remove(error["_id"])    
        logging.debug("save_result = %s, remove_result = %s" % (save_result, remove_result))
    logging.info("step copy_to_usercache DONE")
    
def move_to_long_term():
    cache_uuid_list = enua.UserCache.get_uuid_list()
    logging.info("cache UUID list = %s" % cache_uuid_list)

    for uuid in cache_uuid_list:
        logging.info("*" * 10 + "UUID %s: moving to long term" % uuid + "*" * 10)
        uh = euah.UserCacheHandler.getUserCacheHandler(uuid)
        uh.moveToLongTerm()


if __name__ == '__main__':
    fix_usercache_errors()
