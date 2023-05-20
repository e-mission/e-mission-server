from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Exports all data for the particular user for the particular day
# Used for debugging issues with trip and section generation 
from future import standard_library
standard_library.install_aliases()
from builtins import *
import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import uuid
import datetime as pydt
import json
import emission.storage.json_wrappers as esj

import emission.net.api.usercache as enau

def save_server_to_phone(user_id_str, file_name):
    logging.info("Saving current server data for user %s to file %s" %
                 (user_id_str, file_name))

    # TODO: Convert to call to get_timeseries once we get that working
    # Or should we even do that?
    retVal = enau.sync_server_to_phone(uuid.UUID(user_id_str))
    json.dump(retVal, open(file_name, "w"), default=esj.wrapped_default, allow_nan=False, indent=4)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: %s <user> <file>" % (sys.argv[0]))
    else:
        save_server_to_phone(user_id_str=sys.argv[1], file_name=sys.argv[2])
