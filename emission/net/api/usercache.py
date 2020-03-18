from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# This is the interface that syncs the usercache to the phone using the REST API.
# Since other services that we are planning to support (like couchdb or Azure) don't 
# need to use the REST services on our server to sync, this is not in the generic interface

# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from past.utils import old_div
import logging
import pymongo

# Our imports
from emission.core.get_database import get_usercache_db
import emission.core.common as ecc

def sync_server_to_phone(uuid):
    """
        Gets the blob to sync to send to the phone and sends it over
        Return None if there is no data
    """
    retrievedData = list(get_usercache_db().find({"user_id": uuid, "metadata.type": "document"}, # query
                                            {'_id': False, 'user_id': False}).sort("metadata.write_ts", pymongo.ASCENDING)) # projection, sort
    
    # logging.debug("retrievedData = %s" % retrievedData)
    return retrievedData

def sync_phone_to_server(uuid, data_from_phone):
    """
        Puts the blob from the phone into the cache
    """
    usercache_db = get_usercache_db()

    for data in data_from_phone:
        # logging.debug("About to insert %s into the database" % data)
        data.update({"user_id": uuid})
        # Hack to deal with milliseconds until we have moved everything over
        if ecc.isMillisecs(data["metadata"]["write_ts"]):
            data["metadata"]["write_ts"] = old_div(float(data["metadata"]["write_ts"]), 1000)

        if "ts" in data["data"] and ecc.isMillisecs(data["data"]["ts"]):
            data["data"]["ts"] = old_div(float(data["data"]["ts"]), 1000)
            
        # logging.debug("After updating with UUId, we get %s" % data)
        document = {'$set': data}
        update_query = {'user_id': uuid,
                        'metadata.type': data["metadata"]["type"],
                        'metadata.write_ts': data["metadata"]["write_ts"],
                        'metadata.key': data["metadata"]["key"]}
        result = usercache_db.update_one(update_query,
                                           document,
                                           upsert=True)
        logging.debug("Updated result for user = %s, key = %s, write_ts = %s = %s" % 
            (uuid, data["metadata"]["key"], data["metadata"]["write_ts"], result.raw_result))

        # I am not sure how to trigger a writer error to test this
        # and whether this is the format expected from the server in the rawResult
        if 'ok' in result.raw_result and result.raw_result['ok'] != 1.0:
            logging.error("In sync_phone_to_server, err = %s" % result.raw_result['writeError'])
            raise Exception()
