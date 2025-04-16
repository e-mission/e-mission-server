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
import emission.analysis.result.user_stat as earus

def sync_server_to_phone(uuid):
    """
        Gets the blob to sync to send to the phone and sends it over
        Return None if there is no data
    """
    retrievedData = list(get_usercache_db().find({"user_id": uuid, "metadata.type": "document"}, # query
                                            {'_id': False, 'user_id': False}).sort("metadata.write_ts", pymongo.ASCENDING)) # projection, sort
    
    # logging.debug("retrievedData = %s" % retrievedData)
    return retrievedData

def _remove_dots(entry_doc):
    keys_to_munge = []
    for key in entry_doc:
        # print(f"Checking {key=}")
        if isinstance(entry_doc[key], dict):
            # print(f"Found dict for {key=}, recursing")
            _remove_dots(entry_doc[key])
        if '.' in key:
            logging.info(f"Found {key=} with dot, adding to {keys_to_munge=}")
            keys_to_munge.append(key)

    # Returning early avoids further processing, if there is nothing to do
    # this will also make the logs much more streamlined
    if len(keys_to_munge) == 0:
        return

    logging.info(f"Before modifying, {keys_to_munge=}")

    for ktm in keys_to_munge:
        munged_key = ktm.replace(".", "_")
        # Get and delete in one swoop
        # https://stackoverflow.com/a/11277439
        logging.info(f"Replacing original dotted key {ktm} with {munged_key=}")
        entry_doc[munged_key] = entry_doc.pop(ktm, None)

    logging.info(f"(After modifying, {entry_doc.keys()=}")

def sync_phone_to_server(uuid, data_from_phone):
    """
        Puts the blob from the phone into the cache
    """
    usercache_db = get_usercache_db()

    last_location_entry = {"data": {"ts": -1}}
    for data in data_from_phone:
        # logging.debug("About to insert %s into the database" % data)
        data.update({"user_id": uuid})
        # Hack to deal with milliseconds until we have moved everything over
        if ecc.isMillisecs(data["metadata"]["write_ts"]):
            data["metadata"]["write_ts"] = old_div(float(data["metadata"]["write_ts"]), 1000)

        if "ts" in data["data"] and ecc.isMillisecs(data["data"]["ts"]):
            data["data"]["ts"] = old_div(float(data["data"]["ts"]), 1000)

        # mongodb/documentDB don't support field names with `.`
        # let's convert them all to `_`
        # this should only happen for stats entries since we control the
        # structure for all other objects and can ensure that they don't have dots
        if "stats" in data["metadata"]["key"]:
            _remove_dots(data)
            
        # logging.debug("After updating with UUId, we get %s" % data)
        document = {'$set': data}
        update_query = {'user_id': uuid,
                        'metadata.type': data["metadata"]["type"],
                        'metadata.write_ts': data["metadata"]["write_ts"],
                        'metadata.key': data["metadata"]["key"]}
        try:
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

            if data["metadata"]["key"] == "background/location":
                last_location_entry = data

        except pymongo.errors.PyMongoError as e:
            logging.error(f"In sync_phone_to_server, while executing {update_query=} on {document=}")
            logging.exception(e)
            raise

    earus.update_upload_timestamp(uuid, "last_location_ts", last_location_entry["data"].get("ts", -1))
    earus.update_upload_timestamp(uuid, "last_phone_data_ts", data["metadata"]["write_ts"])
