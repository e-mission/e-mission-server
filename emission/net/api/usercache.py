# This is the interface that syncs the usercache to the phone using the REST API.
# Since other services that we are planning to support (like couchdb or Azure) don't 
# need to use the REST services on our server to sync, this is not in the generic interface

# Standard imports
import logging

# Our imports
from emission.core.get_database import get_usercache_db

def sync_server_to_phone(uuid):
    """
        Gets the blob to sync to send to the phone and sends it over
        Return None if there is no data
    """
    retrievedData = list(get_usercache_db().find({"user_id": uuid, "metadata.type": "document"}, # query
                                            {'_id': False, 'user_id': False})) # projection
    
    # logging.debug("retrievedData = %s" % retrievedData)
    return retrievedData

def sync_phone_to_server(uuid, data_from_phone):
    """
        Puts the blob from the phone into the cache
    """
    for data in data_from_phone:
        logging.debug("About to insert %s into the database" % data)
        data.update({"user_id": uuid})
        logging.debug("After updating with UUId, we get %s" % data)
        document = {'$set': data}
        update_query = {'user_id': uuid,
                        'metadata.type': data["metadata"]["type"],
                        'metadata.write_ts': data["metadata"]["write_ts"],
                        'metadata.key': data["metadata"]["key"]}
        result = get_usercache_db().update(update_query,
                                           document,
                                           upsert=True)
        logging.debug("Updated result for key = %s, write_ts = %s = %s" % 
            (data["metadata"]["key"], data["metadata"]["write_ts"], result))
        if 'err' in result and result['err'] is not None:
            logging.error("In sync_phone_to_server, err = %s" % result['err'])
            raise Exception()
