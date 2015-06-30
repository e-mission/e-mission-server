# This is the interface that syncs the usercache to the phone using the REST API.
# Since other services that we are planning to support (like couchdb or Azure) don't 
# need to use the REST services on our server to sync, this is not in the generic interface

from get_database import get_usercache_db
import logging

def sync_server_to_phone(uuid):
    """
        Gets the blob to sync to send to the phone and sends it over
        Return None if there is no data
    """
    retrievedData = get_usercache_db().find_one(uuid)
    logging.debug("retrievedData = %s" % retrievedData)
    if "server_to_phone" not in retrievedData:
        # Handle the case in which there is no "server_to_phone" field
        # probably because the data has just been created
        return None
    else:
        return retrievedData["server_to_phone"]

def sync_phone_to_server(uuid, data_from_phone):
    """
        Puts the blob from the phone into the cache
    """
    document = {
                  '$set': {
                      '_id': uuid,
                      'user_id': uuid,
                      'phone_to_server': data_from_phone
                  }
               }
    result = self.db.update({'user_id': uuid},
                             document,
                             upsert=True)
    logging.debug("Updated result = %s" % result)
    if 'err' in result and result['err'] is not None:
        logging.error("In sync_phone_to_server, err = %s" % result['err'])
        raise Exception()
