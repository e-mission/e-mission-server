import usercache.abstract_usercache as ucauc # ucauc = usercache.abstract_usercache
from get_database import get_usercache_db
import logging

"""
Format of the usercache_db.
Note that this assumes that we have a single user cache object per user.
We could also structure this to have multiple user cache objects per user -
maybe one for the data from the phone and one for the data to the phone.
We are going to go with the single object for now, since it is the easiest
option, but we can restructure it in this class if we want to.

As described in 
https://github.com/e-mission/e-mission-data-collection/wiki/Data-format-design

    {
        '_id': uuid,
        'user_id': uuid,
        'phone_to_server': 
        {
            'user':
            {
            },
            'background':
            {
            }
        }
        'server_to_phone': 
        {
            'user':
            {
            },
            'background_config':
            {
            },
            'common':
            {
            }
        }
    }
"""

class BuiltinUserCache(ucauc.UserCache):
    def __init__(self, uuid):
        super(BuiltinUserCache, self).__init__(uuid)
        self.fq_user_data_to = lambda(key): "server_to_phone.user.%s" % key
        self.fq_user_data_from = lambda(key): "phone_to_server.user.%s" % key
        self.fq_background_to = lambda(key): "server_to_phone.background_config.%s" % key
        self.fq_background_from = lambda(key): "phone_to_server.background.%s" % key
        self.db = get_usercache_db()

    def putUserDataForPhone(self, key, value):
        """
            server -> phone
            Note that this assumes that we have a single cache document per user.
        """
        fq_key = self.fq_user_data_to(key)
        self._putIntoCache(fq_key, value)

    def getUserDataFromPhone(self, key):
        """
            phone -> server
            Returns None if the key does not exist
        """
        fq_key = self.fq_user_data_from(key)
        return self._getFromCache(fq_key)

    def putBackgroundConfigForPhone(self, key, value):
        """
            server -> phone
            Note that this assumes that we have a single cache document per user.
        """
        fq_key = self.fq_background_to(key)
        return self._putIntoCache(fq_key, value)

    def getBackgroundDataFromPhone(self, key):
        """
            phone -> server
            Returns None if the key does not exist
        """
        fq_key = self.fq_background_from(key)
        return self._getFromCache(fq_key)

    def _putIntoCache(self, fq_key, value):
        """
            Put the value with the specified fully qualified name into the cache.
            This is (currently) not intended to be used directly.
            Instead, it is intended to be used by the other top level methods in here
        """
        # If the field does not exist, $set will add a new field with the
        # specified value, provided that the new field does not violate a type
        # constraint. If you specify a dotted path for a non-existent field,
        # $set will create the embedded documents as needed to fulfill the
        # dotted path to the field.
        document = {
                      '$set': {
                          '_id': self.uuid,
                          'user_id': self.uuid,
                          fq_key: value
                      }
                   }
        logging.debug("Updating %s spec to %s" % (self.uuid, document))
        result = self.db.update({'user_id': self.uuid},
                                 document,
                                 upsert=True)
        logging.debug("Updated result = %s" % result)

    def _getFromCache(self, fq_key):
        """
            Get the value with the specified fully qualified name from the cache.
            This is (currently) not intended to be used directly.
            Instead, it is intended to be used by the other top level methods in here
            Returns None if the key does not exist
        """
        key_parts = fq_key.split(".")
        retrievedDoc = self.db.find_one(self.uuid, {'_id': False, fq_key: True})
        returnedDoc = retrievedDoc
        logging.debug("Returned doc = %s" % returnedDoc)
        for key in key_parts:
            logging.debug("Considering key %s" % key)
            if key in returnedDoc:
                returnedDoc = returnedDoc[key]
            else:
                returnedDoc = None
        return returnedDoc
            

    # TODO: This name may be a bit confusing.
    # This is the data that we got from the phone
    # but we are clearing it from the cache, not from the phone (where it would
    # have already been cleared)
    def clearUserDataFromPhone(self, key_list):
        for key in key_list:
            self._clearFromCache(self.fq_user_data_from(key))

    def clearBackgroundDataFromPhone(self, key_list):
        for key in key_list:
            self._clearFromCache(self.fq_background_from(key))

    def _clearFromCache(self, fq_key):
        document = {"$unset": {fq_key: ""}}
        self.db.update({'user_id': self.uuid}, document)

