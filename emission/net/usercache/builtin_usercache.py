from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import time
import pymongo

# Our imports
import emission.net.usercache.abstract_usercache as ucauc # ucauc = usercache.abstract_usercache
from emission.core.get_database import get_usercache_db

"""
Format of the usercache_db.
Note that this assumes that we have a single user cache object per user.
We could also structure this to have multiple user cache objects per user -
maybe one for the data from the phone and one for the data to the phone.
We are going to go with the single object for now, since it is the easiest
option, but we can restructure it in this class if we want to.

The logical structure is shown in 
https://github.com/e-mission/e-mission-data-collection/wiki/User-cache-data-format-design-considerations
and the physical structure is shown in
https://github.com/e-mission/e-mission-data-collection/wiki/User-cache-data-format-design-considerations

    {
      "metadata": {
        "write_ts": 1435856137,
        "read_ts": 1435856138,
        "type": "document",
        "key": "data/carbon_footprint",
        "plugin": "data"
      },
      "data" : {
        "mine": 45.64,
        "avg": 21.35,
        "optimal": 44.21
      }
    },
    {
      "metadata": {
        "write_ts": 1435856237,
        "read_ts": 1435856238,
        "type": "message",
        "key": "background/location",
        // processed_ts is not yet set because it hasn't yet been processed
      },
      "data" : {
        "mLat": 45.64,
        "mLng": 21.35,
        "time": 1435856237,
      }
    }
"""

class BuiltinUserCache(ucauc.UserCache):
    def __init__(self, user_id):
        super(BuiltinUserCache, self).__init__(user_id)
        self.key_query = lambda key: {"metadata.key": key};
        self.ts_query = lambda tq: BuiltinUserCache._get_ts_query(tq)
        self.type_query = lambda entry_type: {"metadata.type": entry_type}
        self.db = get_usercache_db()

    @staticmethod
    def _get_ts_query(tq):
        return tq.get_query()

    @staticmethod
    def get_uuid_list():
        return get_usercache_db().distinct("user_id")

    def putDocument(self, key, value):
        """
            server -> phone
            Note that this assumes that we have a single cache document per user.
        """
        metadataDoc = {
                        'write_ts': time.time(),
                        'type': 'document',
                        'key': key,
                      }
        # If the field does not exist, $set will add a new field with the
        # specified value, provided that the new field does not violate a type
        # constraint.
        #
        # TODO: Should we store the user_id in the metadata doc, or outside?
        # If inside, we need to 
        document = {
                      '$set': {
                          'user_id': self.user_id,
                          'metadata': metadataDoc,
                          'data': value
                      }
                   }

        queryDoc = {'user_id': self.user_id,
                    'metadata.type': 'document',
                    'metadata.key': key}
        # logging.debug("Updating %s spec to %s" % (self.user_id, document))
        result = self.db.update_one(queryDoc,
                                document,
                                upsert=True)
        logging.debug("Result = %s after updating document %s" % (result, key))

    def _get_msg_query(self, key_list = None, time_query = None):
        ret_query = {"user_id": self.user_id}
        ret_query.update({"$or": [self.type_query("message"),
                                  self.type_query("sensor-data"),
                                  self.type_query("rw-document")]})
        if key_list is not None and len(key_list) > 0:
            key_query_list = []
            for key in key_list:
                key_query_list.append(self.key_query(key))
            ret_query.update({"$or": key_query_list})
        if (time_query is not None):
            ret_query.update(time_query.get_query())
        return ret_query

    def getMessage(self, key_list = None, timeQuery = None):
        """
            phone -> server
            Returns None if the key does not exist
        """
        read_ts = time.time()
        combo_query = self._get_msg_query(key_list, timeQuery)
        
        # In the handler, we assume that the messages are processed in order of
        # the write timestamp, because we use the last_ts_processed to mark the
        # beginning of the entry for the next query. So let's sort by the
        # write_ts before returning.
        retrievedMsgs = list(self.db.find(combo_query).sort("metadata.write_ts", pymongo.ASCENDING).limit(100000))
        logging.debug("Found %d messages in response to query %s" % (len(retrievedMsgs), combo_query))
        return retrievedMsgs

    def getMessageCount(self):
        """
            phone -> server
            Returns None if the key does not exist
        """
        read_ts = time.time()
        combo_query = self._get_msg_query(None, None)
        count = self.db.count_documents(combo_query)
        logging.debug("For %s, found %s messages in usercache" %
                      (self.user_id, count))
        return count

    def clearProcessedMessages(self, timeQuery, key_list=None):
        del_query = self._get_msg_query(key_list, timeQuery)
        logging.debug("About to delete messages matching query %s" % del_query)
        del_result = self.db.delete_many(del_query)
        logging.debug("Delete result = %s" % del_result)

    def getDocumentKeyList(self):
        return self.getKeyListForType("document")

    def getSensorDataKeyList(self):
        return self.getKeyListForType("sensor-data")

    def getMessageKeyList(self):
        return self.getKeyListForType("message")

    def getKeyListForType(self, message_type):
        return self.db.find({"user_id": self.user_id, "metadata.type": message_type}).distinct("metadata.key")

    def clearObsoleteDocument(self, key):
        """
        Just because we put a document into the cache once doesn't mean that it
        needs to always be there. We can delete obsolete documents - e.g. old
        entries in the cache. Currently, we delete entries by key - this
        ensures that values are not pushed over and over again.
        """
        queryDoc = {'user_id': self.user_id,
                    'metadata.type': 'document',
                    'metadata.key': key}
        
        result = self.db.delete_many(queryDoc)
        logging.debug("Result of removing document with key %s is %s" % (key, result))
