import usercache.abstract_usercache as ucauc # ucauc = usercache.abstract_usercache
from get_database import get_usercache_db
import logging
import time

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
    def __init__(self, uuid):
        super(BuiltinUserCache, self).__init__(uuid)
        self.key_query = lambda(key): {"metadata.key": key};
        self.ts_query = lambda(tq): {"$and": [{"metadata.%s" % tq.timeType: {"$gte": tq.startTs}},
                {"metadata.%s" % tq.timeType: {"$lte": tq.endTs}}]}
        self.type_query = lambda(entry_type): {"metadata.type": entry_type}
        self.get_utc_ts = lambda(_): int(time.time())
        self.db = get_usercache_db()

    def putDocument(self, key, value):
        """
            server -> phone
            Note that this assumes that we have a single cache document per user.
        """
        metadataDoc = {
                        'write_ts': self.get_utc_ts("_"),
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
                          'user_id': self.uuid,
                          'metadata': metadataDoc,
                          'data': value
                      }
                   }

        queryDoc = {'user_id': self.uuid,
                    'metadata.type': 'document',
                    'metadata.key': key}
        logging.debug("Updating %s spec to %s" % (self.uuid, document))
        result = self.db.update(queryDoc,
                                document,
                                upsert=True)
        logging.debug("Result = %s after updating document" % result)

    def getMessage(self, key, timeQuery = None):
        """
            phone -> server
            Returns None if the key does not exist
        """
        read_ts = self.get_utc_ts("_")
        combo_query = {"user_id": self.uuid}
        combo_query.update({"$or": [self.type_query("message"), self.type_query("rw-document")]})
        combo_query.update(self.key_query(key))
        if (timeQuery is not None):
            combo_query.update(self.ts_query(timeQuery))
        
        # We first update the read timestamp and then actually read the messages
        # This ensures that the values that we return have the read_ts set
        # Is this important/useful? Dunno
        update_read = {
            '$set': {
                'metadata.read_ts': read_ts
            }
        }
        update_result = self.db.update(combo_query, update_read)
        logging.debug("result = %s after updating read timestamp", update_result)
        retrievedMsgs = list(self.db.find(combo_query))
        logging.debug("Found %d messages in response to query %s" % (len(retrievedMsgs), combo_query))
        return retrievedMsgs

    def clearProcessedMessages(self, timeQuery, key_list=None):
        del_query = self.ts_query(timeQuery)
        if key_list is not None:
            key_query_list = []
            for key in key_list:
                key_query_list.append(self.key_query(key))
        if key_query_list is not None:
            del_query.update({"$or": key_query_list})
        logging.debug("About to delete messages matching query %s" % del_query)
        del_result = self.db.remove(del_query)
        logging.debug("Delete result = %s" % del_result)
