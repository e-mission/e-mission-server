from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# The server side implementation of the user cache

# This is an abstract superclass that can be implemented with couchbase, azure,
# or our builtin solution.

# Note also that this is the interface between the user cache and the rest of the code
# The REST interface between the rest of the code and this cache is in
# CFC_WebApp, consistent with the rest of our philosophy 

# If a different sync mechanism such as couchbase or azure is being used, then
# the communication between the phone and the server will happen through their
# API, not our REST API. Separating the REST part from the integration with the
# rest of the code (ha!) allows us to separate those concerns as well

# Let's switch to new-style classes finally!!
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
class UserCache(object):
    @staticmethod
    def getUserCache(uuid):
        """
            Returns the current usercache configured in the config file,
            or the builtin cache if nothing is configured
        """
        # We put the import statement in here to avoid circular dependencies
        # It is also not needed anywhere else since this is a completely static
        # function otherwise
        import emission.net.usercache.builtin_usercache as biuc
        return biuc.BuiltinUserCache(uuid)

    def __init__(self, user_id):
        self.user_id = user_id

    @staticmethod
    def get_uuid_list():
        import emission.net.usercache.builtin_usercache as biuc
        return biuc.BuiltinUserCache.get_uuid_list()

    def putDocument(self, key, value):
        """
        Store this key-value pair into the server -> phone part of the cache.
        This will be automatically pushed to the phone at the appropriate time.

        We don't need to specify the entry type since it is implied by the
        method name, and the server doesn't put read-write documents.
        """
        pass

    def getMessage(self, key, timeQuery = None):
        """
        Retrieve values from the phone->server part of the cache.
        These have been automatically retrieved from the phone.

        We don't need to specify the entry type since it is implied by the method name.
        We are, however, allowed to specify a time query (e.g. start_ts <
        read_ts <= end_ts). Note that the current interface only allows a
        single timeQuery to be specified, which is less rich than the current
        metadata format.

        However, we can easily extend this to a set of time queries if we need it.
        """
        pass

    def getMessageCount(self):
        pass

    # TODO: Should we have a separate clear method, or should we just clear on get?
    # I prefer a separate clear method, since it allows more flexibility in the
    # consumption and the plugin architecture
    def clearProcessedMessages(self, timeQuery, key_list = None):
        """
        Clear all messages that match the time query
        We could specify the same time query as specified for "get", allowing
        us to ensure that there are no race conditions.
        
        If we extended the timeQuery, we could also support 'read_ts' exists,
        which would allow us to delete message that have already been read
        """
        pass

    def getDocumentKeyList(self):
        """
        Get the list of keys for the "document" message types
        """
        pass

    def getSensorDataKeyList(self):
        """
        Get the list of keys for the "sensor-data" message types
        """
        pass

    def getMessageKeyList(self):
        """
        Get the list of keys for the "message" message types
        """
        pass

    def clearObsoleteDocument(self, key):
        pass
