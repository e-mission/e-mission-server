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
        import usercache.builtin_usercache
        return usercache.builtin_usercache.BuiltinUserCache(uuid)

    def __init__(self, uuid):
        self.uuid = uuid

    def putUserDataForPhone(self, key, value):
        """
        Store this key-value pair into the user results part of the cache.
        This will be automatically pushed to the phone at the appropriate time
        """
        pass

    def getUserDataFromPhone(self, key):
        """
        Retrieve user specified values from the cache.
        These have been automatically retrieved from the phone
        """
        pass

    def putBackgroundConfigForPhone(self, key, value):
        """
        Store the configuration for background operation into the phone.
        This will be automatically pushed to the phone at the appropriate time
        """
        pass

    def getBackgroundDataFromPhone(self, key):
        """
        Get the data collected as part of background processing on the phone.
        These have been automatically retrieved from the phone
        """
        pass

    # For information that we are retrieving from the phone, we can clear the
    # cache once we have processed it.
    # TODO: Figure out whether we need versioning
    # Also, TODO, figure out how we prevent a memory leak in the data that the
    # plugins push? How do we ensure that they clean up after themselves?
    # Some kind of auto-cleanup for both sides sounds like a good idea...

    def clearUserDataFromPhone(self, key_list):
        pass

    def clearBackgroundDataFromPhone(self, key_list):
        pass
