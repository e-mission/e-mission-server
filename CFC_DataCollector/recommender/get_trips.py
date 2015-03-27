import os, sys
import logging
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
from main.userclient import getClientSpecificQueryFilter
from main.get_database import get_section_db
import query_modules

class TripIterator:
    """
    Extracts Trip objects that can be passed
    into the Google Maps API section
    """

    """ filter_queries are a tuple ["module_name", "function_name"]
    or ["module_name", "function_name", "options"] (see module.py)


    *** for example, if we wanted the top 5 trips, we would pass in the query:
        ["trips", "get top trips", 5]

    *** note: some functions do not allow/have options

    """
    def __init__(self, user_uuid, filter_queries):
        # Query section_db with queryList and
        # instantiate the cursor object
        # returned as an instance variable
        assert (len(filter_queries) >= 2), "filter_queries too short"

        mod, query = filter_queries[0], filter_queries[1]
        query_function = query_modules.modules.get(mod).get(query)

        try:
            if len(filter_queries) == 3:
                # options
                option = filter_queries[2]
                self.storedIter = query_function(user_uuid, option)
            else:
                pass
                # no options
                self.storedIter = query_function(user_uuid)
        except TypeError:
            logging.warn("Found no query function for filter_queries: ", filter_queries);
            self.storedIter = []

        """
        clientSpecificQuery = getClientSpecificQueryFilter(user_uuid)
        completeQuery = clientSpecificQuery + queryList
        Sections = get_section_db()
        self.cur = Sections.find({"and": completeQuery})
        """

    def __iter__(self):
        return self.storedIter.__iter__()

    def next(self):
        # Get next record from cursor and
        # cast it to a Trip object, or one
        # of the Trip subclasses.
        trip = next(self.storedIter)
        if trip is None:
            return None
        return Trip(trip)

    def hasNext(self):
        return len(self.storedIter) > 0
