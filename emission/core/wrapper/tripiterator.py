from __future__ import print_function
from __future__ import absolute_import
# Standard imports
import os, sys
import logging
import traceback

# Our imports
from emission.core.get_database import get_section_db
from . import filter_modules
from . import trip_old as trip

class TripIterator(object):
    """
    Extracts Trip objects that can be passed
    into the Google Maps API section

    filter_queries are a tuple ["module_name", "function_name"]
    or ["module_name", "function_name", "options"] (see module.py)


    *** for example, if we wanted the top 5 trips, we would pass in the query:
        ["trips", "get top trips", 5]

    *** note: some functions do not allow/have options
    """
    def __init__(self, user_uuid, filter_queries, trip_class=trip.E_Mission_Trip):
        # Query section_db with queryList and
        # instantiate the cursor object
        # returned as an instance variable
        assert (len(filter_queries) >= 2), "filter_queries too short"
        self.trip_class = trip_class

        mod, query = filter_queries[0], filter_queries[1]
        query_function = filter_modules.modules.get(mod).get(query)
        try:
            if len(filter_queries) == 3:
                # options
                option = filter_queries[2]
                self.storedIter = query_function(user_uuid, option)
            else:
                # no options
                # print "Query function: ", query_function
                self.storedIter = query_function(user_uuid)
            # print "storedIter = %s" % self.storedIter
        except TypeError as e:
            print(e)
            print("something went wrong, here is some info:")
            traceback.print_exc()
            #logging.warn("Found no query function for filter_queries: ", filter_queries);
            self.storedIter = []
	'''
        clientSpecificQuery = getClientSpecificQueryFilter(user_uuid)
        completeQuery = clientSpecificQuery + queryList
        Sections = get_section_db()
        self.cur = Sections.find({"and": completeQuery})
	'''
    def __iter__(self):
        return self

    def next(self):
        trip = next(self.storedIter)
        return self.trip_class.trip_from_json(trip) if trip else []

    def close(self):
        logging.debug("Closing query for class %s" % self.storedIter)
        try:
           self.storedIter.close()
        except AttributeError:
           logging.debug("Non cursor iterator, skipping close")
