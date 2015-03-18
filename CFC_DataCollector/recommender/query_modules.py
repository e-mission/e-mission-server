""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

Note: the actual function for queries are located in queries

"""

modules = {
   'trips': {'get top trips': getTopTrips, 'get all trips': getAllTrips},
   'utility': {},
   'pertubation': {}
 }
