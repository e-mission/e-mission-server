# Removes all materialized views and the pipeline state.
# This will cause us to reprocess the pipeline from scratch
# As history begins to accumulate, we may want to specify a point to reset the
# pipeline to instead of deleting everything
import logging
logging.basicConfig(level=logging.DEBUG)

import emission.core.get_database as edb

if __name__ == '__main__':
    print "Deleting all trips"
    print edb.get_trip_new_db().remove()
    print "Deleting all sections"
    print edb.get_section_new_db().remove()
    print "Deleting pipeline state"
    print edb.get_pipeline_state_db().remove()
