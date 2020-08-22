from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import arrow

import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe

def convert_wrapper_to_entry(key, wrapper):
    logging.debug("found user_id in wrapper %s" % wrapper["user_id"])
    wrapper_entry = ecwe.Entry.create_entry(wrapper["user_id"], key, wrapper)
    wrapper_entry["_id"] = wrapper["_id"]
    return wrapper_entry

def convert_collection(collection, key):
    result_cursor = collection.find()
    logging.info("About to convert %s entries" % collection.estimated_document_count())
    for i, wrapper in enumerate(result_cursor):
        entry = convert_wrapper_to_entry(key, wrapper)
        if entry.get_id() != wrapper["_id"]:
            logging.warn("entry.id = %s, wrapper.id = %s" % (entry.get_id(), wrapper["_id"]))
        if i % 10000 == 0:
            print("converted %s -> %s" % (wrapper, entry))
        edb.get_timeseries_db().insert(entry)
        # collection.remove(wrapper)

def move_ts_entries(key):
    tdb = edb.get_timeseries_db()
    atdb = edb.get_analysis_timeseries_db()

    result_cursor = tdb.find({'metadata.key': key})
    logging.info("About to convert %s entries" % tdb.count_documents({'metadata.key': key}))

    for i, entry_doc in enumerate(result_cursor):
      try:
        if i % 10000 == 0:
            print("moved %s from one ts to the other" % (entry_doc))
        atdb.insert(entry_doc)
        # tdb.remove(entry_doc)
      except:
        logging.info("Got error while moving %s, skipping" % (entry_doc))

if __name__ == '__main__':
    # No arguments - muahahahaha. Just going to copy known fields over.
#    convert_collection(edb.get_trip_new_db(), "segmentation/raw_trip")
#    convert_collection(edb.get_place_db(), "segmentation/raw_place")
#    convert_collection(edb.get_section_new_db(), "segmentation/raw_section")
#    convert_collection(edb.get_stop_db(), "segmentation/raw_stop")
    move_ts_entries("segmentation/raw_trip")
    move_ts_entries("segmentation/raw_place")
    move_ts_entries("segmentation/raw_section")
    move_ts_entries("segmentation/raw_stop")
    move_ts_entries("analysis/smoothing")
