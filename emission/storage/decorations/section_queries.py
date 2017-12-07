from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pymongo

import emission.core.get_database as edb
import emission.core.wrapper.section as ecws
import emission.core.wrapper.entry as ecwe

import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta


def get_sections_for_trip(user_id, trip_id):
    curr_query = {"user_id": user_id, "data.trip_id": trip_id}
    return _get_sections_for_query(curr_query, "data.start_ts")

def get_sections_for_trip_list(user_id, trip_list):
    curr_query = {"user_id": user_id, "data.trip_id": {"$in": trip_list}}
    return _get_sections_for_query(curr_query, "data.start_ts")

def _get_sections_for_query(section_query, sort_field):
    section_query.update({"metadata.key": "segmentation/raw_section"})
    logging.debug("Returning sections for query %s" % section_query)
    section_doc_cursor = edb.get_analysis_timeseries_db().find(
        section_query).sort(sort_field, pymongo.ASCENDING)
    logging.debug("result cursor length = %d" % section_doc_cursor.count())
    return [ecwe.Entry(doc) for doc in section_doc_cursor]
