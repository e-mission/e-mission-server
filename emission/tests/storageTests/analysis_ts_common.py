from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import uuid
import json

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.timequery as estt

import emission.core.get_database as edb


def createNewTripLike(utest, key, wrapper):
    new_trip = wrapper()
    new_trip.start_ts = 5
    new_trip.start_fmt_time = "5 secs"
    new_trip.end_ts = 6
    new_trip.end_fmt_time = "6 secs"
    new_trip_id = esta.TimeSeries.get_time_series(utest.testUserId).insert_data(
        utest.testUserId, key, new_trip)
    new_trip_entry = esta.TimeSeries.get_time_series(utest.testUserId).get_entry_from_id(
        key, new_trip_id)
    utest.assertIsNotNone(new_trip_entry.get_id())
    utest.assertEqual(new_trip_entry.user_id, utest.testUserId)
    return new_trip_entry

def createNewPlaceLike(utest, key, wrapper):
    new_place = wrapper()
    new_place.enter_ts = 5
    new_place.exit_ts = 6
    new_trip_id = esta.TimeSeries.get_time_series(utest.testUserId).insert_data(
        utest.testUserId, key, new_place)
    new_place_entry = esta.TimeSeries.get_time_series(utest.testUserId).get_entry_from_id(
        key, new_trip_id)
    utest.assertIsNotNone(new_place_entry.get_id())
    utest.assertEqual(new_place_entry.user_id, utest.testUserId)
    return new_place_entry


def saveTripLike(utest, key, wrapper):
    new_trip = createNewTripLike(utest, key, wrapper)
    utest.assertEqual(edb.get_analysis_timeseries_db().count_documents(
        {"metadata.key": key, "data.end_ts": 6}), 1)
    utest.assertEqual(edb.get_analysis_timeseries_db().find_one(
        {"metadata.key": key, "data.end_ts": 6})["_id"], new_trip.get_id())
    utest.assertEqual(edb.get_analysis_timeseries_db().find_one(
        {"metadata.key": key, "data.end_ts": 6})["user_id"], utest.testUserId)
    return new_trip

def savePlaceLike(utest, key, wrapper):
    new_place = createNewPlaceLike(utest, key, wrapper)
    utest.assertEqual(edb.get_analysis_timeseries_db().count_documents(
        {"metadata.key": key, "data.exit_ts": 6}), 1)
    utest.assertEqual(edb.get_analysis_timeseries_db().find_one(
        {"metadata.key": key, "data.exit_ts": 6})["_id"], new_place.get_id())
    utest.assertEqual(edb.get_analysis_timeseries_db().find_one(
        {"metadata.key": key, "data.exit_ts": 6})["user_id"], utest.testUserId)
    return new_place

def queryTripLike(utest, key, wrapper):
    new_trip = createNewTripLike(utest, key, wrapper)
    ret_arr_time = esda.get_objects(key, utest.testUserId,
                                    estt.TimeQuery("data.start_ts", 4, 6))
    utest.assertEqual(ret_arr_time, [new_trip.data])

def queryPlaceLike(utest, key, wrapper):
    new_trip = createNewPlaceLike(utest, key, wrapper)
    ret_arr_time = esda.get_objects(key, utest.testUserId,
                                    estt.TimeQuery("data.enter_ts", 4, 6))
    utest.assertEqual(ret_arr_time, [new_trip.data])

def getObject(utest, key, wrapper):
    if key == esda.RAW_TRIP_KEY or key == esda.RAW_SECTION_KEY:
        new_obj = createNewTripLike(utest, key, wrapper)
    else:
        new_obj = createNewPlaceLike(utest, key, wrapper)
    ret_obj = esda.get_object(key, new_obj.get_id())
    utest.assertEqual(ret_obj, new_obj.data)
    utest.assertEqual(ret_obj, new_obj.data)

