from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import unittest
import datetime as pydt
import logging
import pymongo
import json
import emission.storage.json_wrappers as esj
import bson.objectid as boi
import numpy as np
import attrdict as ad
import pandas as pd

# Our imports
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.section as ecws
import emission.core.wrapper.entry as ecwe

import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.cleaning.cleaning_methods.speed_outlier_detection as eaics
import emission.analysis.intake.cleaning.cleaning_methods.jump_smoothing as eaicj

import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.tests.common as etc

class TestLocationSmoothing(unittest.TestCase):
    def setUp(self):
        # We need to access the database directly sometimes in order to
        # forcibly insert entries for the tests to pass. But we put the import
        # in here to reduce the temptation to use the database directly elsewhere.
        import emission.core.get_database as edb
        import uuid

        self.testUUID = uuid.uuid4()
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        with open("emission/tests/data/smoothing_data/trip_list.txt") as tfp:
            self.trip_entries = json.load(tfp,
                                      object_hook=esj.wrapped_object_hook)
        for trip_entry in self.trip_entries:
            trip_entry["user_id"] = self.testUUID
            self.ts.insert(trip_entry)

        self.trip_entries = [ecwe.Entry(t) for t in self.trip_entries]

        with open("emission/tests/data/smoothing_data/section_list.txt") as sfp:
            self.section_entries = json.load(sfp,
                                         object_hook=esj.wrapped_object_hook)
        for section_entry in self.section_entries:
            section_entry["user_id"] = self.testUUID
            self.ts.insert(section_entry)

        self.section_entries = [ecwe.Entry(s) for s in self.section_entries]

    def tearDown(self):
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})

    def loadPointsForTrip(self, trip_id):
        import emission.core.get_database as edb

        with open("emission/tests/data/smoothing_data/%s" % trip_id) as pfp:
            entries = json.load(pfp,
                                 object_hook=esj.wrapped_object_hook)
        tsdb = edb.get_timeseries_db()
        for entry in entries:
            entry["user_id"] = self.testUUID
            edb.save(tsdb, entry)

    def testPointFilteringShanghaiJump(self):
        classicJumpTrip1 = self.trip_entries[0]
        self.loadPointsForTrip(classicJumpTrip1.get_id())
        classicJumpSections1 = [s for s in self.section_entries
                                if s.data.trip_id == classicJumpTrip1.get_id()]
        outlier_algo = eaics.BoxplotOutlier()
        jump_algo = eaicj.SmoothZigzag(False, 100)

        for i, section_entry in enumerate(classicJumpSections1):
            logging.debug("-" * 20 + "Considering section %s" % i + "-" * 20)

            section_df = self.ts.get_data_df("background/filtered_location",
                            esda.get_time_query_for_trip_like(esda.RAW_SECTION_KEY,
                                                              section_entry.get_id()))
            with_speeds_df = eaicl.add_dist_heading_speed(section_df)

            maxSpeed = outlier_algo.get_threshold(with_speeds_df)
            logging.debug("Max speed for section %s = %s" % (i, maxSpeed))

            jump_algo.filter(with_speeds_df)
            logging.debug("Retaining points %s" % np.nonzero(jump_algo.inlier_mask_))

            to_delete_mask = np.logical_not(jump_algo.inlier_mask_)
            logging.debug("Deleting points %s" % np.nonzero(to_delete_mask))

            delete_ids = list(with_speeds_df[to_delete_mask]._id)
            logging.debug("Deleting ids %s" % delete_ids)

            # Automated checks. Might be able to remove logging statements later
            if i != 2:
                # Not the bad section. Should not be filtered
                self.assertEqual(np.count_nonzero(to_delete_mask), 0)
                self.assertEqual(len(delete_ids), 0)
            else:
                # The bad section, should have the third point filtered
                self.assertEqual(np.count_nonzero(to_delete_mask), 1)
                self.assertEqual([str(id) for id in delete_ids], ["55d8c4837d65cb39ee983cb4"])

    def testPointFilteringRichmondJump(self):
        classicJumpTrip1 = self.trip_entries[6]
        self.loadPointsForTrip(classicJumpTrip1.get_id())
        classicJumpSections1 = [s for s in self.section_entries
                                if s.data.trip_id == classicJumpTrip1.get_id()]
        outlier_algo = eaics.BoxplotOutlier()
        jump_algo = eaicj.SmoothZigzag(False, 100)

        for i, section_entry in enumerate(classicJumpSections1):
            logging.debug("-" * 20 + "Considering section %s" % i + "-" * 20)

            section_df = self.ts.get_data_df("background/filtered_location",
                            esda.get_time_query_for_trip_like(esda.RAW_SECTION_KEY,
                                                              section_entry.get_id()))
            with_speeds_df = eaicl.add_dist_heading_speed(section_df)

            maxSpeed = outlier_algo.get_threshold(with_speeds_df)
            logging.debug("Max speed for section %s = %s" % (i, maxSpeed))

            jump_algo.filter(with_speeds_df)
            logging.debug("Retaining points %s" % np.nonzero(jump_algo.inlier_mask_))

            to_delete_mask = np.logical_not(jump_algo.inlier_mask_)
            logging.debug("Deleting points %s" % np.nonzero(to_delete_mask))

            delete_ids = list(with_speeds_df[to_delete_mask]._id)
            logging.debug("Deleting ids %s" % delete_ids)

            # There is only one section
            self.assertEqual(i, 0)
            # The bad section, should have the third point filtered
            self.assertEqual(np.count_nonzero(to_delete_mask), 1)
            self.assertEqual([str(id) for id in delete_ids], ["55e86dbb7d65cb39ee987e09"])

    # Tests for the special handling of big jumps in trips where all other points are in clusters
    # If there are multiple clusters, then the simple alternation of GOOD and BAD fails
    # and we need a more sophisticated check
    def testFilterAllClusters(self):
        import pandas as pd
        import itertools

        outlier_algo = eaics.BoxplotOutlier()
        jump_algo = eaicj.SmoothZigzag(False, 100)
        backup_algo = eaicj.SmoothPosdap(eaicl.MACH1)

        # basic check that if the backup algo is not specified, we return the original values
        with_speeds_df = pd.read_csv("emission/tests/data/smoothing_data/all_cluster_case_2.csv")
        with_speeds_df.drop(["distance", "speed", "heading"], axis="columns", inplace=True)
        with_speeds_df["loc"] = with_speeds_df["loc"].apply(lambda lstr: json.loads(lstr.replace("'",  '"')))
        (sel_algo, filtered_points) = eaicl.get_points_to_filter(with_speeds_df, outlier_algo, jump_algo, None)
        # original values, inserted in
        # https://github.com/e-mission/e-mission-server/pull/897/commits/434a9a19b7f41ae868102e0154df95db8ec633c4
        # removed in https://github.com/e-mission/e-mission-server/pull/897/commits/67f5c86206e41168c6f3664fa5a2d4152d9d4091
        expected_result_idx = list(itertools.chain([0], range(2,11), range(12, 14)))
        self.assertEqual(list(filtered_points.dropna().index), expected_result_idx)
        self.assertEqual(sel_algo, jump_algo)
        self.assertEqual(sel_algo.__class__.__name__.split(".")[-1], "SmoothZigzag")

        # US to ocean jump: case 1 of https://github.com/e-mission/e-mission-docs/issues/843
        with_speeds_df = pd.read_csv("emission/tests/data/smoothing_data/all_cluster_case_1.csv", index_col=0)
        with_speeds_df.drop(["distance", "speed", "heading"], axis="columns", inplace=True)
        with_speeds_df["loc"] = with_speeds_df["loc"].apply(lambda lstr: json.loads(lstr.replace("'",  '"')))
        (sel_algo, filtered_points) = eaicl.get_points_to_filter(with_speeds_df, outlier_algo, jump_algo, backup_algo)
        expected_result_idx = list(range(16, 21))
        self.assertEqual(list(filtered_points.dropna().index), expected_result_idx)
        self.assertEqual(sel_algo, backup_algo)
        self.assertEqual(sel_algo.__class__.__name__.split(".")[-1], "SmoothPosdap")
  
        # PR to pakistan jump: case 2 of https://github.com/e-mission/e-mission-docs/issues/843
        with_speeds_df = pd.read_csv("emission/tests/data/smoothing_data/all_cluster_case_2.csv")
        with_speeds_df.drop(["distance", "speed", "heading"], axis="columns", inplace=True)
        with_speeds_df["loc"] = with_speeds_df["loc"].apply(lambda lstr: json.loads(lstr.replace("'",  '"')))
        (sel_algo, filtered_points) = eaicl.get_points_to_filter(with_speeds_df, outlier_algo, jump_algo, backup_algo)
        expected_result_idx = [11]
        self.assertEqual(list(filtered_points.dropna().index), expected_result_idx)
        self.assertEqual(sel_algo, backup_algo)
        self.assertEqual(sel_algo.__class__.__name__.split(".")[-1], "SmoothPosdap")

    # I found some super weird behavior while fixing the issue with the early
    # return when there are no jumps
    # It looks like series -> numpy array -> non_zero returns a tuple so we can't get its length directly
    # instead, we return the size of the first element in the tuple
    def testWeirdNumpyBehavior(self):
        test = pd.Series([True] * 10)
        test = test.to_numpy()
        self.assertEqual(test.shape, (10,))
        self.assertEqual(np.logical_not(test).shape, (10,))
        np.testing.assert_equal(np.nonzero(np.logical_not(test)), (np.array([], dtype=np.int64),))
        self.assertEqual(np.nonzero(np.logical_not(test))[0].shape[0], 0)

    def testPointFilteringZigzag(self):
        classicJumpTrip1 = self.trip_entries[8]
        self.loadPointsForTrip(classicJumpTrip1.get_id())
        classicJumpSections1 = [s for s in self.section_entries
                                if s.data.trip_id == classicJumpTrip1.get_id()]
        outlier_algo = eaics.BoxplotOutlier()
        jump_algo = eaicj.SmoothZigzag(False, 100)

        for i, section_entry in enumerate(classicJumpSections1):
            logging.debug("-" * 20 + "Considering section %s" % i + "-" * 20)

            section_df = self.ts.get_data_df("background/filtered_location",
                            esda.get_time_query_for_trip_like(esda.RAW_SECTION_KEY,
                                                              section_entry.get_id()))
            with_speeds_df = eaicl.add_dist_heading_speed(section_df)

            maxSpeed = outlier_algo.get_threshold(with_speeds_df)
            logging.debug("Max speed for section %s = %s" % (i, maxSpeed))

            jump_algo.filter(with_speeds_df)
            logging.debug("Retaining points %s" % np.nonzero(jump_algo.inlier_mask_))

            to_delete_mask = np.logical_not(jump_algo.inlier_mask_)
            logging.debug("Deleting points %s" % np.nonzero(to_delete_mask))

            delete_ids = list(with_speeds_df[to_delete_mask]._id)
            logging.debug("Deleting ids %s" % delete_ids)

            if i == 0:
                # this is the zigzag section
                self.assertEqual(np.nonzero(to_delete_mask)[0].tolist(),
                                 [25, 64, 114, 115, 116, 117, 118, 119, 120, 123, 126])
                self.assertEqual(delete_ids,
                                 [boi.ObjectId('55edafe77d65cb39ee9882ff'),
                                  boi.ObjectId('55edcc157d65cb39ee98836e'),
                                  boi.ObjectId('55edcc1f7d65cb39ee988400'),
                                  boi.ObjectId('55edcc1f7d65cb39ee988403'),
                                  boi.ObjectId('55edcc1f7d65cb39ee988406'),
                                  boi.ObjectId('55edcc1f7d65cb39ee988409'),
                                  boi.ObjectId('55edcc1f7d65cb39ee98840c'),
                                  boi.ObjectId('55edcc207d65cb39ee988410'),
                                  boi.ObjectId('55edcc207d65cb39ee988412'),
                                  boi.ObjectId('55edcc217d65cb39ee98841f'),
                                  boi.ObjectId('55edcc217d65cb39ee988429')])
            else:
                self.assertEqual(len(np.nonzero(to_delete_mask)[0]), 0)
                self.assertEqual(len(delete_ids), 0)

    def testFilterSection(self):
        jump_trips = [self.trip_entries[0], self.trip_entries[6], self.trip_entries[8]]
        for i, trip in enumerate(jump_trips):
            self.loadPointsForTrip(trip.get_id())
            logging.debug("=" * 20 + "Considering trip %s: %s" %
                          (i, trip.data.start_fmt_time) + "=" * 20)
            curr_sections = [s for s in self.section_entries
                             if s.data.trip_id == trip.get_id()]
            # for j, section in enumerate(esdt.get_sections_for_trip(self.testUUID, trip.get_id())):
            for j, section_entry in enumerate(curr_sections):
                logging.debug("-" * 20 + "Considering section %s: %s" %
                              (j, section_entry.data.start_fmt_time) + "-" * 20)
                loc_df = self.ts.get_data_df("background/filtered_location",
                        esda.get_time_query_for_trip_like(esda.RAW_SECTION_KEY,
                                                          section_entry.get_id()))
                entry_to_store = eaicl.filter_jumps(self.testUUID, section_entry, loc_df)
                self.ts.insert(entry_to_store)
                # TODO: Figure out how to make collections work for the wrappers and then change this to an Entry
                filtered_points_entry = ad.AttrDict(self.ts.get_entry_at_ts(
                    "analysis/smoothing", "data.section", section_entry.get_id()))
                logging.debug("filtered_points_entry = %s" % filtered_points_entry)
                if i == 0 and j == 2:
                    # Shanghai jump
                    self.assertIsNotNone(filtered_points_entry)
                    self.assertEqual(len(filtered_points_entry.data.deleted_points), 1)
                    self.assertEqual(list(filtered_points_entry.data.deleted_points), [boi.ObjectId("55d8c4837d65cb39ee983cb4")])
                elif i == 1 and j == 0:
                    # Richmond jump
                    self.assertIsNotNone(filtered_points_entry)
                    self.assertEqual(len(filtered_points_entry.data.deleted_points), 1)
                    self.assertEqual(list(filtered_points_entry.data.deleted_points), [boi.ObjectId("55e86dbb7d65cb39ee987e09")])
                elif i == 2 and j == 0:
                    # SF zigzag
                    self.assertIsNotNone(filtered_points_entry)
                    # Note that this is slightly different from the prior check in testPointFilteringZigZag.
                    # It seems to be 100% reproducible, so it is not "random".
                    # It is also robust to reading the section from the database versus using the in-memory list
                    # I looked at the logs, and this is kind of weird. In both cases, the detected max speed is
                    # 76.2167353311. But in the prior check the 126-137 points are in the same cluster.
                    # And in this check, they are split (126-127, 127-130 and 130-137).
                    # Which is why this one detects point 130 while the other doesn't.
                    # I still don't know why this should happen. Maybe the speed is right at the cutoff?
                    self.assertEqual(len(filtered_points_entry.data.deleted_points), 12)
                    self.assertEqual(list(filtered_points_entry.data.deleted_points),
                                     [boi.ObjectId('55edafe77d65cb39ee9882ff'),
                                      boi.ObjectId('55edcc157d65cb39ee98836e'),
                                      boi.ObjectId('55edcc1f7d65cb39ee988400'),
                                      boi.ObjectId('55edcc1f7d65cb39ee988403'),
                                      boi.ObjectId('55edcc1f7d65cb39ee988406'),
                                      boi.ObjectId('55edcc1f7d65cb39ee988409'),
                                      boi.ObjectId('55edcc1f7d65cb39ee98840c'),
                                      boi.ObjectId('55edcc207d65cb39ee988410'),
                                      boi.ObjectId('55edcc207d65cb39ee988412'),
                                      boi.ObjectId('55edcc217d65cb39ee98841f'),
                                      boi.ObjectId('55edcc217d65cb39ee988429'),
                                      boi.ObjectId('55edcc227d65cb39ee988434')])
                else:
                    # not a zigzag, shouldn't find anything
                    self.assertIsNotNone(filtered_points_entry)
                    self.assertEqual(len(filtered_points_entry.data.deleted_points), 0)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()



