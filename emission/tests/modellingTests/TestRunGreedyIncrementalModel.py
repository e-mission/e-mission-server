import unittest
import logging
import json
import numpy as np
import uuid
import time

import bson.json_util as bju

import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esdatq
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.core.wrapper.entry as ecwe
import emission.core.get_database as edb


class TestRunGreedyModel(unittest.TestCase):
    
    def setUp(self):
        """
        sets up the end-to-end run model test with Confirmedtrip data from a
        test set of Confirmedtrip entries
        """
        logging.basicConfig(
            format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)

        # emission/tests/data/real_examples/shankari_2016-06-20.expected_confirmed_trips
        self.user_id = uuid.UUID('aa9fdec9-2944-446c-8ee2-50d79b3044d3')
        self.ts = esta.TimeSeries.get_time_series(self.user_id)
        self.new_trips_per_invocation = 3
        self.model_type = eamumt.ModelType.GREEDY_SIMILARITY_BINNING
        self.model_storage = eamums.ModelStorage.DOCUMENT_DATABASE
        sim_threshold = 500  # meters
        self.greedy_model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": sim_threshold,
            "apply_cutoff": False,
            "incremental_evaluation": True
        }

        # test data can be saved between test invocations, check if data exists before generating
        self.initial_data = list(self.ts.find_entries([esdatq.CONFIRMED_TRIP_KEY]))  

        if len(self.initial_data) == 0:

            # first time running against this database instance:
            # 1. load trips from source file into database
            # 2. create an initial entry for the incremental binning model

            # load in existing trips
            input_file = 'emission/tests/data/real_examples/shankari_2016-06-20.expected_confirmed_trips'
            with open(input_file, 'r') as f:
                trips_json = json.loads(f.read(), object_hook=bju.object_hook)
                trips = [ecwe.Entry(r) for r in trips_json]
            logging.debug(f'loaded {len(trips)} trips from {input_file}')
            self.ts.bulk_insert(trips)

            # confirm write to database succeeded
            self.initial_data = list(self.ts.find_entries([esdatq.CONFIRMED_TRIP_KEY]))
            if len(self.initial_data) == 0:
                logging.debug(f'test setup failed while loading trips from file')
                self.fail()

            logging.debug('writing initial trip model')
            eamur.update_trip_model(
                user_id=self.user_id,
                model_type=self.model_type,
                model_storage=self.model_storage,
                min_trips=4,  # there are 4 similar and labelled trips in the file
                model_config=self.greedy_model_config
            )

        logging.debug(f'setup: found {len(self.initial_data)} trips in database')

        # determine which trips are similar, and find the
        # centroid of their origins and destinations to build
        # new similar trips from
        metric = eamso.OriginDestinationSimilarity()
        features = []
        
        for trip in self.initial_data:
            f = metric.extract_features(trip)
            features.append(f)
        
        # 2022-07-07 rjf: the Confirmedtrip dataset used here has 6 trips (initially)
        #   but only 5 are "similar" within 500 meters. here we dynamically dis-
        #   include trip 6. set up like this in case we have to switch datasets 
        #   in the future (as long as the outliers are not similar!)
        # 2022-07-11 rjf: ooh, let's remove the ones without labels too
        similar_matrix = [[metric.similar(t1, t2, sim_threshold)
                for t1 in features]
                for t2 in features]
        self.similar_trips = []
        self.similar_features = []
        for idx, f in enumerate(self.initial_data):
            has_labels = len(self.initial_data[idx]['data']['user_input']) > 0
            sim = [similar_matrix[idx][i] for i in range(len(features)) if i != idx]
            similar = any(sim)
            if has_labels and similar:
                self.similar_trips.append(self.initial_data[idx])
                self.similar_features.append(features[idx])
        
        # after running, how many trips should be stored together in a similar bin?
        self.initial_similar_trips = len(self.similar_trips)
        self.expected_trips = self.initial_similar_trips + self.new_trips_per_invocation
        logging.debug(f"end of test, expecting {self.expected_trips} trips")

        # find the centroid of the similar trip data
        src_x, src_y, dst_x, dst_y = np.mean(self.similar_features, axis=0)
        self.origin = [src_x, src_y]
        self.destination = [dst_x, dst_y]


    def tearDown(self):
        """
        clean up database entries related to this test
        """
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.user_id})

    def testIncrementalRun(self):
        """
        incremental trip models train from Confirmedtrip entries at most
        once. to test this behavior, a model is built based on a small
        Confirmedtrip dataset stored at a file location (See setUp, above).
        this happens once and is not repeated when the test is re-run,
        unless a new database instance is spun up. within the test method,
        an additional few mock trips are generated with a later timestamp.
        the training model should 1) only see the new trips, 2) have been
        trained on the expected number of trips at completion.
        """
        # create a new trip sampling from the centroid and the existing
        # set of user input data
        # timestamps for these rows cannot be within the last 5 seconds
        # based on invariant set in pipeline_queries.py by the
        # END_FUZZ_AVOID_LTE constant.
        label_data = etmm.extract_trip_labels(self.similar_trips)
        new_trips = etmm.generate_mock_trips(
            user_id=self.user_id,
            trips=self.new_trips_per_invocation,
            origin=self.origin,
            destination=self.destination,
            label_data=label_data,
            threshold=0.0001, # ~10m,
            start_ts=time.time() - 20,
            end_ts=time.time() - 10
        )

        self.ts.bulk_insert(new_trips)
        all_trips = list(self.ts.find_entries([esdatq.CONFIRMED_TRIP_KEY]))
        logging.debug(f'total of {len(all_trips)} now stored in database')

        # train the new model on the complete collection of trips
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=self.model_type,
            model_storage=self.model_storage,
            min_trips=self.initial_similar_trips,
            model_config=self.greedy_model_config
        )
        updated_model = eamur._load_stored_trip_model(
            self.user_id,
            model_type=self.model_type,
            model_storage=self.model_storage,
            model_config=self.greedy_model_config
        )

        # the 6th trip in the original dataset was an outlier and should form it's own cluster
        self.assertEqual(len(updated_model.bins), 2, 
            'there should be two bins, one with similar trips, one with an outlier')

        trips_in_bin = len(updated_model.bins['0']['features'])
        print(f'trips in bins: {[len(x["features"]) for x in updated_model.bins.values()]}')
        self.assertEqual(trips_in_bin, self.expected_trips,
            'expected number of trips stored in bin')

        self.assertEqual(len(updated_model.bins['1']['features']), 1,
            'the second bin should have exactly one entry (an outlier)')
        