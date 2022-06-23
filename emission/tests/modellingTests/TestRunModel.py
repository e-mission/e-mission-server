import unittest


import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.storage.timeseries.abstract_timeseries as esta


class TestSimilarityAux(unittest.TestCase):
    """these tests were copied forward during a refactor of the tour model
    [https://github.com/e-mission/e-mission-server/blob/10772f892385d44e11e51e796b0780d8f6609a2c/emission/analysis/modelling/tour_model_first_only/load_predict.py#L114]

    it's uncertain what condition they are in besides having been refactored to
    use the more recent tour modeling code.    
    """
    def setUp(self):
        self.all_users = esta.TimeSeries.get_uuid_list()
        if len(self.all_users) == 0:
            self.fail('test invariant failed: no users found')
    
    def testTrip1(self):

        # case 1: the new trip matches a bin from the 1st round and a cluster from the 2nd round
        user_id = self.all_users[0]
        eamur.update_trip_model(user_id, eamumt.ModelType.GREEDY_SIMILARITY_BINNING, eamums.ModelStorage.DATABASE)
        filter_trips = eamur._get_trips_for_user(user_id, None, 0)
        new_trip = filter_trips[4]
        # result is [{'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'church', 'replaced_mode': 'drove_alone'},
        # 'p': 0.9333333333333333}, {'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'entertainment',
        # 'replaced_mode': 'drove_alone'}, 'p': 0.06666666666666667}]
        pl, _ = eamur.predict_labels_with_n(new_trip)
        assert len(pl) > 0, f"Invalid prediction {pl}"

    def testTrip2(self):

        # case 2: no existing files for the user who has the new trip:
        # 1. the user is invalid(< 10 existing fully labeled trips, or < 50% of trips that fully labeled)
        # 2. the user doesn't have common trips
        user_id = self.all_users[1]
        eamur.update_trip_model(user_id, eamumt.ModelType.GREEDY_SIMILARITY_BINNING, eamums.ModelStorage.DATABASE)
        filter_trips = eamur._get_trips_for_user(user_id, None, 0)
        new_trip = filter_trips[0]
        # result is []
        pl, _ = eamur.predict_labels_with_n(new_trip)
        assert len(pl) == 0, f"Invalid prediction {pl}"

    def testTrip3(self):

        # case3: the new trip is novel trip(doesn't fall in any 1st round bins)
        user_id = self.all_users[0]
        eamur.update_trip_model(user_id, eamumt.ModelType.GREEDY_SIMILARITY_BINNING, eamums.ModelStorage.DATABASE)
        filter_trips = eamur._get_trips_for_user(user_id, None, 0)
        new_trip = filter_trips[0]
        # result is []
        pl = eamur.predict_labels_with_n(new_trip)
        assert len(pl) == 0, f"Invalid prediction {pl}"

    # case 4: the new trip falls in a 1st round bin, but predict to be a new cluster in the 2nd round
    # result is []
    # no example for now
