
import logging
from pathlib import Path
from typing import Callable, List, Optional, Tuple
from emission.analysis.modelling.probabilistic_clustering_model.prediction import Prediction
from emission.analysis.modelling.probabilistic_clustering_model.probabilistic_clustering_model import ProbabilisticClusteringModel
from emission.analysis.modelling.similarity.similarity_metric import SimilarityMetric
from emission.analysis.modelling.tour_model.similarity import similarity
from emission.analysis.modelling.tour_model_first_only.load_predict import loadModelStage
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
from emission.core.wrapper.confirmedtrip import Confirmedtrip
import emission.analysis.modelling.probabilistic_clustering_model.util as util
import emission.analysis.modelling.similarity.confirmed_trip_feature_extraction as ctfe

RADIUS=500


class GreedySimilarityClustering(ProbabilisticClusteringModel):

    def __init__(self, dir: Path, user_id: str, metric: SimilarityMetric, sim_thresh: float, apply_cutoff: bool = True) -> None:
        """instantiate a clustering model for a user

        :param dir: the model load/save directory
        :type dir: Path
        :param user_id: identity (UUID) of the e-mission user
        :type user_id: str
        :param metric: type of similarity metric to use
        :type metric: SimilarityMetric
        :param sim_thresh: max distance threshold for similarity (assumed meters)
        :type sim_thresh: float
        :param apply_cutoff: ignore clusters which are small, based on a "knee point" heuristic (default True)
        :type apply_cutoff: bool
        """
        super().__init__()
        self.directory = dir
        self.user_id = user_id
        self.metric = metric
        self.sim_thresh = sim_thresh
        self.apply_cutoff = apply_cutoff
        self.trip_locations_by_bin = {}
        self.trip_labels_by_bin = {}
        self.loaded = False
        

    def load(self, user_id: str):
        self.trip_locations_by_bin = util.load_json_model_stage('locations_first_round_' + str(user_id))
        self.trip_labels_by_bin = util.load_json_model_stage('user_labels_first_round_' + str(user_id))
        self.loaded = True

    def save(self, user_id: str):
        """save this model to disk for the given user

        :param user_id: id for the user associated with this model
        :type user_id: str
        """

        # see load_predict.py save_models as called by build_user_model

        pass

    def fit(self, data: List[Confirmedtrip], labels: List[int]):
        """train the model by passing data, where each row in the data
        corresponds to a label at the matching index of the label input

        :param data: 2D array of features to train from
        :type data: List[Confirmedtrip]
        :param labels: vector of labels associated with the input data
        :type labels: List[int]
        """
        self.trip_labels_by_bin = {}
        self.trip_locations_by_bin = {}
        num_bins = 0
        
        # assign bins to all trips
        for trip in data:
            trip_location = self.metric.extract_features(trip)
            trip_labels = ctfe.label_features(trip)
            bin_id = self.find_bin(trip)
            if bin_id is not None:
                # add to existing bin
                self.trip_locations_by_bin[bin_id].append(trip_location)
                self.trip_labels_by_bin[bin_id].append(trip_labels)
            else:
                # create new bin
                new_bin_id = num_bins
                self.trip_locations_by_bin[new_bin_id] = [trip_location]
                self.trip_labels_by_bin[new_bin_id] = [trip_labels]
                num_bins += 1
        
        if len(self.trip_locations_by_bin) > 1 and self.apply_cutoff:
            # apply the cutoff heuristic to reduce over-fitting to small clusters
            bin_sizes = [len(b) for b in self.trip_locations_by_bin.values()]
            cutoff_idx, cutoff_value = util.find_knee_point(bin_sizes)
            logging.debug("bins = %s, elbow distance = %s" % (num_bins, cutoff_value))

        # bins = bins.sort(key=lambda bin: len(bin), reverse=True)

        # see similarity.py fit method
        # if self.cutoff:
        #     self.delete_bins()
        # self.labels_ = self.get_result_labels()

        # see build_save_model for creating the self.user_labels

        pass

    def predict(self, trip: Confirmedtrip) -> Tuple[List[Prediction], int]: 
        if not self.loaded:
            msg = (
                "predict called on unloaded clustering model " 
                f"for user {self.user_id}"
            )
            raise IOError(msg)

        logging.debug(f"running greedy similarity clustering")       
        selected_bin = self.find_bin(trip)
        if selected_bin is None:
            logging.debug(f"unable to predict bin for trip {trip}")
            return [], -1
        else:
            labels = self.trip_labels_by_bin[selected_bin]
            logging.debug(f"found cluster {selected_bin} with labels {labels}")
            return labels, len(self.trip_locations_by_bin[selected_bin])
        
    def find_bin(self, trip: Confirmedtrip) -> Optional[int]:
        """finds a bin which contains at least one matching feature

        :param trip: incoming trip features to test with
        :type trip: Confirmedtrip
        :return: either a bin number, or, None if no match was found
        :rtype: Optional[int]
        """
        trip_features = self.metric.extract_features(trip) 
        bin_ids = list(self.trip_locations_by_bin.keys())
        selected_bin = None
        for bin_id in bin_ids:
            this_bin = self.trip_locations_by_bin[bin_id]
            for binned_features in this_bin:
                if self.metric.similar(trip_features, binned_features, self.sim_thresh):
                    selected_bin = bin_id
                    break
        return selected_bin