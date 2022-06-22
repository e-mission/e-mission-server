import logging
from typing import Dict, List, Optional, Tuple

import emission.analysis.modelling.similarity.similarity_metric as eamss
import emission.analysis.modelling.tour_model.label_processing as lp
import emission.analysis.modelling.user_label_model.user_label_prediction_model as eamuu
import emission.analysis.modelling.user_label_model.util as util
import emission.core.wrapper.confirmedtrip as ecwc
import pandas as pd


class GreedySimilarityBinning(eamuu.UserLabelPredictionModel):

    def __init__(
        self,
        metric: eamss.SimilarityMetric,
        sim_thresh: float,
        apply_cutoff: bool = False,
    ) -> None:
        """
        instantiate a clustering model for a user.

        replaces the original similarity class
        [https://github.com/e-mission/e-mission-server/blob/5b9e608154de15e32df4f70a07a5b95477e7dbf5/emission/analysis/modelling/tour_model/similarity.py#L67]

        this technique employs a greedy similarity heuristic to associate
        trips with collections of probabilistic class labels. in pseudocode:
        
        # fit
        for each bin_id, bin in bins:
            for each bin_trip in bin.trips:
                if similar(trip, bin_trip):
                    append trip to bin.trips

        # prediction
        for each bin_id, bin in bins:
            for each bin_trip in bin.trips:
                if similar(trip, bin_trip):
                    return bin.predictions: List[Prediction]

        the number of predictions is not assumed to be the number of features.

        :param dir: the model load/save directory
        :param user_id: identity (UUID) of the e-mission user
        :param metric: type of similarity metric to use
        :param sim_thresh: max distance threshold for similarity (assumed meters)
        :param apply_cutoff: ignore clusters which are small, based on a "knee point" heuristic (default False)
        """
        super().__init__()
        self.metric = metric
        self.sim_thresh = sim_thresh
        self.apply_cutoff = apply_cutoff
        self.bins: Dict[int, Dict] = {}
        self.loaded = False

    def fit(self, trips: List[ecwc.Confirmedtrip]):
        """train the model by passing data, where each row in the data
        corresponds to a label at the matching index of the label input

        :param trips: 2D array of features to train from
        """
        self.bins = {}
        self._assign_bins(trips)
        if len(self.bins) > 1 and self.apply_cutoff:
            self._apply_cutoff()
        self._generate_predictions()
        logging.info(f"model fit to trip data")

    def predict(self, trip: ecwc.Confirmedtrip) -> Tuple[List[Dict], int]:
        if not self.loaded:
            msg = f"predict called on unloaded model for user {self.user_id}"
            raise IOError(msg)

        logging.debug(f"running greedy similarity clustering")
        predicted_bin, bin_record = self._nearest_bin(trip)
        if predicted_bin is None:
            logging.debug(f"unable to predict bin for trip {trip}")
            return [], -1
        else:
            labels = bin_record['prediction']
            n_features = len(bin_record['features'])
            logging.debug(f"found cluster {predicted_bin} with labels {labels}")
            return labels, n_features

    def is_incremental(self) -> bool:
        """
        greedy similarity binning is not an incremental model
        """
        return False

    def to_dict(self) -> Dict:
        return self.bins

    def from_dict(self, model: Dict):
        self.bins = model

    def extract_features(self, trip: ecwc.Confirmedtrip) -> List[float]:
        features = self.metric.extract_features(trip)
        return features

    def _assign_bins(self, trips: List[ecwc.Confirmedtrip]):
        """
        assigns each trip to a bin by greedy similarity search
        [see https://github.com/e-mission/e-mission-server/blob/5b9e608154de15e32df4f70a07a5b95477e7dbf5/emission/analysis/modelling/tour_model/similarity.py#L118]

        :param data: trips to assign to bins
        :type data: List[Confirmedtrip]
        """
        for trip in trips:
            trip_features = self.extract_features(trip)
            trip_labels = trip['data']['user_input']
            bin_id, bin_record = self._nearest_bin(trip)
            if bin_id is not None:
                # add to existing bin
                bin_record['features'].append(trip_features)
                bin_record['labels'].append(trip_labels)
            else:
                # create new bin
                new_bin_id = len(self.bins)
                new_bin_record = {
                    "features": [trip_features],
                    "labels": [trip_labels],
                    "predictions": []
                }
                self.bins[new_bin_id] = new_bin_record

    def _nearest_bin(self, trip: ecwc.Confirmedtrip) -> Tuple[Optional[int], Optional[Dict]]:
        """
        finds a bin which contains at least one matching feature. the 
        first record matching by similarity measure is returned. if
        none are found, (None, None) is returned.

        :param trip: incoming trip features to test with
        :return: nearest bin record, if found
        """
        trip_features = self.extract_features(trip)
        selected_bin = None
        selected_record = None
        
        for bin_id, bin_record in self.bins.items():
            if self.metric.similar(trip_features, bin_record['features'], self.sim_thresh):
                selected_bin = bin_id
                selected_record = bin_record
                break
                
        return selected_bin, selected_record

    def _apply_cutoff(self):
        """
        removes small clusters by an "elbow search" heuristic. see
        https://stackoverflow.com/a/2022348/4803266.
        """
        num_bins = len(self.bins)
        bin_sizes = [len(bin_rec['features']) for bin_rec in self.bins.values()]
        _, cutoff_bin_size = util.find_knee_point(bin_sizes)
        logging.debug(
            "bins = %s, elbow distance = %s" % (num_bins, cutoff_bin_size)
        )

        updated_bins = {bin_id: bin_rec 
                        for bin_id, bin_rec in self.bins.items() 
                        if len(bin_rec['features']) >= cutoff_bin_size}

        removed = len(self.bins) - len(updated_bins)
        logging.debug(
            f"removed %s bins with less than %s entries"
            % (removed, cutoff_bin_size)
        )
        # previous version held onto the removed bins for analysis,
        # we could do that here if that use case is still relevant
        self.bins = updated_bins

    def _generate_predictions(self):
        """
        helper function to transform binned features and labels into predictions.
        taken from [https://github.com/e-mission/e-mission-server/blob/10772f892385d44e11e51e796b0780d8f6609a2c/emission/analysis/modelling/tour_model_first_only/build_save_model.py#L40]

        for each bin, the unique label combinations are counted. their 
        probability is estimated with label_count / total_labels.
        """
        for _, bin_record in self.bins:
            user_label_df = pd.DataFrame(bin_record['labels'])
            user_label_df = lp.map_labels(user_label_df).dropna()
            # compute the sum of trips in this cluster
            sum_trips = len(user_label_df)
            # compute unique label sets and their probabilities in one cluster
            # 'p' refers to probability
            unique_labels = user_label_df.groupby(user_label_df.columns.tolist()).size().reset_index(name='uniqcount')
            unique_labels['p'] = unique_labels.uniqcount / sum_trips
            labels_columns = user_label_df.columns.to_list()
            bin_label_combo_list = []
            for i in range(len(unique_labels)):
                one_set_labels = {}
                # e.g. labels_only={'mode_confirm': 'pilot_ebike', 'purpose_confirm': 'work', 'replaced_mode': 'walk'}
                labels_only = {column: unique_labels.iloc[i][column] for column in labels_columns}
                one_set_labels["labels"] = labels_only
                one_set_labels['p'] = unique_labels.iloc[i]['p']
                # e.g. one_set_labels = {'labels': {'mode_confirm': 'walk', 'replaced_mode': 'walk', 'purpose_confirm': 'exercise'}, 'p': 1.0}
                bin_label_combo_list.append(one_set_labels)
            bin_record['predictions'] = bin_label_combo_list
