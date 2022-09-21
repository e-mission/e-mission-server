import logging
from tokenize import group
from typing import Dict, List, Optional, Tuple

import emission.analysis.modelling.similarity.similarity_metric_type as eamssmt
import emission.analysis.modelling.tour_model.label_processing as lp
import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.util as util
import emission.analysis.modelling.trip_model.config as eamtc
import emission.core.wrapper.confirmedtrip as ecwc
import pandas as pd


class GreedySimilarityBinning(eamuu.TripModel):

    is_incremental: bool = False  # overwritten during __init__

    def __init__(self, config=None):
        """
        instantiate a clustering model for a user.

        replaces the original similarity class
        [https://github.com/e-mission/e-mission-server/blob/5b9e608154de15e32df4f70a07a5b95477e7dbf5/emission/analysis/modelling/tour_model/similarity.py#L67]

        this technique employs a greedy similarity heuristic to associate
        trips with collections of probabilistic class labels. new bins are
        created when the next feature vector is not similar to any existing bins.
        for a new feature vector to be similar to an existing bin, it must be
        similar to all of the previous feature vectors found in that bin, by way
        of a provided similarity metric and threshold value.

        in pseudocode:
        
        # fit
        for each bin_id, bin in bins:
            for each bin_feature_row in bin.feature_rows:
                if not similar(trip.feature_row, bin_feature_row):
                    return
                append trip to bin

        the prediction of labels for some input trip takes a similar form,
        where the first bin that is found to be similar is treated as the 
        class label to apply:

        # prediction
        for each bin_id, bin in bins:
            for each bin_feature_row in bin.feature_rows:
                if not similar(trip.feature_row, bin_feature_row):
                    break
            return bin_id

        to train the predictions, label sets are aggregated within a bin so that
        the occurences of some unique label combination is counted. the probability
        of a specific unique label combination is assigned by the proportion
        of counts of this unique label set to the total number of trips stored at
        this bin. the set of unique label sets and their prediction value are then
        returned during prediction.

        in terms of the data structure of the model, each bin is a Dictionary with 
        three fields, "feature_rows", "labels", and "predictions", each a list.
        whereas the number and index of "feature_rows" and "labels" are assumed to 
        match and be idempotent across multiple training calls, the "predictions" 
        are over-written at each call of "fit" and are not assumed to match the number 
        of "feature_rows" or "labels" stored in a bin.

        historical note: the original similarity class (link above) used a nested list data 
        structure to capture the notion of binning. this was then copied into
        a Dict when the model needed to be saved. the same technique can be re-written to 
        work directly on Dictionaries with no loss in the algorithm's time complexity. this 
        also helps when running in incremental mode to persist relevant training data and to
        minimize codec + serialization errors.

        the data takes the form:
        {
            bin_id: {
                "feature_rows": [
                    [f1, f2, .., fn],
                    ...
                ],
                "labels": [
                    { label1: value1, ... }
                ],
                "predictions": [
                    { "labels": { label1: value1, ... }, 'p': p_val }
                ]
            }
        }
        where
        - bin_id:  str    index of a bin containing similar trips, as a string
                          (string type for bin_id comes from mongodb object key type requirements)
        - f_x:     float  feature value (an ordinate such as origin.x)
        - label_x: str    OpenPATH user label category such as "mode_confirm"
        - value_x: str    user-provided label for a category
        - p_val:   float  probability of a prediction, real number in [0, 1]

        :param config: if provided, a manual configuration for testing purposes. these
                       values should be provided by the config file when running OpenPATH.
                       see config.py for more details.
        """

        if config is None:
            config = eamtc.get_config_value_or_raise('model_parameters.greedy')
            logging.debug(f'GreedySimilarityBinning loaded model config from file')
        else:
            logging.debug(f'GreedySimilarityBinning using model config argument')
        
        expected_keys = [
            'metric',
            'similarity_threshold_meters', 
            'apply_cutoff', 
            'incremental_evaluation'
        ]
        for k in expected_keys:
            if config.get(k) is None:
                msg = f"greedy trip model config missing expected key {k}"
                raise KeyError(msg)

        self.metric = eamssmt.SimilarityMetricType.from_str(config['metric']).build()
        self.sim_thresh = config['similarity_threshold_meters']
        self.apply_cutoff = config['apply_cutoff']
        self.is_incremental = config['incremental_evaluation']

        self.bins: Dict[str, Dict] = {}
        

    def fit(self, trips: List[ecwc.Confirmedtrip]):
        """train the model by passing data, where each row in the data
        corresponds to a label at the matching index of the label input

        :param trips: 2D array of features to train from
        """
        
        logging.debug(f'fit called with {len(trips)} trips')
        unlabeled = list(filter(lambda t: len(t['data']['user_input']) == 0, trips))
        if len(unlabeled) > 0:
            msg = f'model.fit cannot be called with unlabeled trips, found {len(unlabeled)}'
            raise Exception(msg)
        self._assign_bins(trips)
        if len(self.bins) > 1 and self.apply_cutoff:
            self._apply_cutoff()
        self._generate_predictions()

        logging.info(f"greedy binning model fit to {len(trips)} rows of trip data")

    def predict(self, trip: ecwc.Confirmedtrip) -> Tuple[List[Dict], int]:

        logging.debug(f"running greedy similarity clustering")
        predicted_bin_id, predicted_bin_record = self._nearest_bin(trip)
        if predicted_bin_id is None:
            logging.debug(f"unable to predict bin for trip {trip}")
            return [], 0
        else:
            predictions = predicted_bin_record['predictions']
            n_features = len(predicted_bin_record['feature_rows'])
            logging.debug(f"found cluster {predicted_bin_id} with predictions {predictions}")
            return predictions, n_features

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
        logging.debug(f"_assign_bins called with {len(trips)} trips")
        for trip in trips:
            trip_features = self.extract_features(trip)
            trip_labels = trip['data']['user_input']
            
            bin_id = self._find_matching_bin_id(trip_features)
            if bin_id is not None:
                # add to existing bin
                logging.debug(f"adding trip to bin {bin_id} with features {trip_features}")
                self.bins[bin_id]['feature_rows'].append(trip_features)
                self.bins[bin_id]['labels'].append(trip_labels)
            else:
                # create new bin
                new_bin_id = str(len(self.bins))
                new_bin_record = {
                    'feature_rows': [trip_features],
                    'labels': [trip_labels],
                    'predictions': []
                }
                logging.debug(f"creating new bin {new_bin_id} at location {trip_features}")
                self.bins[new_bin_id] = new_bin_record

    def _find_matching_bin_id(self, trip_features: List[float]) -> Optional[str]:
        """
        finds an existing bin where all bin features are "similar" to the incoming
        trip features.

        :param trip_features: feature row for the incoming trip
        :return: the id of a bin if a match was found, otherwise None
        """
        for bin_id, bin_record in self.bins.items():
                matches_bin = all([self.metric.similar(trip_features, bin_sample, self.sim_thresh)
                    for bin_sample in bin_record['feature_rows']])
                if matches_bin:
                    return bin_id
        return None

    def _nearest_bin(self, trip: ecwc.Confirmedtrip) -> Tuple[Optional[int], Optional[Dict]]:
        """
        finds a bin which contains at least all matching features. the 
        first record matching by similarity measure is returned. if
        none are found, (None, None) is returned.

        [see https://github.com/e-mission/e-mission-server/blob/10772f892385d44e11e51e796b0780d8f6609a2c/emission/analysis/modelling/tour_model_first_only/load_predict.py#L46]

        :param trip: incoming trip features to test with
        :return: nearest bin record, if found
        """
        logging.debug(f"_nearest_bin called")

        trip_features = self.extract_features(trip)
        
        for bin_id, bin_record in self.bins.items():
            for bin_features in bin_record['feature_rows']:
                if self.metric.similar(trip_features, bin_features, self.sim_thresh):
                    logging.debug(f"found nearest bin id {bin_id}")
                    logging.debug(f"similar: {trip_features}, {bin_features}")
                    return bin_id, bin_record

        return None, None

    def _apply_cutoff(self):
        """
        removes small clusters by an "elbow search" heuristic. see
        https://stackoverflow.com/a/2022348/4803266.
        Copied over from https://github.com/e-mission/e-mission-server/blob/5b9e608154de15e32df4f70a07a5b95477e7dbf5/emission/analysis/modelling/tour_model/similarity.py#L158
        """
        # the cutoff point is an index along the sorted bins. any bin with a gte
        # index value is removed, as that bin has been found to be smaller than the cutoff.
        # This was the last line of calc_cutoff_bins in the old code, and is moved to the equivalent of delete_bins in the new code
        bins_sorted =  self.bins.sort(key=lambda bin: len(bin['feature_rows']), reverse=True)
        


# The first two lines below correspond to the original lines below in the original elbow_distance
#         y = [0] * len(self.bins)
#         for i in range(len(self.bins)):
#             y[i] = len(self.bins[i])
        num_bins = len(bins_sorted)
        bin_sizes = [len(bin_rec['feature_rows']) for bin_rec in bins_sorted.values()]
        _, cutoff_bin_size = util.find_knee_point(bin_sizes)
        logging.debug(
            "bins = %s, elbow distance = %s" % (num_bins, cutoff_bin_size)
        )

        updated_bins = {bin_id: bin_rec 
                        for bin_id, bin_rec in bins_sorted.items() 
                        if len(bin_rec['feature_rows']) >= cutoff_bin_size}

        removed = len(bins_sorted) - len(updated_bins)
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
        for _, bin_record in self.bins.items():
            user_label_df = pd.DataFrame(bin_record['labels'])
            user_label_df = lp.map_labels(user_label_df).dropna()
            # compute the sum of trips in this cluster
            sum_trips = len(user_label_df)
            # compute unique label sets and their probabilities in one cluster
            # 'p' refers to probability
            group_cols = user_label_df.columns.tolist()
            unique_labels = user_label_df.groupby(group_cols).size().reset_index(name='uniqcount')
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
