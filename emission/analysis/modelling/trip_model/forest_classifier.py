import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import joblib
from typing import Dict, List, Optional, Tuple
from sklearn.metrics.pairwise import haversine_distances
import emission.core.wrapper.confirmedtrip as ecwc
import logging
import numpy as np
import copy

import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.dbscan_svm as eamtd
import emission.analysis.modelling.trip_model.util as eamtu
import emission.analysis.modelling.trip_model.config as eamtc
import emission.storage.timeseries.builtin_timeseries as estb
from sklearn.exceptions import NotFittedError

from sklearn.ensemble import RandomForestClassifier

EARTH_RADIUS = 6371000

class ForestClassifier(eamuu.TripModel):

    def __init__(self,config=None):

        if config is None:
            config = eamtc.get_config_value_or_raise('model_parameters.forest')
            logging.debug(f'ForestClassifier loaded model config from file')
        else:
            logging.debug(f'ForestClassifier using model config argument')
    
        random_forest_expected_keys = [
            'loc_feature',
            'n_estimators',
            'criterion',
            'max_depth',
            'min_samples_split',
            'min_samples_leaf',
            'max_features',
            'bootstrap',
        ]            
        cluster_expected_keys= [
            'radius',
            'size_thresh',  
            'purity_thresh',
            'gamma',
            'C',
            'use_start_clusters',
            'use_trip_clusters',
        ]

        for k in random_forest_expected_keys:
            if config.get(k) is None:
                msg = f"forest trip model config missing expected key {k}"
                raise KeyError(msg)   
        
        if config['loc_feature'] == 'cluster':
            for k in cluster_expected_keys:
                if config.get(k) is None:
                    msg = f"cluster trip model config missing expected key {k}"
                    raise KeyError(msg)

        self.loc_feature = config['loc_feature']
        self.radius = config['radius']
        self.size_thresh = config['size_thresh']
        self.purity_thresh = config['purity_thresh']
        self.gamma = config['gamma']
        self.C = config['C']
        self.n_estimators = config['n_estimators']
        self.criterion =config['criterion']
        self.max_depth = config['max_depth'] if config['max_depth'] != 'null' else None
        self.min_samples_split = config['min_samples_split']
        self.min_samples_leaf = config['min_samples_leaf']
        self.max_features = config['max_features']
        self.bootstrap = config['bootstrap']
        self.random_state = config['random_state']
        # self.drop_unclustered = drop_unclustered
        self.use_start_clusters = config['use_start_clusters']
        self.use_trip_clusters = config['use_trip_clusters']
        self.base_features = [
            'duration',
            'distance',
            'start_local_dt_year',
            'start_local_dt_month',
            'start_local_dt_day',
            'start_local_dt_hour',
            'start_local_dt_weekday',
            'end_local_dt_year',  # most likely the same as the start year
            'end_local_dt_month',  # most likely the same as the start month
            'end_local_dt_day',
            'end_local_dt_hour',
            'end_local_dt_weekday',
        ]
        self.targets = ['mode_true', 'purpose_true', 'replaced_true']

        if self.loc_feature == 'cluster':
            # clustering algorithm to generate end clusters
            self.end_cluster_model = eamtd.DBSCANSVMCluster(
                loc_type='end',
                radius=self.radius,
                size_thresh=self.size_thresh,
                purity_thresh=self.purity_thresh,
                gamma=self.gamma,
                C=self.C)

            if self.use_start_clusters or self.use_trip_clusters:
                # clustering algorithm to generate start clusters
                self.start_cluster_model = eamtd.DBSCANSVMCluster(
                    loc_type='start',
                    radius=self.radius,
                    size_thresh=self.size_thresh,
                    purity_thresh=self.purity_thresh,
                    gamma=self.gamma,
                    C=self.C)

                if self.use_trip_clusters:
                    # helper class to generate trip-level clusters
                    self.trip_grouper = eamtd.TripGrouper(
                        start_cluster_col='start_cluster_idx',
                        end_cluster_col='end_cluster_idx')

            # wrapper class to generate one-hot encodings for cluster indices
            self.cluster_enc = eamtu.OneHotWrapper(sparse=False,
                                             handle_unknown='ignore')

        # wrapper class to generate one-hot encodings for purposes and modes
        self.purpose_enc = eamtu.OneHotWrapper(impute_missing=True,
                                         sparse=False,
                                         handle_unknown='error')
        self.mode_enc = eamtu.OneHotWrapper(impute_missing=True,
                                      sparse=False,
                                      handle_unknown='error')

        # ensemble classifiers for each label category
        self.purpose_predictor = RandomForestClassifier(
            n_estimators=self.n_estimators,
            criterion=self.criterion,
            max_depth=self.max_depth,
            min_samples_split=self.min_samples_split,
            min_samples_leaf=self.min_samples_leaf,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            random_state=self.random_state)
        self.mode_predictor = RandomForestClassifier(
            n_estimators=self.n_estimators,
            criterion=self.criterion,
            max_depth=self.max_depth,
            min_samples_split=self.min_samples_split,
            min_samples_leaf=self.min_samples_leaf,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            random_state=self.random_state)
        self.replaced_predictor = RandomForestClassifier(
            n_estimators=self.n_estimators,
            criterion=self.criterion,
            max_depth=self.max_depth,
            min_samples_split=self.min_samples_split,
            min_samples_leaf=self.min_samples_leaf,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            random_state=self.random_state)
        

    def fit(self,trips: List[ecwc.Confirmedtrip]):
             # get location features
        logging.debug(f'fit called with {len(trips)} trips')

        unlabeled = list(filter(lambda t: len(t['data']['user_input']) == 0, trips))
        if len(unlabeled) > 0:
            msg = f'model.fit cannot be called with unlabeled trips, found {len(unlabeled)}'
            raise Exception(msg)        
        data_df = estb.BuiltinTimeSeries.to_data_df("analysis/confirmed_trip",trips)

        if self.loc_feature == 'cluster':
            # fit clustering model(s) and one-hot encode their indices
            # TODO: consolidate start/end_cluster_model in a single instance
            # that has a location_type parameter in the fit() method
            self.end_cluster_model.fit(data_df)

            clusters_to_encode = self.end_cluster_model.train_df[[
                'end_cluster_idx'
            ]].copy()  # copy is to avoid SettingWithCopyWarning

            if self.use_start_clusters or self.use_trip_clusters:
                self.start_cluster_model.fit(data_df)

                if self.use_start_clusters:
                    clusters_to_encode = pd.concat([
                        clusters_to_encode,
                        self.start_cluster_model.train_df[['start_cluster_idx']]
                    ],
                                                   axis=1)
                if self.use_trip_clusters:
                    start_end_clusters = pd.concat([
                        self.end_cluster_model.train_df[['end_cluster_idx']],
                        self.start_cluster_model.train_df[['start_cluster_idx']]
                    ],
                                                   axis=1)
                    trip_cluster_idx = self.trip_grouper.fit_transform(
                        start_end_clusters)
                    clusters_to_encode.loc[:,
                                           'trip_cluster_idx'] = trip_cluster_idx

            loc_features_df = self.cluster_enc.fit_transform(
                clusters_to_encode.astype(int))

            # clean the df again because we need it in the next step
            # TODO: remove redundancy
            self.train_df = self._clean_data(data_df)

            # TODO: move below code into a reusable function
            if self.train_df.purpose_true.isna().any():
                num_nan = self.train_df.purpose_true.value_counts(
                    dropna=False).loc[np.nan]
                logging.info(
                    f'dropping {num_nan}/{len(self.train_df)} trips that are missing purpose labels'
                )
                self.train_df = self.train_df.dropna(
                    subset=['purpose_true']).reset_index(drop=True)
            if len(self.train_df) == 0:
                # i.e. no valid trips after removing all nans
                raise Exception('no valid trips; nothing to fit')

        else:  # self.loc_feature == 'coordinates'
            self.train_df = self._clean_data(data_df)

            # TODO: move below code into a reusable function
            if self.train_df.purpose_true.isna().any():
                num_nan = self.train_df.purpose_true.value_counts(
                    dropna=False).loc[np.nan]
                logging.info(
                    f'dropping {num_nan}/{len(self.train_df)} trips that are missing purpose labels'
                )
                self.train_df = self.train_df.dropna(
                    subset=['purpose_true']).reset_index(drop=True)
            if len(self.train_df) == 0:
                # i.e. no valid trips after removing all nans
                raise Exception('no valid trips; nothing to fit')

            loc_features_df = self.train_df[[
                'start_lon', 'start_lat', 'end_lon', 'end_lat'
            ]]

        # prepare data for the ensemble classifiers

        # note that we want to use purpose data to aid our mode predictions,
        # and use both purpose and mode data to aid our replaced-mode
        # predictions
        # thus, we want to one-hot encode the purpose and mode as data
        # features, but also preserve an unencoded copy for the target columns

        # dataframe holding all features and targets
        self.Xy_train = pd.concat(
            [self.train_df[self.base_features + self.targets], loc_features_df],
            axis=1)

        # encode purposes and modes
        onehot_purpose_df = self.purpose_enc.fit_transform(
            self.Xy_train[['purpose_true']], output_col_prefix='purpose')
        onehot_mode_df = self.mode_enc.fit_transform(
            self.Xy_train[['mode_true']], output_col_prefix='mode')
        self.Xy_train = pd.concat(
            [self.Xy_train, onehot_purpose_df, onehot_mode_df], axis=1)

        # for predicting purpose, drop encoded purpose and mode features, as
        # well as all target labels
        self.X_purpose = self.Xy_train.dropna(subset=['purpose_true']).drop(
            labels=self.targets + self.purpose_enc.onehot_encoding_cols +
            self.mode_enc.onehot_encoding_cols,
            axis=1)

        # for predicting mode, we want to keep purpose data
        self.X_mode = self.Xy_train.dropna(subset=['mode_true']).drop(
            labels=self.targets + self.mode_enc.onehot_encoding_cols, axis=1)

        # for predicting replaced-mode, we want to keep purpose and mode data
        self.X_replaced = self.Xy_train.dropna(subset=['replaced_true']).drop(
            labels=self.targets, axis=1)

        self.y_purpose = self.Xy_train['purpose_true'].dropna()
        self.y_mode = self.Xy_train['mode_true'].dropna()
        self.y_replaced = self.Xy_train['replaced_true'].dropna()

        # fit classifiers
        if len(self.X_purpose) > 0:
            self.purpose_predictor.fit(self.X_purpose, self.y_purpose)
        if len(self.X_mode) > 0:
            self.mode_predictor.fit(self.X_mode, self.y_mode)
        if len(self.X_replaced) > 0:
            self.replaced_predictor.fit(self.X_replaced, self.y_replaced)
        logging.info(f"Forest model fit to {len(trips)} rows of trip data")

    def predict(self, trips: List[float]) -> Tuple[List[Dict], int]:
        logging.debug(f"forest classifier predict called with {len(trips)} trips")

        if len(trips) == 0:
            msg = f'model.predict cannot be called with 0 trips'
            raise Exception(msg)    
        
        # CONVERT TRIPS TO dataFrame
        test_df = estb.BuiltinTimeSeries.to_data_df("analysis/confirmed_trip",trips)

        self.X_test_for_purpose = self._get_X_test_for_purpose(test_df)

        ########################
        ### make predictions ###
        ########################
        # note that we want to use purpose data to aid our mode predictions,
        # and use both purpose and mode data to aid our replaced-mode
        # predictions
        try:
            purpose_proba_raw = self.purpose_predictor.predict_proba(
                self.X_test_for_purpose)
            purpose_proba = pd.DataFrame(
                purpose_proba_raw, columns=self.purpose_predictor.classes_)
            purpose_pred = purpose_proba.idxmax(axis=1)

            # update X_test with one-hot-encoded purpose predictions to aid
            # mode predictor
            onehot_purpose_df = self.purpose_enc.transform(
                pd.DataFrame(purpose_pred).set_index(
                    self.X_test_for_purpose.index))
            self.X_test_for_mode = pd.concat(
                [self.X_test_for_purpose, onehot_purpose_df], axis=1)

            mode_proba, replaced_proba = self._try_predict_proba_mode_replaced()

        except NotFittedError as e:
            # if we can't predict purpose, we can still try to predict mode and
            # replaced-mode without one-hot encoding the purpose

            purpose_pred = np.full((len(self.X_test_for_purpose), ), np.nan)
            purpose_proba_raw = np.full((len(self.X_test_for_purpose), 1), 0)
            purpose_proba = pd.DataFrame(purpose_proba_raw, columns=[np.nan])

            self.X_test_for_mode = self.X_test_for_purpose
            mode_proba, replaced_proba = self._try_predict_proba_mode_replaced()

        mode_pred = mode_proba.idxmax(axis=1)
        replaced_pred = replaced_proba.idxmax(axis=1)

        if (purpose_pred.dtype == np.float64 and mode_pred.dtype == np.float64
                and replaced_pred.dtype == np.float64):
            # this indicates that all the predictions are np.nan so none of the
            # random forest classifiers were fitted
            raise NotFittedError

        proba_dfs = []
        for label_type, proba in zip(
            ['purpose', 'mode', 'replaced'],
            [purpose_proba, mode_proba, replaced_proba]):
            proba['top_pred'] = proba.idxmax(axis=1)
            proba['top_proba'] = proba.max(axis=1, skipna=True)
            proba['clusterable'] = self._clusterable(
                self.X_test_for_purpose).astype(bool)
            proba = pd.concat([proba], keys=[label_type], axis=1)
            proba_dfs += [proba]

        self.proba_df = pd.concat(proba_dfs, axis=1)
        return self.proba_df

    def _try_predict_proba_mode_replaced(self):
        """ Try to predict mode and replaced-mode. Handles error in case the 
            ensemble algorithms were not fitted. 
        
            Requires self.X_test_for_mode to have already been set. (These are 
            the DataFrames containing the test data to be passed into self.
            mode_predictor.) 

            Returns: mode_proba and replaced_proba, two DataFrames containing 
                class probabilities for mode and replaced-mode respectively
        """

        try:
            # predict mode
            mode_proba_raw = self.mode_predictor.predict_proba(
                self.X_test_for_mode)
            mode_proba = pd.DataFrame(mode_proba_raw,
                                      columns=self.mode_predictor.classes_)
            mode_pred = mode_proba.idxmax(axis=1)

            # update X_test with one-hot-encoded mode predictions to aid
            # replaced-mode predictor
            onehot_mode_df = self.mode_enc.transform(
                pd.DataFrame(mode_pred).set_index(self.X_test_for_mode.index))
            self.X_test_for_replaced = pd.concat(
                [self.X_test_for_mode, onehot_mode_df], axis=1)
            replaced_proba = self._try_predict_proba_replaced()

        except NotFittedError as e:
            mode_proba_raw = np.full((len(self.X_test_for_mode), 1), 0)
            mode_proba = pd.DataFrame(mode_proba_raw, columns=[np.nan])

            # if we don't have mode predictions, we *could* still try to
            # predict replaced mode (but if the user didn't input mode labels
            # then it's unlikely they would input replaced-mode)
            self.X_test_for_replaced = self.X_test_for_mode
            replaced_proba = self._try_predict_proba_replaced()

        return mode_proba, replaced_proba

    def _get_X_test_for_purpose(self, test_df):
        """ Do the pre-processing to get data that we can then pass into the 
            ensemble classifiers. 
        """
        if self.loc_feature == 'cluster':
            # get clusters
            self.end_cluster_model.predict(test_df)
            clusters_to_encode = self.end_cluster_model.test_df[[
                'end_cluster_idx'
            ]].copy()  # copy is to avoid SettingWithCopyWarning

            if self.use_start_clusters or self.use_trip_clusters:
                self.start_cluster_model.predict(test_df)

                if self.use_start_clusters:
                    clusters_to_encode = pd.concat([
                        clusters_to_encode,
                        self.start_cluster_model.test_df[['start_cluster_idx']]
                    ],
                                                   axis=1)
                if self.use_trip_clusters:
                    start_end_clusters = pd.concat([
                        self.end_cluster_model.test_df[['end_cluster_idx']],
                        self.start_cluster_model.test_df[['start_cluster_idx']]
                    ],
                                                   axis=1)
                    trip_cluster_idx = self.trip_grouper.transform(
                        start_end_clusters)
                    clusters_to_encode.loc[:,
                                           'trip_cluster_idx'] = trip_cluster_idx

            # one-hot encode the cluster indices
            loc_features_df = self.cluster_enc.transform(clusters_to_encode)
        else:  # self.loc_feature == 'coordinates'
            test_df = self._clean_data(test_df)
            loc_features_df = test_df[[
                'start_lon', 'start_lat', 'end_lon', 'end_lat'
            ]]

        # extract the desired data
        X_test = pd.concat([
            test_df[self.base_features].reset_index(drop=True),
            loc_features_df.reset_index(drop=True)
        ],
                           axis=1)

        return X_test


    def _clusterable(self, test_df):
        """ Check if the end points can be clustered (i.e. are within <radius> 
            meters of an end point from the training set) 
        """
        if self.loc_feature == 'cluster':
            return self.end_cluster_model.test_df.end_cluster_idx >= 0

        n_samples = test_df.shape[0]
        clustered = np.ones(shape=n_samples, dtype=int) * False

        train_coordinates = self.train_df[['end_lat', 'end_lon']]
        train_radians = np.radians(train_coordinates)

        for idx, row in test_df.reset_index(drop=True).iterrows():
            # calculate the distances between the ith test data and all points,
            # then find the minimum distance for each point and check if it's
            # within the distance threshold.
            # unfortunately, pairwise_distances_argmin() does not support
            # haversine distance, so we have to reimplement it ourselves
            new_loc_radians = np.radians(row[["end_lat", "end_lon"]].to_list())
            new_loc_radians = np.reshape(new_loc_radians, (1, 2))
            dist_matrix_meters = haversine_distances(
                new_loc_radians, train_radians) * EARTH_RADIUS

            shortest_dist = np.min(dist_matrix_meters)
            if shortest_dist < self.radius:
                clustered[idx] = True

        return clustered
    
    def _clean_data(self, df):
        """ Clean a dataframe of trips. 
            (Drop trips with missing start/end locations, expand the user input 
            columns, ensure all essential columns are present)

            Args:
                df: a dataframe of trips. must contain the columns 'start_loc', 
                'end_loc', and should also contain the user input columns 
                ('mode_confirm', 'purpose_confirm', 'replaced_mode') if 
                available
        """
        assert 'start_loc' in df.columns and 'end_loc' in df.columns

        # clean up the dataframe by dropping entries with NaN locations and
        # reset index
        num_nan = 0
        if df.start_loc.isna().any():
            num_nan += df.start_loc.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['start_loc'])
        if df.end_loc.isna().any():
            num_nan += df.end_loc.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['end_loc'])

        # expand the 'start_loc' and 'end_loc' column into 'start_lat',
        # 'start_lon', 'end_lat', and 'end_lon' columns
        df = self.expand_coords(df)

        # drop trips with missing coordinates
        if df.start_lat.isna().any():
            num_nan += df.start_lat.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['start_lat'])
        if df.start_lon.isna().any():
            num_nan += df.start_lon.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['start_lon'])
        if df.end_lat.isna().any():
            num_nan += df.end_lat.value_counts(dropna=False).loc[np.nan]
            df = df.dropna(subset=['end_lat'])
        if df.end_lon.isna().any():
            num_nan = df.end_lon.value_counts(dropna=False).loc[np.nan]
            df += df.dropna(subset=['end_lon'])
        if num_nan > 0:
            logging.info(
                f'dropped {num_nan} trips that are missing location coordinates'
            )

        df = df.rename(
            columns={
                'mode_confirm': 'mode_true',
                'purpose_confirm': 'purpose_true',
                'replaced_mode': 'replaced_true'
            })

        for category in ['mode_true', 'purpose_true', 'replaced_true']:
            if category not in df.columns:
                # for example, if a user labels all their trip modes but none of their trip purposes
                df.loc[:, category] = np.nan

        return df.reset_index(drop=True)
    
    def expand_coords(exp_df, purpose=None):
        """
            copied and modifed from get_loc_df_for_purpose() in the 'Radius
            selection' notebook
        """
        purpose_trips = exp_df
        if purpose is not None:
            purpose_trips = exp_df[exp_df.purpose_confirm == purpose]

        dfs = [purpose_trips]
        for loc_type in ['start', 'end']:
            df = pd.DataFrame(
                purpose_trips[loc_type +
                            "_loc"].apply(lambda p: p["coordinates"]).to_list(),
                columns=[loc_type + "_lon", loc_type + "_lat"])
            df = df.set_index(purpose_trips.index)
            dfs.append(df)

        # display.display(end_loc_df.head())
        return pd.concat(dfs, axis=1)
    
def to_dict(self):
    """
    Convert the model to a dictionary suitable for storage.
    """
    data = {
        'purpose_predictor': joblib.dumps(self.purpose_predictor).hex(),
        'mode_predictor': joblib.dumps(self.mode_predictor).hex(),
        'replaced_predictor': joblib.dumps(self.replaced_predictor).hex(),
        'cluster_enc': joblib.dumps(self.cluster_enc).hex(),
        'purpose_enc': joblib.dumps(self.purpose_enc).hex(),
        'mode_enc': joblib.dumps(self.mode_enc).hex(),
    }

    if self.loc_feature == 'cluster':
        data.update({
        'end_cluster_model' : joblib.dumps(self.end_cluster_model).hex(),
        'start_cluster_model': joblib.dumps(self.start_cluster_model).hex(),
        'trip_grouper': joblib.dumps(self.trip_grouper).hex()})

    return data

def from_dict(self, model_data: Dict):
    """
    Load the model from a dictionary.
    """
    self.purpose_predictor = joblib.loads(bytes.fromhex(model_data['purpose_predictor']))
    self.mode_predictor = joblib.loads(bytes.fromhex(model_data['mode_predictor']))
    self.replaced_predictor = joblib.loads(bytes.fromhex(model_data['replaced_predictor']))
    self.cluster_enc = joblib.loads(bytes.fromhex(model_data['cluster_enc']))
    self.purpose_enc = joblib.loads(bytes.fromhex(model_data['purpose_enc']))
    self.mode_enc = joblib.loads(bytes.fromhex(model_data['mode_enc']))
    if self.loc_feature == 'cluster':    
        self.end_cluster_model = joblib.loads(bytes.fromhex(model_data['end_cluster_model']))
        self.start_cluster_model = joblib.loads(bytes.fromhex(model_data['start_cluster_model']))
        self.trip_grouper = joblib.loads(bytes.fromhex(model_data['trip_grouper']))

