import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import joblib
from typing import Dict, List, Optional, Tuple
import emission.core.wrapper.confirmedtrip as ecwc
import logging
import numpy as np


import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.dbscan_svm as eamtd
import emission.analysis.modelling.trip_model.util as eamtu
import emission.analysis.modelling.trip_model.config as eamtc

from sklearn.ensemble import RandomForestClassifier
class ForestClassifier(eamuu.TripModel):

    def __init__(self,config=None):

        # expected_keys = [
        #     'metric',
        #     'similarity_threshold_meters', 
        #     'apply_cutoff', 
        #     'incremental_evaluation'
        # ]
        # for k in expected_keys:
        #     if config.get(k) is None:
        #         msg = f"greedy trip model config missing expected key {k}"
        #         raise KeyError(msg)      
        if config is None:
            config = eamtc.get_config_value_or_raise('model_parameters.forest')
            logging.debug(f'ForestClassifier loaded model config from file')
        else:
            logging.debug(f'ForestClassifier using model config argument')
        
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
        

    def fit(self,data: List[ecwc.Confirmedtrip],data_df=None):
             # get location features
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
    
    def predict(self, data: List[float]) -> Tuple[List[Dict], int]:
        pass

    def to_dict(self) -> Dict:
        return joblib.dump(self,compress=3)
    
    def from_dict(self, model: Dict):
        pass

    def is_incremental(self) -> bool:
        pass

    def extract_features(self, trip: ecwc.Confirmedtrip) -> List[float]:
        pass

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