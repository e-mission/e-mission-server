import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import joblib
from typing import Dict, List, Optional, Tuple
from sklearn.metrics.pairwise import haversine_distances
import emission.core.wrapper.confirmedtrip as ecwc
import logging
from io import BytesIO

import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.trip_model.config as eamtc
import emission.storage.timeseries.builtin_timeseries as estb
import emission.storage.decorations.trip_queries as esdtq
from emission.analysis.modelling.trip_model.models import ForestClassifier

EARTH_RADIUS = 6371000

class ForestClassifierModel(eamuu.TripModel):

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
        maxdepth =config['max_depth'] if config['max_depth']!='null' else None
        self.model=ForestClassifier( loc_feature=config['loc_feature'],
                                     radius= config['radius'],
                                     size_thresh=config['radius'],
                                     purity_thresh=config['purity_thresh'],
                                     gamma=config['gamma'],
                                     C=config['C'],
                                     n_estimators=config['n_estimators'],
                                     criterion=config['criterion'],
                                     max_depth=maxdepth, 
                                     min_samples_split=config['min_samples_split'],
                                     min_samples_leaf=config['min_samples_leaf'],
                                     max_features=config['max_features'],
                                     bootstrap=config['bootstrap'],
                                     random_state=config['random_state'],
                                     # drop_unclustered=False,
                                     use_start_clusters=config['use_start_clusters'],
                                     use_trip_clusters=config['use_trip_clusters'])
        

    def fit(self,trips: List[ecwc.Confirmedtrip]):
        '''
        trips : List of Entry type data 
        '''
        # check and raise exception if no data to fit
        logging.debug(f'fit called with {len(trips)} trips')

        unlabeled = list(filter(lambda t: len(t['data']['user_input']) == 0, trips))
        if len(unlabeled) > 0:
            msg = f'model.fit cannot be called with unlabeled trips, found {len(unlabeled)}'
            raise Exception(msg)    
        
        #Convert List of Entry to dataframe     
        data_df = estb.BuiltinTimeSeries.to_data_df("analysis/confirmed_trip",trips)
        labeled_trip_df = esdtq.filter_labeled_trips(data_df)
        expanded_labeled_trip_df= esdtq.expand_userinputs(labeled_trip_df)
        #fit models on dataframe
        self.model.fit(expanded_labeled_trip_df)       


    def predict(self, trip: List[float]) -> Tuple[List[Dict], int]:
        '''
        trip : A single trip whose mode, pupose and replaced mode are required
        returns.
        '''

        #check if theres no trip to predict        
        logging.debug(f"forest classifier predict called with {len(trip)} trips")
        if len(trip) == 0:
            msg = f'model.predict cannot be called with an empty trip'
            raise Exception(msg)        
        # CONVERT TRIP TO dataFrame        
        test_df = estb.BuiltinTimeSeries.to_data_df("analysis/confirmed_trip",[trip])
        predcitions_df= self.model.predict(test_df)

        # the predictions_df currently holds the highest probable options
        # individually in all three categories. the predictions_df are in the form 
        #
        # purpose_pred | purpose_proba | mode_pred | mode_proba | replaced_pred | replaced proba 
        # dog-park     |   1.0         |  e-bike   | 0.99       | walk          | 1.1 
        #
        #
        # However, to keep the trip model general, the forest model is expected to return  
        #
        #PREDICTIONS [  {'labels': {'mode_confirm': 'e-bike', 'replaced_mode': 'walk', 'purpose_confirm': 'dog-park'},
        #                 'p':  ( Currently average of the 3 probabilities)}]
        labels= {
            'mode_confirm': predcitions_df['mode_pred'].iloc[0],
            'replaced_mode' : predcitions_df['replaced_pred'].iloc[0],
            'purpose_confirm' : predcitions_df['purpose_pred'].iloc[0]
            }
        
        avg_proba = predcitions_df[['purpose_proba','mode_proba','replaced_proba']].mean(axis=1).iloc[0]
        predictions =[{
            'labels' : labels,
            'p' : avg_proba
        }]
        return predictions, len(predictions)

    def to_dict(self):
        """
        Convert the model to a dictionary suitable for storage.
        """
        data={}
        attr=[ 'purpose_predictor','mode_predictor','replaced_predictor','purpose_enc','mode_enc','train_df']
        if self.model.loc_feature == 'cluster':
            ## confirm this includes all the extra encoders/models
            attr.extend([ 'cluster_enc','end_cluster_model','start_cluster_model','trip_grouper'])
        for attribute_name in attr:
            if not hasattr(self.model,attribute_name):
                raise ValueError(f"Attribute {attribute_name} not found in the model")

            buffer=BytesIO()
            try:
                joblib.dump(getattr(self.model,attribute_name),buffer)
            except Exception as e:
                raise RuntimeError(f"Error serializing { attribute_name}: {str(e)}")    
            buffer.seek(0)
            data[attribute_name]=buffer.getvalue()

        return data

    def from_dict(self,model: Dict):
        """
        Load the model from a dictionary.
        """
        attr=[ 'purpose_predictor','mode_predictor','replaced_predictor','purpose_enc','mode_enc','train_df']
        if self.model.loc_feature == 'cluster':
            ## TODO : confirm this includes all the extra encoders/models
            attr.extend([ 'cluster_enc','end_cluster_model','start_cluster_model','trip_grouper'])
        for attribute_name in attr:
            if attribute_name not in model:
                raise ValueError(f"Attribute {attribute_name} missing in the model")
            try:
                buffer = BytesIO(model[attribute_name])
                setattr(self.model,attribute_name, joblib.load(buffer))
            except Exception as e:
                raise RuntimeError(f"Error deserializing { attribute_name}: {str(e)}")    
                # If we do not wish to raise the exception after logging the error, comment the line above

    def extract_features(self, trip: ecwc.Confirmedtrip) -> List[float]:
        """
        extract the relevant features for learning from a trip for this model instance

        :param trip: the trip to extract features from
        :type trip: Confirmedtrip
        :return: a vector containing features to predict from
        :rtype: List[float]
        """
        pass

    def is_incremental(self) -> bool:
        """
        whether this model requires the complete user history to build (False),
        or, if only the incremental data since last execution is required (True).

        :return: if the model is incremental. the current timestamp will be recorded
        in the analysis pipeline. the next call to this model will only include 
        trip data for trips later than the recorded timestamp.
        :rtype: bool
        """
        pass