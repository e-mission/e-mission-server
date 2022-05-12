from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import List, Tuple
import numpy as np
from pathlib import Path

from emission.analysis.modelling.probabilistic_clustering_model.prediction import Prediction


class ProbabilisticClusteringModel(metaclass=ABCMeta):

    @abstractmethod
    def load(self, user_id: str):
        """load a model from disk for the given user

        :param user_id: id for the user associated with this model
        :type user_id: str
        """

    @abstractmethod
    def save(self, user_id: str):
        """save this model to disk for the given user

        :param user_id: id for the user associated with this model
        :type user_id: str
        """
        pass

    @abstractmethod
    def fit(data: List[List[float]], labels: List[int]):
        """train the model on data, where each row in the data
        corresponds to a label at the matching index of the label input

        :param data: 2D array of features to train from
        :type data: List[List[float]]
        :param labels: vector of labels associated with the input data
        :type labels: List[int]
        """
        pass

    @abstractmethod
    def predict(self, data: List[float]) -> Tuple[List[Prediction], int]: 
        """use this model to predict labels for some data

        :param data: a single row of features in the model's feature space
        :type data: List[float]
        :return: the predictions and the total count of observations
        :rtype: Tuple[List[Prediction], int] 
        """
        pass


# data: List[ConfirmedTrip]
#
# for user in users:
#   time: int = get_latest_time(user)
#   data = preprocess.read_data_since_time(user, time)
#   
#   filtered_data = preprocess.filter_data(data, RADIUS)
#   if not valid:
#       ...
#   filepath = file_path(user)
#   model = c.load(filepath)
#   X = extract_features(filtered_data)
#   model.fit()
#   model.save(filepath)

