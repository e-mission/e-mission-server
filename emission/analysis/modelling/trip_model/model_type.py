from __future__ import annotations
from enum import Enum
import emission.analysis.modelling.trip_model.trip_model as eamuu
import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamug
import emission.analysis.modelling.trip_model.forest_classifier as eamuf


SIMILARITY_THRESHOLD_METERS=500


class ModelType(Enum):
    # ENUM_NAME_CAPS = 'SHORTHAND_NAME_CAPS'
    GREEDY_SIMILARITY_BINNING = 'GREEDY'
    RANDOM_FOREST_CLASSIFIER = 'FOREST'
    
    def build(self, config=None) -> eamuu.TripModel:
        """
        instantiates the requested user model type with the configured
        parameters. 
        
        hey YOU! if future model types are created, they should be added here!

        :param model_type: internally-used model name (an enum)
        :return: a user label prediction model
        :raises KeyError: if the requested model name does not exist
        """
        # Dict[ModelType, TripModel]
        MODELS = {                
                ModelType.GREEDY_SIMILARITY_BINNING: eamug.GreedySimilarityBinning,
                ModelType.RANDOM_FOREST_CLASSIFIER: eamuf.ForestClassifierModel
                }    
        model = MODELS.get(self)
        if model is None:
            available_models = ', '.join([ e.name for e in ModelType])
            raise KeyError(f"ModelType {self.name} not found in factory, Available models are {available_models}."\
                           "Otherwise please add new model to build method")
        return model(config)

    @classmethod
    def names(cls):
        return list(map(lambda e: e.name, list(cls)))

    @property
    def model_name(self):
        """
        used in filenames, database tables, etc. should be
        a POSIX-compliant name.

        when adding new model types, this should be set on the
        right-hand side of the enum, above.

        :return: a simple name for this model type
        :rtype: str
        """
        return self.value

    @classmethod
    def from_str(cls, str):
        """attempts to match the provided string to a known ModelType
        since a short name is 'nicer', we attempt to match on the enum
        value first (for example, 'greedy'). as a fallback, we attempt
        to match on the full ModelType name (for example, 
        'GREEDY_SIMILARITY_BINNING'). not case sensitive.

        :param str: a string name of a ModelType
        """
        try:
            str_caps = str.upper()
            return cls(str_caps)
        except ValueError:
            try:
                return cls[str_caps]
            except KeyError:
                names_list = '{' + ','.join(cls.names) + '}'
                msg = f'{str} is not a known ModelType, should be one of {names_list}'
                raise KeyError(msg)