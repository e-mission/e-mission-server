from enum import Enum


class ModelType(Enum):
    GREEDY_SIMILARITY_BINNING = 'greedy'
    
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