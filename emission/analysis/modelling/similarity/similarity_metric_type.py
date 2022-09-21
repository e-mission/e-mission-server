from __future__ import annotations
import enum


import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.analysis.modelling.similarity.similarity_metric as eamss

class SimilarityMetricType(enum.Enum):
    OD_SIMILARITY = 0

    def build(self) -> eamss.SimilarityMetric:
        """
        
        hey YOU! add future similarity metric types here please!

        :raises KeyError: if the SimilarityMetricType isn't found in the below dictionary
        :return: the associated similarity metric
        """
        metrics = {
            SimilarityMetricType.OD_SIMILARITY: eamso.OriginDestinationSimilarity()
        }

        metric = metrics.get(self)
        if metric is None:
            names = "{" + ",".join(SimilarityMetricType.names) + "}"
            msg = f"unknown metric type {metric}, must be one of {names}"
            raise KeyError(msg)
        else:
            return metric


    @classmethod
    def names(cls):
        return list(map(lambda e: e.name, list(cls)))

    @classmethod
    def from_str(cls, str):
        """attempts to match the provided string to a known SimilarityMetricType.
        not case sensitive.

        :param str: a string name of a SimilarityMetricType
        """
        try:
            str_caps = str.upper()
            return cls[str_caps]
        except KeyError:
            names = "{" + ",".join(cls.names) + "}"
            msg = f"{str} is not a known SimilarityMetricType, must be one of {names}"
            raise KeyError(msg)