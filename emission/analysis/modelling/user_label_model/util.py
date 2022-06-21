from typing import Dict, List, Optional, Tuple
import jsonpickle as jpickle
import logging
from past.utils import old_div
import numpy
from numpy.linalg import norm

import emission.storage.decorations.analysis_timeseries_queries as esda


def load_fs(filename: str, numpy_decode: bool = True) -> Optional[dict]:
    """loads model state as a pickled object on the file system.
    if the file is not found, returns an empty dict.

    :param filename: file name to load
    :type filename: str
    :param numpy_decode: if part of the data is numpy encoded
    :type numpy_decode: bool
    :return: json object parsed, or, an empty list
    :rtype: Dict
    """
    raise Exception("deprecated, use db instead")
    logging.debug(f"At stage: loading model")
    try:
        with open(filename, "r") as f:
            contents = f.read()
    except FileNotFoundError:
        logging.info(f"No model found at {filename}, no prediction")
        return None
    
    try:
        if numpy_decode:
            # see https://jsonpickle.github.io/extensions.html
            import jsonpickle.ext.numpy as jsonpickle_numpy
            jsonpickle_numpy.register_handlers()
        result = jpickle.loads(contents)
        return result
    except Exception as e:
        msg = (
            f"failure decoding stored model at {filename}, "
            f"numpy_decode={numpy_decode}"
        )
        raise IOError(msg) from e


def save_fs(filename: str, obj: object):
    """save model state as a pickled object on the file system

    :param filename: filename to write
    :type filename: str
    :param obj: the object to pickle + store
    :type obj: object

    """
    raise Exception("deprecated, use db instead")
    try:
        logging.debug("At stage: saving model")
        obj_capsule = jpickle.dumps(obj)
        with open(filename, "w") as fd:
            fd.write(obj_capsule)
    except Exception as e:
        msg = f"failed writing clustering model contents to file system"
        raise IOError(msg) from e
        

def find_knee_point(values: List[float]) -> Tuple[float, int]:
    """for a list of values, find the value which represents the cut-off point
    or "elbow" in the function when values are sorted.

    based on this stack overflow answer: https://stackoverflow.com/a/2022348/4803266
    And summarized by the statement: "A quick way of finding the elbow is to draw a
    line from the first to the last point of the curve and then find the data point
    that is farthest away from that line."

    :param values: list of values from which to select a cut-off
    :type values: List[float]
    :return: the index and value to use as a cutoff
    :rtype: Tuple[int, float]
    """
    N = len(values)
    x = list(range(N))
    max = 0
    index = -1
    a = numpy.array([x[0], values[0]])
    b = numpy.array([x[-1], values[-1]])
    n = norm(b - a)
    new_y = []
    for i in range(0, N):
        p = numpy.array([x[i], values[i]])
        dist = old_div(norm(numpy.cross(p - a, p - b)), n)
        new_y.append(dist)
        if dist > max:
            max = dist
            index = i
    value = values[index]
    return [index, value]
