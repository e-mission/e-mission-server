from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from past.utils import old_div
import math
import logging
import numpy as np
import emission.core.common as ec
import emission.analysis.section_features as sf

def calDistance(point1, point2):
    return ec.calDistance([point1.longitude, point1.latitude], [point2.longitude, point2.latitude])

def calHeading(point1, point2):
    return sf.calHeading([point1.longitude, point1.latitude],
                    [point2.longitude, point2.latitude])

def calHC(point1, point2, point3):
    return sf.calHC([point1.longitude, point1.latitude],
                    [point2.longitude, point2.latitude],
                    [point3.longitude, point3.latitude])

def calSpeed(point1, point2):
    distanceDelta = calDistance(point1, point2)
    timeDelta = point2.ts - point1.ts
    # print "Distance delta = %s and time delta = %s" % (distanceDelta, timeDelta)
    # assert(timeDelta != 0)
    if (timeDelta == 0):
        # logging.debug("timeDelta = 0, distanceDelta = %s, returning speed = 0" % distanceDelta)
        if (distanceDelta > 0.01):
            # The float is back, but the error persists, specially on iOS data.
            # [-117.8805108778835, 33.89008340952731]
            # [-117.8805008149502, 33.88954523082551]
            # u'ts': 1466115972.178098,
            # u'ts': 1466115972.178098,
            # Distance between points ... 59.8499494256
            # happens fairly frequently actually
            # https://github.com/e-mission/e-mission-server/issues/407#issuecomment-248974661
            logging.warning("Distance between points %s, %s is %s, although the time delta = 0" %
                (point1, point2, distanceDelta))
            pass
            # assert(distanceDelta < 0.01)
        return 0

    return old_div(distanceDelta,timeDelta)
