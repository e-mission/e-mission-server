# Standard imports
import math
import logging
import numpy as np
import emission.core.common as ec

def calSpeed(point1, point2):
    distanceDelta = ec.calDistance([point1.mLongitude, point1.mLatitude], [point2.mLongitude, point2.mLatitude])
    timeDelta = point2.mTime - point1.mTime
    # print "Distance delta = %s and time delta = %s" % (distanceDelta, timeDelta)
    # assert(timeDelta != 0)
    if (timeDelta == 0):
        logging.debug("timeDelta = 0, distanceDelta = %s, returning speed = 0")
        assert(distanceDelta < 0.01)
        return 0

    # TODO: Once we perform the conversions from ms to secs as part of the
    # usercache -> timeseries switch, we need to remove this division by 1000
    return distanceDelta/(float(timeDelta)/1000)
