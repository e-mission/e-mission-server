# Standard imports
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
            # While converting ms -> secs on the server, we were treating the ms as a int.
            # This meant that we frequently got the same value, specially with fast sampling.
            # Let us turn off the assert and log an error message for now.
            # Once the float is back, we can remove this and see if it recurs
            logging.error("Distance between points %s, %s is %s, although the time delta = 0" %
                (point1, point2, distanceDelta))
            # assert(distanceDelta < 0.01)
        return 0

    return distanceDelta/timeDelta
