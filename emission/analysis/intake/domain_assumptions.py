import logging
import emission.core.wrapper.motionactivity as ecwm

def is_motorized(motion_type):
    return motion_type == ecwm.MotionTypes.IN_VEHICLE

def is_walking_type(motion_type):
    return motion_type == ecwm.MotionTypes.WALKING or \
           motion_type == ecwm.MotionTypes.ON_FOOT

def is_walking_speed(speed):
    """
    https://en.wikipedia.org/wiki/Preferred_walking_speed
    http://movement.osu.edu/papers/WalkRun_LongSrinivasan_PREPRINT2012.pdf
    (looks like ~ 1.7 at the outer range)
    """
    return not speed > 1.7

def is_bicycling_speed(speed):
    """
    From http://journals.sagepub.com/doi/pdf/10.1177/1687814015616918,
    non motorized bike max speed is 30 km/h = 8.333 m/s
    """
    return not speed > 8.5

def is_too_short_bicycle_ride(duration):
    if duration < 60:
        return True
    else:
        return False

def is_too_short_motorized_ride(duration):
    if duration < 5 * 60:
        return True
    else:
        return False

def is_too_short_bike_vehicle_transition(duration):
    if duration < 60:
        return True
    else:
        return False
