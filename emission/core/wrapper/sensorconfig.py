from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Sensorconfig(ecwb.WrapperBase):
    props = {"is_duty_cycling": ecwb.WrapperBase.Access.RO,  # latitude of the point
             "simulate_user_interaction": ecwb.WrapperBase.Access.RO, # Simulate user interaction through generating notifications on every sync
             "accuracy": ecwb.WrapperBase.Access.RO, # longitude of the point
             "accuracy_threshold": ecwb.WrapperBase.Access.RO, # threshold for valid accuracies
             "filter_distance": ecwb.WrapperBase.Access.RO, # distance for the distance filter (unused on android)
             "filter_time": ecwb.WrapperBase.Access.RO,     # time interval for the time filter (unused on iOS)
             "geofence_radius": ecwb.WrapperBase.Access.RO, # radius for the geofence if we are duty cycling
             "trip_end_stationary_mins": ecwb.WrapperBase.Access.RO,  # time after which we detect that a trip has ended
             "ios_use_visit_notifications_for_detection": ecwb.WrapperBase.Access.RO,  # iOS only: whether we should use visit notifications or manual checks for detecting trip ends
             "ios_use_remote_push_for_sync": ecwb.WrapperBase.Access.RO,  # iOS only: whether we should use remote push for sync
             "android_geofence_responsiveness": ecwb.WrapperBase.Access.RO} # android only: geofence responsiveness
    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
