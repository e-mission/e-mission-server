from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import bson.objectid as boi

import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.metadata as ecwm

class Entry(ecwb.WrapperBase):
  props = {"metadata": ecwb.WrapperBase.Access.WORM,
           "data": ecwb.WrapperBase.Access.WORM,
           "_id": ecwb.WrapperBase.Access.WORM,
           "user_id": ecwb.WrapperBase.Access.WORM}

  enums = {}
  geojson = []
  nullable = []
  local_dates = []

  def _populateDependencies(self):
    # logging.debug("self = %s" % self)
    # logging.debug("metadata = %s" % self.metadata)
    # logging.debug("key = %s" % self.metadata.key)
    if "metadata" in self:
        wrapper_class_name = Entry._getData2Wrapper()[self.metadata.key]
        self._setattr("_wrapper_type", wrapper_class_name)

  @staticmethod
  def _getData2Wrapper():
    return {
            ### BEGIN: incoming data types ###
            # all location points from the phone
            "background/location": "location",
            # "valid" location points from the phone, after removing low-accuracy points
            "background/filtered_location": "location",
            # "motionactivity" results from the phone, indicating walk/bike or "motorized"
            "background/motion_activity": "motionactivity",
            # battery readings, to determine power drain empirically
            "background/battery": "battery",
            # transition events for the tracking finite state machine on the phone
            "statemachine/transition": "transition",
            # phone sensing configuration (e.g. sensing frequency, geofencing,...)
            "config/sensor_config": "sensorconfig",
            # phone sync configuration (sync interval,...)
            "config/sync_config": "syncconfig",
            # user consent time + protocol version
            "config/consent": "consentconfig",
            # webapp API call time, measured on the server
            "stats/server_api_time": "statsevent",
            # intended to log the occurrence of errors in the webapp
            "stats/server_api_error": "statsevent",
            # pipeline stage time, measured on the server
            "stats/pipeline_time": "statsevent",
            # intended to log the occurrence of errors in the pipeline
            "stats/pipeline_error": "statsevent",
            # time for various client operations, measured on the client
            # comparison with the server_api_time can help debug networking issues
            "stats/client_time": "statsevent",
            # events, such as button presses, on the client
            "stats/client_nav_event": "statsevent",
            # errors detected on the client. Again, can be correlated with
            # server calls to debug networking issues
            "stats/client_error": "statsevent",
            # incidents (smiley/frownie) reported by the user from the phone
            "manual/incident": "incident",
            # user confirmation of the travel mode, potentially selected from a
            # rich set of travel modes that cannot be detected using sensors
            "manual/mode_confirm": "userlabel",
            # user confirmation of the travel purpose
            "manual/purpose_confirm": "userlabel",
            #user response to the survey questions
            "manual/survey_response": "userlabel",
            # user survey results. The survey results are a JSON blob
            "manual/survey": "survey",
            # user confirmation of the destination (unsure how this will
            # interact with purpose
            "manual/destination_confirm": "userlabel",
            ### END: incoming data types ###
            ### BEGIN: analysis result data types ###
            ### ** BEGIN: objects generated after the initial segmentation step **
            # trips from one place to another
            "segmentation/raw_trip": "rawtrip",
            # places between trips
            "segmentation/raw_place": "rawplace",
            # sections within a trip (e.g. walk -> bus -> walk has 3 sections)
            "segmentation/raw_section": "section",
            # stops between sections
            "segmentation/raw_stop": "stop",
            # untracked time (e.g. when phone was out of battery)
            "segmentation/raw_untracked": "untrackedtime",
            ### ** END: objects generated after the initial segmentation step **
            # object indicating which points need to be removed from the trajectory
            # because they represent zig-zags
            "analysis/smoothing": "smoothresults",
            ### ** BEGIN: objects generated after the second cleaned segmentation step
            ### same explanations as the corresponding segmentation/* objects
            "analysis/cleaned_trip": "cleanedtrip",
            "analysis/cleaned_place": "cleanedplace",
            "analysis/cleaned_section": "cleanedsection",
            "analysis/cleaned_stop": "stop",
            "analysis/cleaned_untracked": "untrackedtime",
            # Resampled locations to ensure that the point density is
            # consistent across operating systems and sampling frequencies
            "analysis/recreated_location": "recreatedlocation",
            ### ** END: objects generated after the second cleaned segmentation step
            ### ** BEGIN: metric outputs. These are not currently stored
            ### they are generated on demand instead
            "metrics/daily_user_count": "modestattimesummary",
            "metrics/daily_mean_count": "modestattimesummary",
            "metrics/daily_user_distance": "modestattimesummary",
            "metrics/daily_mean_distance": "modestattimesummary",
            "metrics/daily_user_duration": "modestattimesummary",
            "metrics/daily_mean_duration": "modestattimesummary",
            "metrics/daily_user_median_speed": "modestattimesummary",
            "metrics/daily_mean_median_speed": "modestattimesummary",
            ### ** END: metric outputs.
            ### ** BEGIN: prediction objects
            # the generated model for the random forest based mode inference
            # saved so that it can be used for prediction without retraining
            "mode_inference/model": "modeinfermodel",
            # the predicted mode for a particular section (one entry per algorithm)
            "inference/prediction": "modeprediction",
            # the predicted labels for a particular trip (one entry per algorithm)
            "inference/labels": "labelprediction",
            # equivalent of cleaned_section, but with the mode set to the 
            # inferred mode instead of just walk/bike/motorized
            # used for consistency and to make the client work whether or not we were
            # the final inferred section mode (possibly an ensemble result)
            "analysis/inferred_section": "inferredsection",
            # the final inferred label data structure (possibly an ensemble result)
            "analysis/inferred_labels": "labelprediction",
            ### ** END: prediction objects
            ### ** BEGIN: confirmed objects which combine inferred and user input values
            "analysis/inferred_trip": "inferredtrip",
            "analysis/expected_trip": "expectedtrip",
            "analysis/confirmed_trip": "confirmedtrip",
            "analysis/confirmed_section": "confirmedsection"
            ### ** END: confirmed objects which combine inferred and user input values
            }

  @staticmethod
  def create_entry(user_id, key, data, create_id = False):
      # type: (UUID, string, emission.core.wrapper.WrapperBase) -> emission.core.wrapper.Entry
      result_entry = Entry()
      result_entry['_id'] = boi.ObjectId()
      result_entry.user_id = user_id
      result_entry.metadata = ecwm.Metadata.create_metadata_for_result(key)
      result_entry.data = data
      result_entry._populateDependencies()
      return result_entry

  @staticmethod
  def create_fake_entry(user_id, key, data, write_ts, create_id=False):
      """Method used to create Synthetic entries"""
      
      result_entry = Entry()
      result_entry['_id'] = boi.ObjectId()
      result_entry.user_id = user_id
      result_entry.metadata = ecwm.Metadata.create_metadata_for_fake_result(key, write_ts)
      result_entry.data = data
      result_entry._populateDependencies()
      return result_entry

  @staticmethod
  def get_dedup_list(key):
      key_class = ecwb.WrapperBase._get_class(Entry._getData2Wrapper()[key])
      all_keys = list(key_class.props.keys())
      valid_keys = [item for item in all_keys if item not in key_class.geojson
                    and item not in key_class.local_dates]
      return valid_keys

  def _build(self, key, obj):
    # logging.debug("entry._build called with %s, %s, %s" % (self, key, obj))
    if key == "data":
        # We need to deserialize according to the type of data
        key_class = self._get_class(self._wrapper_type)
        return key_class._constructor(obj, self._configuration)
    else:
        return super(Entry, self)._build(key, obj)
