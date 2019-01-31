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
    return {"background/location": "location",
            "background/filtered_location": "location",
            "background/motion_activity": "motionactivity",
            "background/battery": "battery",
            "statemachine/transition": "transition",
            "config/sensor_config": "sensorconfig",
            "config/sync_config": "syncconfig",
            "config/consent": "consentconfig",
            "stats/server_api_time": "statsevent",
            "stats/server_api_error": "statsevent",
            "stats/pipeline_time": "statsevent",
            "stats/pipeline_error": "statsevent",
            "stats/client_time": "statsevent",
            "stats/client_nav_event": "statsevent",
            "stats/client_error": "statsevent",
            "manual/incident": "incident",
            "manual/mode_confirm": "userlabel",
            "manual/purpose_confirm": "userlabel",
            "segmentation/raw_trip": "rawtrip",
            "segmentation/raw_place": "rawplace",
            "segmentation/raw_section": "section",
            "segmentation/raw_stop": "stop",
            "segmentation/raw_untracked": "untrackedtime",
            "analysis/smoothing": "smoothresults",
            "analysis/cleaned_trip": "cleanedtrip",
            "analysis/cleaned_place": "cleanedplace",
            "analysis/cleaned_section": "cleanedsection",
            "analysis/cleaned_stop": "stop",
            "analysis/cleaned_untracked": "untrackedtime",
            "analysis/recreated_location": "recreatedlocation",
            "metrics/daily_user_count": "modestattimesummary",
            "metrics/daily_mean_count": "modestattimesummary",
            "metrics/daily_user_distance": "modestattimesummary",
            "metrics/daily_mean_distance": "modestattimesummary",
            "metrics/daily_user_duration": "modestattimesummary",
            "metrics/daily_mean_duration": "modestattimesummary",
            "metrics/daily_user_median_speed": "modestattimesummary",
            "metrics/daily_mean_median_speed": "modestattimesummary",
            "mode_inference/model": "modeinfermodel",
            "inference/prediction": "modeprediction",
            "analysis/inferred_section": "inferredsection",
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
