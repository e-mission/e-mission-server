import logging
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
            "statemachine/transition": "transition",
            "config/sensor_config": "sensorconfig",
            "analysis/smoothing": "smoothresults"}

  @staticmethod
  def create_entry(user_id, key, data):
      result_entry = Entry()
      result_entry.user_id = user_id
      result_entry.metadata = ecwm.Metadata.create_metadata_for_result(key)
      result_entry.data = data
      result_entry._populateDependencies()
      return result_entry

  def _build(self, key, obj):
    # logging.debug("entry._build called with %s, %s, %s" % (self, key, obj))
    if key == "data":
        # We need to deserialize according to the type of data
        key_class = self._get_class(self._wrapper_type)
        return key_class._constructor(obj, self._configuration)
    else:
        return super(Entry, self)._build(key, obj)
