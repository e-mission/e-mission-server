import logging
import emission.core.wrapper.wrapperbase as ecwb

class Entry(ecwb.WrapperBase):
  props = {"metadata": ecwb.WrapperBase.Access.RO,
           "data": ecwb.WrapperBase.Access.RO,
           "_id": ecwb.WrapperBase.Access.RO,
           "user_id": ecwb.WrapperBase.Access.RO}

  enums = {}
  geojson = []

  def _populateDependencies(self):
    # logging.debug("self = %s" % self)
    # logging.debug("metadata = %s" % self.metadata)
    # logging.debug("key = %s" % self.metadata.key)
    wrapper_class_name = self._getData2Wrapper()[self.metadata.key]
    self._setattr("_wrapper_type", wrapper_class_name)

  def _getData2Wrapper(self):
    return {"background/location": "location",
            "background/filtered_location": "location",
            "background/motion_activity": "motionactivity"}

  def _build(self, key, obj):
    # logging.debug("entry._build called with %s, %s, %s" % (self, key, obj))
    if key == "data":
        # We need to deserialize according to the type of data
        key_class = self._get_class(self._wrapper_type)
        return key_class._constructor(obj, self._configuration)
    else:
        return super(Entry, self)._build(key, obj)
