import logging
import datetime as pydt
import emission.core.wrapper.wrapperbase as ecwb

class Metadata(ecwb.WrapperBase):
  props = {"key": ecwb.WrapperBase.Access.RO,
           "platform": ecwb.WrapperBase.Access.RO,
           "type": ecwb.WrapperBase.Access.RO,
           "write_ts": ecwb.WrapperBase.Access.RO,
           "write_dt": ecwb.WrapperBase.Access.RW,
           "write_ts_formatted": ecwb.WrapperBase.Access.RW,
           "read_ts": ecwb.WrapperBase.Access.RO}

  def _populateDependencies(self):
    if self.write_ts is not None:
      self.write_dt = pydt.datetime.fromtimestamp(self.write_ts)
      self.write_ts_formatted = pydt.datetime.fromtimestamp(self.write_ts)

  def isAndroid(self):
    return self.platform == "android"

  def isIOS(self):
    return self.platform == "ios"
