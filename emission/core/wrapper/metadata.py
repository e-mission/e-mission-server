import logging
import datetime as pydt
import emission.core.wrapper.wrapperbase as ecwb

class Metadata(ecwb.WrapperBase):
  props = {"key": ecwb.WrapperBase.Access.RO,
           "platform": ecwb.WrapperBase.Access.RO,
           "type": ecwb.WrapperBase.Access.RO,
           "write_ts": ecwb.WrapperBase.Access.RO,
           "write_dt": ecwb.WrapperBase.Access.RW,
           "time_zone": ecwb.WrapperBase.Access.RW,
           "write_fmt_time": ecwb.WrapperBase.Access.RW,
           "read_ts": ecwb.WrapperBase.Access.RO}

  enums = {}

  def _populateDependencies(self):
    pass

  def isAndroid(self):
    return self.platform == "android"

  def isIOS(self):
    return self.platform == "ios"
