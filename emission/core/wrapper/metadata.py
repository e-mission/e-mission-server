import logging
import datetime as pydt
import pytz
import time

import emission.core.wrapper.wrapperbase as ecwb

class Metadata(ecwb.WrapperBase):
  props = {"key": ecwb.WrapperBase.Access.WORM,
           "platform": ecwb.WrapperBase.Access.WORM,
           "type": ecwb.WrapperBase.Access.WORM,
           "write_ts": ecwb.WrapperBase.Access.WORM,
           "write_dt": ecwb.WrapperBase.Access.WORM,
           "time_zone": ecwb.WrapperBase.Access.WORM,
           "write_fmt_time": ecwb.WrapperBase.Access.WORM,
           "read_ts": ecwb.WrapperBase.Access.WORM}

  enums = {}
  geojson = []
  nullable = []

  def _populateDependencies(self):
    pass

  @staticmethod
  def create_metadata_for_result(key):
      m = Metadata()
      m.key = key
      m.platform = "server"
      m.write_ts = time.time()
      m.write_dt = pydt.datetime.utcfromtimestamp(m.write_ts).replace(tzinfo=pytz.utc)
      m.time_zone = "America/Los_Angeles"
      m.write_fmt_time = str(m.write_dt.astimezone(pytz.timezone(m.time_zone)))
      return m

  def isAndroid(self):
    return self.platform == "android"

  def isIOS(self):
    return self.platform == "ios"
