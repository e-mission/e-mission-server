from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.userinput as ecwui
import emission.core.wrapper.wrapperbase as ecwb

class Placeuserinput(ecwui.Userinput):
    props = ecwui.Userinput.props
    props.update({"start_ts": ecwb.WrapperBase.Access.RO,    # geojson representation of the point
             "start_local_dt": ecwb.WrapperBase.Access.RO, # start datetime in local time
             "start_fmt_time": ecwb.WrapperBase.Access.RO, # start formatted time (in timezone of point)
             "end_ts": ecwb.WrapperBase.Access.WORM, # end UTC timestamp (in secs)
             "end_local_dt": ecwb.WrapperBase.Access.RO, # end datetime in local time
             "end_fmt_time": ecwb.WrapperBase.Access.RO, # end formatted time (in timezone of point)
    })

    enums = {}
    geojson = []
    nullable = []
    local_dates = ["start_local_dt", "end_local_dt"]

    def _populateDependencies(self):
        pass
