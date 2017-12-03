from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

class Cleanedtrip(ecwt.Trip):
    props = ecwt.Trip.props
    props.update({"raw_trip": ecwb.WrapperBase.Access.WORM
                  })

    def _populateDependencies(self):
        super(Cleanedtrip, self)._populateDependencies()
