from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

class Untrackedtime(ecwt.Trip):
    """
    When we detect that we have a section of untracked time, we represent it
    by this structure. It is basically the "UNKNOWN" trip discussed in 
    https://github.com/e-mission/e-mission-server/issues/378#issuecomment-242959426
    This data structure is similar to a trip and represents the trip that was
    taken at some unknown time when tracking was off. But we don't know when
    that trip was and we don't know the route or the sections, so we represent
    it by a new data type.
    """
    props = ecwt.Trip.props

    def _populateDependencies(self):
        super(Untrackedtime, self)._populateDependencies()
