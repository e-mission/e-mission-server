from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.location as ecwl
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.wrapperbase as ecwb

class Recreatedlocation(ecwl.Location):
    props = ecwl.Location.props
    props.update({
        "mode": ecwb.WrapperBase.Access.WORM, # The mode associated with this point
        "section": ecwb.WrapperBase.Access.WORM, # The section that this point belongs to
        "idx": ecwb.WrapperBase.Access.WORM # The index of this point in this section
    })
    enums = ecwl.Location.enums
    enums.update({"mode": ecwm.MotionTypes})
    def _populateDependencies(self):
        super(Recreatedlocation, self)._populateDependencies()
