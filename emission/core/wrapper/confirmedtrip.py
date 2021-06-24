from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

class Confirmedtrip(ecwt.Trip):
    props = ecwt.Trip.props
    props.update({"raw_trip": ecwb.WrapperBase.Access.WORM,
                  "cleaned_trip": ecwb.WrapperBase.Access.WORM,
# the confirmed section that is the "primary"
# https://github.com/e-mission/e-mission-docs/issues/476#issuecomment-738120752
                  "primary_section": ecwb.WrapperBase.Access.WORM,
                  "inferred_primary_mode": ecwb.WrapperBase.Access.WORM,
# the user input will have all `manual/*` entries
# let's make that be somewhat flexible instead of hardcoding into the data model
                  "user_input": ecwb.WrapperBase.Access.WORM
                  })

    def _populateDependencies(self):
        super(Confirmedtrip, self)._populateDependencies()
