from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.place as ecwp
import emission.core.wrapper.wrapperbase as ecwb

class Confirmedplace(ecwp.Place):
    props = ecwp.Place.props
    props.update({"raw_place": ecwb.WrapperBase.Access.WORM,
                  "cleaned_place": ecwb.WrapperBase.Access.WORM,
# the user input will have all `manual/*` entries
# let's make that be somewhat flexible instead of hardcoding into the data model
                  "user_input": ecwb.WrapperBase.Access.WORM,
                  "additions": ecwb.WrapperBase.Access.WORM
                  })

    def _populateDependencies(self):
        super(Confirmedplace, self)._populateDependencies()
