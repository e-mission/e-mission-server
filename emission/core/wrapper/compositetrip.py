from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.confirmedtrip as ecwc
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.modeprediction as ecwm

class Compositetrip(ecwc.Confirmedtrip):
    props = ecwc.Confirmedtrip.props
    props.update({#      # confirmedplace stuff
                  "end_confirmed_place": ecwb.WrapperBase.Access.WORM, # object contains all properties for the destination confirmed_place object
                  "locations": ecwb.WrapperBase.Access.WORM, # object containing cleaned location entries (max 100)
                  #      # sections stuff
                  "cleaned_section": ecwb.WrapperBase.Access.WORM,
                  "inferred_section": ecwb.WrapperBase.Access.WORM,
                  "inferred_mode": ecwb.WrapperBase.Access.WORM, # inferred by mode inference algo
                  "confirmed_mode": ecwb.WrapperBase.Access.WORM, # confirmed by user
# mode to be used for analysis; confirmed mode if we know factors for it, inferred mode otherwise
                  "analysis_mode": ecwb.WrapperBase.Access.WORM,
# mode for user display; inferred mode if not confirmed; confirmed mode otherwise
                  "display_mode": ecwb.WrapperBase.Access.WORM
    })
    #      # sections stuff
    enums = {"inferred_mode": ecwm.PredictedModeTypes,
             "analysis_mode": ecwm.PredictedModeTypes}

    def _populateDependencies(self):
        super(Compositetrip, self)._populateDependencies()
