from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.cleanedsection as ecwc
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.modeprediction as ecwm

class Confirmedsection(ecwc.Cleanedsection):
    props = ecwc.Cleanedsection.props
    # for a detailed explanation of what the various modes represent,
    # see https://github.com/e-mission/e-mission-docs/issues/476
    props.update({"cleaned_section": ecwb.WrapperBase.Access.WORM,
                  "inferred_section": ecwb.WrapperBase.WORM,
                  "inferred_mode": ecwb.WrapperBase.WORM, # inferred by mode inference algo
                  "confirmed_mode": ecwb.WrapperBase.WORM, # confirmed by user
# mode to be used for analysis; confirmed mode if we know factors for it, inferred mode otherwise
                  "analysis_mode": ecwb.WrapperBase.WORM,
# mode for user display; inferred mode if not confirmed; confirmed mode otherwise
                  "display_mode": ecwb.WrapperBase.WORM
                  })

    enums = {"inferred_mode": ecwm.PredictedModeTypes,
             "analysis_mode": ecwm.PredictedModeTypes}

    def _populateDependencies(self):
        super(ecwc.Confirmedsection, self)._populateDependencies()
