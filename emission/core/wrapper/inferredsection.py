from builtins import *
import emission.core.wrapper.cleanedsection as ecwc
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.modeprediction as ecwm

class Inferredsection(ecwc.Cleanedsection):
    props = ecwc.Cleanedsection.props
    props.update({"cleaned_section": ecwb.WrapperBase.Access.WORM
                  })

    enums = {"sensed_mode": ecwm.PredictedModeTypes}

    def _populateDependencies(self):
        super(ecwc.Cleanedsection, self)._populateDependencies()
