from builtins import *
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

class Expectedtrip(ecwt.Trip):
    props = ecwt.Trip.props
    props.update({"raw_trip": ecwb.WrapperBase.Access.WORM,
                  "cleaned_trip": ecwb.WrapperBase.Access.WORM,
                  "inferred_labels": ecwb.WrapperBase.Access.WORM,
                  "inferred_trip": ecwb.WrapperBase.Access.WORM,
                  "expectation": ecwb.WrapperBase.Access.WORM,
                  "confidence_threshold": ecwb.WrapperBase.Access.WORM,
                  })

    def _populateDependencies(self):
        super(Expectedtrip, self)._populateDependencies()
