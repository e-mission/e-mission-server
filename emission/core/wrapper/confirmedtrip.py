from builtins import *
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

class Confirmedtrip(ecwt.Trip):
    props = ecwt.Trip.props
    props.update({"raw_trip": ecwb.WrapperBase.Access.WORM,
                  "cleaned_trip": ecwb.WrapperBase.Access.WORM,
                  "inferred_labels": ecwb.WrapperBase.Access.WORM,
                  "inferred_trip": ecwb.WrapperBase.Access.WORM,
                  "expectation": ecwb.WrapperBase.Access.WORM,
                  "confidence_threshold": ecwb.WrapperBase.Access.WORM,
                  "expected_trip": ecwb.WrapperBase.Access.WORM,
                  "inferred_section_summary": ecwb.WrapperBase.Access.WORM,
                  "cleaned_section_summary": ecwb.WrapperBase.Access.WORM,
                  "ble_sensed_summary": ecwb.WrapperBase.Access.WORM,
# the user input will have all `manual/*` entries
# let's make that be somewhat flexible instead of hardcoding into the data model
                  "user_input": ecwb.WrapperBase.Access.WORM,
                  "additions": ecwb.WrapperBase.Access.WORM
                  })

    def _populateDependencies(self):
        super(Confirmedtrip, self)._populateDependencies()
