import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

class Cleanedtrip(ecwt.Trip):
    props = ecwt.Trip.props
    props.update({"raw_trip": ecwb.WrapperBase.Access.WORM,
                  "distance": ecwb.WrapperBase.Access.WORM,
                  })

    def _populateDependencies(self):
        super(Cleanedtrip, self)._populateDependencies()