from builtins import *
import emission.core.wrapper.confirmedtrip as ecwc
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.modeprediction as ecwm

class Compositetrip(ecwc.Confirmedtrip):
    props = ecwc.Confirmedtrip.props
    props.update({
                  "start_confirmed_place": ecwb.WrapperBase.Access.WORM, # object contains all properties for the source confirmed_place object
                  "end_confirmed_place": ecwb.WrapperBase.Access.WORM, # object contains all properties for the destination confirmed_place object
                  "confirmed_trip": ecwb.WrapperBase.Access.WORM, # the id of the corresponding confirmed trip
                  "locations": ecwb.WrapperBase.Access.WORM, # list containing cleaned location entries (max 100)
                  "sections": ecwb.WrapperBase.Access.WORM, # list containing cleaned sections during the trip
    })

    def _populateDependencies(self):
        super(Compositetrip, self)._populateDependencies()
