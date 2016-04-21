import logging
import emission.core.wrapper.trip as ecwt

class Cleanedtrip(ecwt.Trip):
    props = {"raw_trip": ecwb.WrapperBase.Access.WORM}

    def _populateDependencies(self):
        self._setattr("type", ecwt.TripTypes.CLEANED)
