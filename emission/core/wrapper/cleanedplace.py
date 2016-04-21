import logging
import emission.core.wrapper.place as ecwp

class Cleanedplace(ecwp.Place):
    props = {"raw_places": ecwb.WrapperBase.Access.WORM, # raw places that were combined to from this cleaned place
            }

    def _populateDependencies(self):
        super(Cleanedplace, self)._populateDependencies()
