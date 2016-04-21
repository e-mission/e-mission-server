import logging
import emission.core.wrapper.location as ecwl

class Recreatedlocation(ecwl.Location):
    props = {"mode": ecwb.WrapperBase.Access.WORM, # The mode associated with this point
             "section": ecwb.WrapperBase.Access.WORM, # The section that this point is associated with
            }

    def _populateDependencies(self):
        super(Recreatedlocation, self)._populateDependencies()
