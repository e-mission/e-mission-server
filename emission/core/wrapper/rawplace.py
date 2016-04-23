import logging
import emission.core.wrapper.place as ecwp
import emission.core.wrapper.wrapperbase as ecwb

# TODO: Remove this, since we are not going to store links from raw to cleaned.
# It is not clear what double links, as opposed to a clever query gives us, and
# using this model means that we cannot treat existing objects as read-only

class Rawplace(ecwp.Place):
    def _populateDependencies(self):
        super(Rawplace, self)._populateDependencies()
