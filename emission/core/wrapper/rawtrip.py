from builtins import *
import logging
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

# TODO: Remove this, since we are not going to store links from raw to cleaned.
# It is not clear what double links, as opposed to a clever query gives us, and
# using this model means that we cannot treat existing objects as read-only

class Rawtrip(ecwt.Trip):
    def _populateDependencies(self):
        super(Rawtrip, self)._populateDependencies()
