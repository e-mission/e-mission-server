import logging
import emission.core.wrapper.wrapperbase as ecwb

class TourModel(obecwb.WrapperBase):

    props = {"user_id" : ecwb.WrapperBase.Access.WORM} # user_id of the E-Missions user the graph represnts 

    def _populateDependencies(self):
        pass
