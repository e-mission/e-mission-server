import logging
import emission.core.wrapper.place as ecwp
import emission.core.wrapper.wrapperbase as ecwb

class Rawplace(ecwp.Place):
    props = ecwp.Place.props
    props.update({"cleaned_place": ecwb.WrapperBase.Access.WORM}) # cleaned place that was formed from this raw place

    def _populateDependencies(self):
        super(Rawplace, self)._populateDependencies()
