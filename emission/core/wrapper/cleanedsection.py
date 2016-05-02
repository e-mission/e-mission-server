import emission.core.wrapper.section as ecws
import emission.core.wrapper.wrapperbase as ecwb

class Cleanedsection(ecws.Section):
    props = ecws.Section.props
    props.update({
        "speeds": ecwb.WrapperBase.Access.WORM, # The speed profile for this section
        "distances": ecwb.WrapperBase.Access.WORM, # The distance profile for this section
        "distance": ecwb.WrapperBase.Access.WORM
    })

    def _populateDependencies(self):
        super(Cleanedsection, self)._populateDependencies()
