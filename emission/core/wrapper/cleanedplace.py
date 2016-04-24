import emission.core.wrapper.place as ecwp
import emission.core.wrapper.wrapperbase as ecwb

class Cleanedplace(ecwp.Place):
    props = ecwp.Place.props
    props.update(
        {"raw_places": ecwb.WrapperBase.Access.WORM, # raw places that were combined to from this cleaned place
         "display_name": ecwb.WrapperBase.Access.WORM # The human readable name for this place
    })

    def _populateDependencies(self):
        super(Cleanedplace, self)._populateDependencies()

    def append_raw_place(self, place_id):
        self["raw_places"].append(place_id)