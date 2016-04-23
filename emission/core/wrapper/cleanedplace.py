import emission.core.wrapper.place as ecwp
import emission.core.wrapper.wrapperbase as ecwb

class Cleanedplace(ecwp.Place):
    props = ecwp.Place.props
    props.update(
        {"raw_places": ecwb.WrapperBase.Access.WORM, # raw places that were combined to from this cleaned place
         "display_name": ecwb.WrapperBase.Access.WORM, # The human readable name for this place
         "osm_type": ecwb.WrapperBase.Access.WORM, # The open street map type for this place. we can use this to query for other geospatial information
         "osm_id": ecwb.WrapperBase.Access.WORM # The open street map id for this place. we can use this to query for other geospatial information
    })

    def _populateDependencies(self):
        super(Cleanedplace, self)._populateDependencies()