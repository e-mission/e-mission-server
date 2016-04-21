import logging
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.wrapperbase as ecwb

class Rawtrip(ecwt.Trip):
    props = ecwt.Trip.props
    props.update({"cleaned_trip": ecwb.WrapperBase.Access.WORM})
    nullable = ecwt.Trip.nullable.append("cleaned_trip")

    def _populateDependencies(self):
        super(Rawtrip, self)._populateDependencies()
