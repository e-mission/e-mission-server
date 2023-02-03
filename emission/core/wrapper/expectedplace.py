from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.core.wrapper.place as ecwp
import emission.core.wrapper.wrapperbase as ecwb

class Expectedplace(ecwp.Place):
    props = ecwp.Place.props
    props.update({"raw_place": ecwb.WrapperBase.Access.WORM,
                  "cleaned_place": ecwb.WrapperBase.Access.WORM,
                  "inferred_labels": ecwb.WrapperBase.Access.WORM,
                  "inferred_place": ecwb.WrapperBase.Access.WORM,
                  "expectation": ecwb.WrapperBase.Access.WORM,
                  "confidence_threshold": ecwb.WrapperBase.Access.WORM,
                  })

    def _populateDependencies(self):
        super(Expectedplace, self)._populateDependencies()
