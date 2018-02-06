from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import datetime as pydt
import emission.core.wrapper.wrapperbase as ecwb

class Smoothresults(ecwb.WrapperBase):
  props = {"section": ecwb.WrapperBase.Access.WORM, # the section for which these points should be deleted
           "deleted_points": ecwb.WrapperBase.Access.WORM, # list of IDs of deleted points
           "outlier_algo": ecwb.WrapperBase.Access.WORM, # the name of the algorithm used to generate outliers
           "filtering_algo": ecwb.WrapperBase.Access.WORM} # the name of the algorithm used to filter the points

  enums = {}
  geojson = []
  nullable = []
  local_dates = []

  def _populateDependencies(self):
    pass

