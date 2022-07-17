from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.net.usercache.formatters.generic.one_time_survey as fgo

def format(entry):
    return fgo.format(entry)
