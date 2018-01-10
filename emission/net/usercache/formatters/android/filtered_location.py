from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.net.usercache.formatters.android.location as fal

def format(entry):
    return fal.format(entry)
