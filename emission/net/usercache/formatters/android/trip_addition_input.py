from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.net.usercache.formatters.generic.userlabel as fgl

def format(entry):
    import arrow

    entry["data"]["start_ts"] = 0
    entry["data"]["end_ts"] = arrow.get()
    return fgl.format(entry)