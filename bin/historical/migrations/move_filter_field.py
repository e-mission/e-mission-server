from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import emission.storage.timeseries.format_hacks.move_filter_field as estfm

if __name__ == '__main__':
    estfm.move_all_filters_to_data()
