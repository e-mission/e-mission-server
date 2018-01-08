from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.get_database as edb
import emission.net.ext_service.habitica.create_party_leaders_script as lead

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    lead.create_party_leaders()
