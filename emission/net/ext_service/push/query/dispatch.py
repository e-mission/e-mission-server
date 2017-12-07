from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import importlib

def get_query_fn(query_type):
    module_name = get_module_name(query_type) 
    logging.debug("module_name = %s" % module_name)
    module = importlib.import_module(module_name) 
    return getattr(module, "query")
   
def get_module_name(query_type):
    return "emission.net.ext_service.push.query.{type}".format(
        type=query_type)
