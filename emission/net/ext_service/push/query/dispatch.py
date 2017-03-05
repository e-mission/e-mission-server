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
