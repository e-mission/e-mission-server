import logging
import importlib

def convert_to_common_format(entry):
    format_fn = get_formatter(entry)
    return format_fn(entry)

def get_formatter(entry):
    module_name = get_module_name(entry.metadata) 
    logging.debug("module_name = %s" % module_name)
    module = importlib.import_module(module_name) 
    return getattr(module, "format")
   
def get_module_name(metadata):
    return "emission.net.usercache.formatters.{platform}.{key}".format(
        platform=metadata.platform,
        key=metadata.key.split("/")[1])
