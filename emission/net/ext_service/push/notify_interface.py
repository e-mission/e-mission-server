# Standard imports
import json

# Note that the URL is hardcoded because the API endpoints are not standardized.
# If we change a push provider, we will need to modify to match their endpoints.
# Hardcoding will remind us of this :)
# We can revisit this if push providers eventually decide to standardize...

try:
    key_file = open('conf/net/ext_service/push.json')
    key_data = json.load(key_file)
except:
    logging.warning("push service not configured, push notifications not supported")

class NotifyInterfaceFactory(self):
    def getNotifyInterface(pushProvider):
        module_name = "emission.net.ext_service.push.notify_interface_impl.%s" % pushProvider
        logging.debug("module_name = %s" % module_name)
        module = importlib.import_module(module_name)
        logging.debug("module = %s" % module)
        interface_obj_fn = getattr(module, "get_interface")
        logging.debug("interface_obj_fn = %s" % interface_obj_fn)
        interface_obj = interface_obj_fn()
        logging.debug("interface_obj = %s" % interface_obj)
        return interface_obj

class NotifyInterface:
    def get_and_invalidate_entries(self):
        pass

    def send_visible_notification(self, token_list, title, message, json_data, dev=False):
        pass

    def send_silent_notification(self, token_list, title, message, json_data, dev=False):
        pass

