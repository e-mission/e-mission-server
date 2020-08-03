from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json

sample_path = "conf/net/ext_service/push.json.sample"
f = open(sample_path, "r")
data = json.loads(f.read())
f.close()

real_path = "conf/net/ext_service/push.json"
data['provider'] = 'firebase'
data['server_auth_token'] = 'firebase_api_key'
data['app_package_name'] = 'edu.berkeley.eecs.embase'
f = open(real_path, "w")
f.write(json.dumps(data))
f.close()
