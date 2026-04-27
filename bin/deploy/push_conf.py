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
data['ios_token_format'] = 'apns'
f = open(real_path, "w")
f.write(json.dumps(data))
f.close()
