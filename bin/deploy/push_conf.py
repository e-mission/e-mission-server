import json

sample_path = "conf/net/ext_service/push.json.sample"
f = open(sample_path, "r")
data = json.loads(f.read())
f.close()

real_path = "conf/net/ext_service/push.json"
data['provider'] = 'ionic'
data['server_auth_token'] = 'firebase_api_key'
f = open(real_path, "w")
f.write(json.dumps(data))
f.close()
