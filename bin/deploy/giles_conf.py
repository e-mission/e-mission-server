import json

path = "conf/net/int_service/giles_conf.json"
f = open(path, "r")
data = json.loads(f.read())
f.close()

data['giles_base_url'] = 'http://50.17.111.19:8079'
f = open(path, "w")
f.write(json.dumps(data))
f.close()