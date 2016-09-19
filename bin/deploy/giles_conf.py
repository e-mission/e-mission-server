import json

sample_path = "conf/net/int_service/giles_conf.json.sample"
f = open(sample_path, "r")
data = json.loads(f.read())
f.close()

real_path = "conf/net/int_service/giles_conf.json"
data['giles_base_url'] = 'http://50.17.111.19:8079'
f = open(real_path, "w")
f.write(json.dumps(data))
f.close()
