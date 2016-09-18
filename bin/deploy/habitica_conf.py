import json

sample_path = "conf/net/ext_service/habitica.json.sample"
f = open(sample_path, "r")
data = json.loads(f.read())
f.close()

real_path = "conf/net/ext_service/habitica.json"
data['url'] = 'https://em-game.eecs.berkeley.edu'
f = open(real_path, "w")
f.write(json.dumps(data))
f.close()
