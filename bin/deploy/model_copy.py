import json

sample_path = "emission/tests//data/seed_model_from_test_data.json"
f = open(sample_path, "r")
data = json.loads(f.read())
f.close()

real_path = "./seed_model.json"
f = open(real_path, "w")
f.write(json.dumps(data))
f.close()
