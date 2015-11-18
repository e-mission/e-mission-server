import emission.ce186.utility_model as ecum

import random

def make_random_user():
	name = str(random.randint())
	user = ecum.UserModel(name)
	utilites = ("sweat", "scenery", "social", "time", "noise", "crowded")
	for u in utilites:
		new_utility = random.randint(1, 101)
		user.increase_utility_by_n(u, new_utility)
	return user

def make_user_base(size):
	user_base = ecum.UserBase()
	for _ in xrange(size):
		user = make_random_user()
		user_base.add_user(user)
	return user_base

