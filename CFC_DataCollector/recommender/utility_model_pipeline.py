"""
Construct user utility model or retrieve from database and update with
augmented trips. Store in database and return the model.
"""

from user_utility_model import UserUtilityModel

def get_training_trips(user_id, filter_function = None):
    #TODO: IMPLEMENT ME
    return []

def build_user_model(user_id, augmented_trips):
  model = UserUtilityModel.find_from_db(user_id)

  if model:
    model.update(augmented_trips)
  else:
    model = UserUtilityModel(augmented_trips)

  model.store_in_db()
  return model

# We have two options for recommendation: adjusting user utility model,
# incorporating factors such as emissions, or adjusting user trips,
# perhaps finding a trip that meets the user
def modify_user_utility_model(user_id, utility_model):
    # change utility model,
    # TODO: IMPLEMENT ME
    return UserUtilityModel(user_id, None)

def modify_trips(trip_list):
    #TODO: IMPLEMENT ME
    return []



