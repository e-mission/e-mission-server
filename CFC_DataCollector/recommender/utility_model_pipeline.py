"""
Construct user utility model or retrieve from database and update with
augmented trips. Store in database and return the model.
"""

from user_utility_model import UserUtilityModel

def build_user_model(user_id, augmented_trips):
  model = UserUtilityModel.find_from_db(user_id)

  if model:
    model.update(augmented_trips)
  else:
    model = UserUtilityModel(user_id, augmented_trips)

  model.store_in_db()
  return model
