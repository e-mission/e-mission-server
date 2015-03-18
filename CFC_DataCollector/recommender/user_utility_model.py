# Phase 1: Build a model for User Utility Function (per Vij, Shankari)
# First, for each trip, we must obtain alternatives through some method
# (currently Google Maps API), alongside the actual trips which were taken.
# Once we have these alternatives, we can extract the features from each of the
# possible trips. Now, we utilize logistic regression with either the simple
# model given by Vij and Shankari, or utilizing their more complicated method.
# At the end, we want user-specific weights for their trip utility function.

# Phase 2: Choose optimal future trips
# Once we have determined a utility function for the user, we can evaluate
# future trips through this metric. This should look similar to our Phase 1,
# where we can obtain a set of alternative routes, and then choose an optimal
# trip based on the user's utility function. However, the key difference is that
# these trips are no longer static--in Phase 1, we can augment each trip with a
# set of alternative routes. In Phase 2, this learning is time-dependent (think traffic).

from get_database import get_utility_model_db
from sklearn import linear_model as lm
from datetime import datetime
from common import calc_car_cost

class UserUtilityModel:
  # TO DO:
  # - define basic data structure
  # - define get_utility_model in get_database

  # return user-specific weights for a given user based on logistic regression on
  # their past trips and potential alternatives
  def __init__(user_id, augmented_trips):
    regression = lm.LogisticRegression()
    for trip in augmented_trips:
      alternatives = trip.get_alternatives()
      trip_features = trip.extract_features()
      features = [trip_features] + [alt.extract_features() for alt in alternatives]
      regression.fit(features, trip_features)

      # cross validation?

    return regression.get_params()

  # update existing model using existing trips
  # for now, just create a new model and return it
  def update(augmented_trips):
    pass

  def self.find_from_db(user_id):
    return get_utility_model_db().find_one({'user_id': user_id})

  def store_in_db():
    model_query = {'user_id': self.user_id}
    model_object = {'cost': self.cost, 'time': self.time, 'mode': self.mode, 'updated_at': datetime.now()}
    get_utility_model_db().update(model_query, model_object, upsert = True)

  # TODO: move to trip.py
  # return an array of feature values for the given trip
  # current features are cost, time, mode
  def extract_features(trip):
    cost = trip.cost
    time = calc_duration(trip)
    mode = trip.single_mode

    return (cost, time, mode)

  def calc_duration(trip):
  	start_time = trip.start_time
    end_time = trip.end_time
    duration = end_time - start_time
    return duration