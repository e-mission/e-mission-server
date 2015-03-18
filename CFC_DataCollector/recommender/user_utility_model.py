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
class UserUtilityModel:
  # TO DO:
  # - define get_utility_model in get_database

  # return user-specific weights for a given user based on logistic regression on
  # their past trips and potential alternatives
  def __init__(trips = [], alternatives = []): # assuming alternatives a list of lists
    assert(len(trips) == len(alternatives))
    self.regression = lm.LogisticRegression()
    self.update(trips, alternatives)
    # for i in range(len(augmented_trips)):
    #   trip_features = trips[i].extract_features()
    #   alt_features = [alt.extract_features() for alt in alternatives[i]]
    #   target_vector = [1] + ([0] * len(alternatives))
    #   self.regression.fit(trip_features + alt_features, target_vector)
    # self.coefficients = regression.coef_

  # update existing model using existing trips
  # for now, just create a new model and return it
  def update(trips = [], alternatives = []):
    assert(len(trips) == len(alternatives))
    for i in range(len(augmented_trips)):
      trip_features = trips[i].extract_features()
      alt_features = [alt.extract_features() for alt in alternatives[i]]
      target_vector = [1] + ([0] * len(alternatives))
      self.regression.fit(trip_features + alt_features, target_vector)
    self.coefficients = regression.coef_

  # calculate the utility of trip using the model
  def predict_utility(trip):
    trip_features = extract_features(trip)
    utility = sum(f * c for f, c in zip(trip_features, self.coefficients))
    return utility

  # find model params from DB and construct a model using these params
  @staticmethod
  def find_from_db(user_id):
    db_model = get_utility_model_db().find_one({'user_id': user_id})
    # contruct and return model using params from DB

  # store the object with the correct extracted features in the database
  # must be filled out in subclass
  def store_in_db():
    pass

  # return an array of feature values for the given trip
  # must be filled out in subclass
  def extract_features(trip):
    pass
