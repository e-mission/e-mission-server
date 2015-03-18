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
  def __init__(augmented_trips = []):
    regression = lm.LogisticRegression()
    for trip in augmented_trips:
      alternatives = trip.get_alternatives()
      trip_features = trip.extract_features()
      alt_features = [alt.extract_features() for alt in alternatives]
      target_vector = [1] + ([0] * len(alternatives))
      regression.fit(trip_features + alt_features, target_vector)

    self.coefficients = regression.coef_
    self.cost = self.coefficients[0]
    self.time = self.coefficients[1]
    self.mode = self.coefficients[2]

  # update existing model using existing trips
  # for now, just create a new model and return it
  def update(augmented_trips):
    pass

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

  def store_in_db():
    model_query = {'user_id': self.user_id}
    model_object = {'cost': self.cost, 'time': self.time, 'mode': self.mode, 'updated_at': datetime.now()}
    get_utility_model_db().update(model_query, model_object, upsert = True)

  # return an array of feature values for the given trip
  # current features are cost, time, mode
  def extract_features(trip):
    cost = trip.calc_cost()
    time = trip.calc_time()
    mode = trip.mode

    return (cost, time, mode)
