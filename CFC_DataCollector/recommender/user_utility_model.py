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
class UserUtilityModel(object):
  # return user-specific weights for a given user based on logistic regression on
  # their past trips and potential alternatives
  def __init__(self, user_id, trips, alternatives): # assuming alternatives a list of lists
    # TODO: Using list() here removes the performance benefits of an iterator.
    # Consider removing/relaxing the assert
    #print len(list(trips)), len(alternatives)
    assert(len(list(trips)) == len(alternatives))
    self.user_id = user_id
    self.regression = lm.LogisticRegression()
    self.update(trips, alternatives)

  # update existing model using existing trips
  # for now, just create a new model and return it
  def update(self, trips = [], alternatives = []):
    assert(len(list(trips)) == len(alternatives))
    trip_features = self.extract_features(trips)
    to_evaluate = []
    for t_f in trip_features:
        to_evaluate.append(t_f)
    for trip_alternatives in alternatives:
      alt_features = self.extract_features(trip_alternatives)
      for a_f in alt_features:
         to_evaluate.append(a_f)
      target_vector = [1] + ([0] * len(list(trip_alternatives)))
      self.regression.fit(trip_features + alt_features, target_vector)
    self.coefficients = self.regression.coef_
    print self.coefficients
    best_trip = None
    best_utility = float("-inf")
    for trip_feature in to_evaluate:
        utility = self.predict_utility(trip_feature)
 	if utility > best_utility:
		best_trip = trip_feature	
		best_utility = utility
    if best_trip == to_evaluate[0]:
        print "Model predicts best trip is: ORIGINAL TRIP", best_trip
    else: 
        print "Model predicts best trip is: Alternative TRIP", best_trip

  # calculate the utility of trip using the model
  def predict_utility(self, trip):
    trip_features = trip
    #trip_features = extract_features(trip)
    utility = sum(f * c for f, c in zip(trip_features, self.coefficients[0]))
    print trip, " Utility: ", utility
    return utility

  # find model params from DB and construct a model using these params
  @staticmethod
  def find_from_db(user_id):
    db_model = get_utility_model_db().find_one({'user_id': user_id})
    # contruct and return model using params from DB

  # store the object with the correct extracted features in the database
  # must be filled out in subclass
  def store_in_db(self):
    pass

  # return an array of feature values for the given trip
  # must be filled out in subclass
  def extract_features(self, trip):
    pass
