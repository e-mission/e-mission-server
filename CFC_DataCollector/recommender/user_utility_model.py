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
from sklearn.preprocessing import normalize
import numpy as np

class UserUtilityModel(object):
  # return user-specific weights for a given user based on logistic regression on
  # their past trips and potential alternatives

  def __init__(self, trips_with_alts):
    #self.num_alternatives = 4
    self.num_alternatives = 8
    self.regression = lm.LogisticRegression()
    self.trips_with_alts = trips_with_alts

  #Trips is a comprehensive list of user trips
  #Alternatives is a list of TripIterators
  def update(self):
    trip_features, labels = self.extract_features()
    #TODO: why the hell is this happening..? absurd feature extraction
    trip_features[np.abs(trip_features) < .001] = 0
    trip_features[np.abs(trip_features) > 1000000] = 0
    nonzero = ~np.all(trip_features==0, axis=1)
    print nonzero
    trip_features = trip_features[nonzero]
    labels = labels[nonzero]

    print "Trip Features: ", trip_features
    self.regression.fit(trip_features, labels)
    self.coefficients = self.regression.coef_
    self.save_coefficients()
    '''
    best_trip = None
    best_utility = float("-inf")
    for i, trip_feature in enumerate(trip_features):
        utility = self.predict_utility(trip_feature)
 	if utility > best_utility:
		best_trip = i
		best_utility = utility
    if labels[best_trip] == 1:
        print "Model predicts best trip is: ORIGINAL TRIP", best_trip
    else: 
        print "Model predicts best trip is: Alternative TRIP", best_trip
    '''

  # calculate the utility of trip using the model
  def predict_utility(self, trip):
    utility = sum(f * c for f, c in zip(trip, self.coefficients[0]))
    print trip, " Utility: ", utility 
    return utility

  # find model params from DB and construct a model using these params
  @staticmethod
  def find_from_db(user_id, modified):
      if modified:
          db_model = get_utility_model_db().find_one({'user_id': user_id, 'type':'recommender'})
      else:
          db_model = get_utility_model_db().find_one({'user_id': user_id, 'type':'user'})
      return db_model
    # contruct and return model using params from DB


  # store the object with the correct extracted features in the database
  # must be filled out in subclass
  def store_in_db(self):
    pass

  def save_coefficients(self):
    pass

  # return an array of feature values for the given trip
  # must be filled out in subclass
  def extract_features(self):
    pass
