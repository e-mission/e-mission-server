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
import alternative_trips_module as atm
from sklearn import linear_model as lm
from sklearn.preprocessing import normalize
import numpy as np
import logging

class UserUtilityModel(object):
  # return user-specific weights for a given user based on logistic regression on
  # their past trips and potential alternatives

  def __init__(self, trips):
    #self.num_alternatives = 4
    self.num_alternatives = 8
    self.regression = lm.LogisticRegression()
    self.trips = trips

  #Trips is a comprehensive list of user trips
  #Alternatives is a list of TripIterators
  def update(self):
    logging.debug("About to extract features for %d trips " % (len(self.trips)))
    trip_features, labels = self.extract_features()
    logging.debug("trip_features.size() = %s, nTrips = %d " % (trip_features.size, len(self.trips)))
    #TODO: why the hell is this happening..? absurd feature extraction
    trip_features = np.nan_to_num(trip_features)
    trip_features[np.abs(trip_features) < .001] = 0
    trip_features[np.abs(trip_features) > 1000000] = 0
    nonzero = ~np.all(trip_features==0, axis=1)
    logging.debug("nonzero list = %s" % nonzero)
    trip_features = trip_features[nonzero]
    labels = labels[nonzero]

    # logging.debug("Trip Features: %s" % trip_features)
    try:
       self.regression.fit(trip_features, labels)
       self.coefficients = self.regression.coef_
       self.save_coefficients()
    except ValueError, e:
       logging.warning("While fitting the regression, got error %s" % e)
       if ("%s" % e) == "The number of classes has to be greater than one":
          logging.warning("Training set has no alternatives!")
       raise e
       # else:
       #    np.save("/tmp/broken_array", trip_features)
       #    raise e
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

  def extract_features(self):
    logging.debug("About to call extract_features(trips)")
    return self.extract_features_for_trips(self.trips)

  def extract_features_for_trips(self, trips):
    '''
       For the specified set of trips, retrieve alternatives and generate all
       their features. The alternatives are computed in here instead of outside
       in order to keep the number of outstanding connections to the database low.
       Otherwise, we keep a connection open for every trip in the trip list.
    '''
    logging.debug("about to get num_features from %s" % self.feature_list)
    num_features = len(self.feature_list)
    logging.debug("num_features = %d " % num_features)
    feature_vector = np.zeros((len(trips * (self.num_alternatives+1)), num_features)) 
    label_vector = np.zeros(len(trips * (self.num_alternatives+1)))
    logging.debug("creating label_vector with size %d" % len(trips * (self.num_alternatives+1)))
    logging.debug("after creation, len = %d, size = %d" % (len(label_vector), label_vector.size))
    sample = 0
    for trip in trips:
        logging.debug("Considering trip %s" % trip._id)
        alt = list(atm.get_alternative_for_trip(trip))
        if len(alt) > 0:
            feature_vector[sample] = self._extract_features(trip)
            label_vector[sample] = 1
            sample += 1
            logging.debug("original sample = %d" % sample)
            for _alt in alt:
                feature_vector[sample] = self._extract_features(_alt)
                label_vector[sample] = 0
                sample += 1
                logging.debug("Alt: %d" % sample)
        else:
            logging.debug("No alternatives found for trip %s, skipping " % trip._id)
	# Close the connection to the database after reading all the alternatives
        try:
	    alt.close()
        except AttributeError:
            logging.debug("Non cursor iterator, skipping close")
    logging.debug("Returning feature vector = %s" % feature_vector)
    return (feature_vector, label_vector)

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
