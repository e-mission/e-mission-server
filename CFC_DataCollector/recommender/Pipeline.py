""" High level pipeline

3 Pipelines
1st pipeline: take trips, come up with alternatives
2nd pipeline: is to just build the model, store the model
3rd pipeline: take the model, trips to improve, and recommend2 pipelines

1. Get Target trips (called by Google Maps team) with user id
    * Save Alternatives table that augmented trips is stored in
    ** Alternatives Table:
    Trip_ip (unique id of the original trip)
    User ID
    Array of legs (mode, cost, startpoint, endpoint, time, distance)
2. Google Maps team will augment the trips, return it back to the user
3. Cost, duration needed for utility function. User utility model outputs ??
4. Pick trips to improve (most common trips)
5. Augment with alternatives and perturbed trips (change start t)
6. Apply model to trips and calculate utility
7. List of ordered recommendation


"""

def getTargetTrips(user_uuid, query_filter=""):
    """Returns a TripFactory of all the Trips to learn from.
    Defaults to using all trips unless there is a filter specified """
    return TripFactory(user_uuid,query_filter)

 
