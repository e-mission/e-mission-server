class Recommendation:
  def __init__(self, user_id, original_trip, recommended_trip):
    self.user_id = user_id
    self.original_trip = original_trip
    self.recommended_trip = recommended_trip

  # load the recommendation from the database
  def load_from_db(self):
    pass

  # save the recommendation to the database
  def save_to_db(self):
    pass
