from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
class Recommendation(object):
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
