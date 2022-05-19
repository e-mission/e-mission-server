from typing import Dict, List
from emission.analysis.modelling.user_label_model.prediction import Prediction

# something like this:
# bin_data = {
#   "predictions": [
#       {'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'insurance', 'replaced_mode': 'drove_alone'}, 'p': 0.9333333333333333}
#   ],
#   "locations": [
#       [-122.00, 39.00, -122.01, 39.00]
#   ],
#   "labels": [
#       {'mode_confirm': 'shared_ride', 'purpose_confirm': 'insurance_payment', 'replaced_mode': 'drove_alone'}
#   ]
# }

BinRecord = Dict

# todo: if OpenPATH goes to Python 3.8, we can use this:
#
# class BinRecord(TypedDict):
#     predictions: List[Prediction]
#     features: List[List[float]]
#     labels: List[Dict[str, str]]


