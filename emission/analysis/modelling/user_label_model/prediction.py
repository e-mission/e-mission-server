from typing import Dict

# something like this:
# x = {'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'church', 'replaced_mode': 'drove_alone'}, 'p': 0.9333333333333333}

Prediction = Dict

# todo: if OpenPATH goes to Python 3.8, we can use this:
#
# class Prediction(TypedDict):
#     labels: Dict[str, str]
#     p: float
