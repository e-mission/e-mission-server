from typing import Dict, TypedDict

# something like this:
# x = {'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'church', 'replaced_mode': 'drove_alone'}, 'p': 0.9333333333333333}

class Prediction(TypedDict):
    labels: Dict[str, str]
    p: float

    @classmethod
    def from_dict(cls, d) -> Prediction:
        labels = d.get('labels')
        p = d.get('p')
        return Prediction(labels, p
)