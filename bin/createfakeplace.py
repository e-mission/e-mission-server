import emission.core.wrapper.entry as ecwe

#Creates a fake, cleaned place in Rhode Island for use in TestNominatim.py:
def create_fake_place():
    fake_id = "place_in_rhodeisland"
    key = "segmentation/raw_place"
    write_ts = 1694344333
    data = {'source': 'FakeTripGenerator','location': {'type': 'Point', 'coordinates': [-71.4128343, 41.8239891]}}
    fake_place = ecwe.Entry.create_fake_entry(fake_id, key, data, write_ts)
    return fake_place