from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
__author__ = 'Yin'
from pymongo import MongoClient
db = MongoClient('54.218.218.130').Test_database
Test_Sections=db.Test_Sections

Test_Sections.remove( { 'user_id' : 'Fate' } )

sections_nopoints={'source': 'Shankari', 'group': 1, '_id': 'Fate_20140321T092124-0700',
               'section_id': 0, 'user_id': 'Fate', 'confirmed_mode': '', 'manual': False,
               'trip_id': '20140321T092123-0700', 'mode': 1, 'section_start_time': '20140321T092123-0700',
               'section_end_time': '20140321T094359-0700', 'type': 'move', 'track_points': []}
Test_Sections.insert(sections_nopoints)

# sections_manypoints={"group": 4, "section_end_time": "20140322T151009-0700", "source": "Shankari", "type": "move", "manual": False, "section_id": 0, "track_points": [{"track_location": {"type": "Point", "coordinates": [-122.27276, 37.83206]}, "time": "20140322T150304-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.27255, 37.83226]}, "time": "20140322T150323-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.27256, 37.83228]}, "time": "2014 0322T150347-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.27307, 37.83478]}, "time": "20140322T150419-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.27355, 37.83743]}, "time": "20140322T150438-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.274, 37.84001]}, "time": "20140322T150509-0700"}, {"track_location": {"type": "Point" , "coordinates": [-122.27501, 37.84418]}, "time": "20140322T150551-0700"}, {"track_location": {" type": "Point", "coordinates": [-122.27644, 37.84591]}, "time": "20140322T150608-0700"}, {"track _location": {"type": "Point", "coordinates": [-122.27687, 37.84643]}, "time": "20140322T150629-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.27917, 37.85343]}, "time": "20140322T150729-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.27968, 37.85665]} , "time": "20140322T150810-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.2802, 37.8587 4]}, "time": "20140322T150830-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.28029, 37.85911]}, "time": "20140322T150850-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.28102, 37.86191]}, "time": "20140322T150910-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.28103, 37.86197]}, "time": "20140322T150930-0700"}, {"track_location": {"type": "Point", "coordinates": [-122.28158, 37.86339]}, "time": "20140322T151009-0700"}], "mode": 4, "user_id": "Fake", "section_start_time": "20140322T150304-0700" , "_id": "Fake_20140322T150304-0700", "trip_id": "20140322T150304-0700", "confirmed_mode": ""}

sections_manypoints={"group": 4,
    "section_end_time": "20140322T151009-0700",
    "source": "Shankari",
    "type": "move",
    "manual": False,
    "section_id": 0,
    "track_points": [
        {"track_location": {"type": "Point", "coordinates": [-122.27276, 37.83206]}, "time": "20140322T150304-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.27255, 37.83226]}, "time": "20140322T150323-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.27256, 37.83228]}, "time": "2014 0322T150347-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.27307, 37.83478]}, "time": "20140322T150419-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.27355, 37.83743]}, "time": "20140322T150438-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.274, 37.84001]}, "time": "20140322T150509-0700"},
        {"track_location": {"type": "Point" , "coordinates": [-122.27501, 37.84418]}, "time": "20140322T150551-0700"},
        {"track_location": {" type": "Point", "coordinates": [-122.27644, 37.84591]}, "time": "20140322T150608-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.27687, 37.84643]}, "time": "20140322T150629-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.27917, 37.85343]}, "time": "20140322T150729-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.27968, 37.85665]} , "time": "20140322T150810-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.2802, 37.85874]}, "time": "20140322T150830-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.28029, 37.85911]}, "time": "20140322T150850-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.28102, 37.86191]}, "time": "20140322T150910-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.28103, 37.86197]}, "time": "20140322T150930-0700"},
        {"track_location": {"type": "Point", "coordinates": [-122.28158, 37.86339]}, "time": "20140322T151009-0700"}],
    "mode": 4,
    "user_id": "Fate",
    "section_start_time": "20140322T150304-0700",
    "_id": "Fate_20140322T150304-0700",
    "trip_id": "20140322T150304-0700",
    "confirmed_mode": ""}

Test_Sections.insert(sections_manypoints)

section_manypoints_2={'group': 1, 'section_end_time': '20140320T155044-0700', 'track_points': [{'track_location': {'type': 'Point', 'coordinates': [-122.263988406, 37.8732744268]}, 'time': '20140320T153907-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.263068786, 37.8727199532]}, 'time': '20140320T154108-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2614056709, 37.8741872567]}, 'time': '20140320T154501-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2595329613, 37.8749194258]}, 'time': '20140320T154744-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2591187306, 37.8750675898]}, 'time': '20140320T154818-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2588007342, 37.8752950206]}, 'time': '20140320T154838-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2587144481, 37.8752638478]}, 'time': '20140320T154901-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586950937, 37.8752277825]}, 'time': '20140320T154916-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586702886, 37.8752147033]}, 'time': '20140320T154938-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586594559, 37.8752279182]}, 'time': '20140320T154958-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586619306, 37.8752439695]}, 'time': '20140320T155022-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586588159, 37.8752440597]}, 'time': '20140320T155044-0700'}], 'type': 'move', 'manual': False, 'section_id': 0, 'source': 'Shankari', 'mode': 1, 'user_id': 'Fate', 'section_start_time': '20140320T153907-0700', '_id': 'Fate_20140320T153907-0700', 'trip_id': '20140320T153907-0700', 'confirmed_mode': ''} 
Test_Sections.insert(section_manypoints_2)

section_manypoints_3={'group': 1, 'section_end_time': '20140320T181057-0700', 'track_points': [{'track_location': {'type': 'Point', 'coordinates': [37.8752440597, -122.2586588159]}, 'time': '20140320T175850-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586337875, 37.8752473405]}, 'time': '20140320T175910-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586360783, 37.875245906]}, 'time': '20140320T175929-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586425346, 37.8752420456]}, 'time': '20140320T175950-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586464048, 37.875239919]}, 'time': '20140320T175959-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586464089, 37.8752399168]}, 'time': '20140320T180000-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586500285, 37.8752379279]}, 'time': '20140320T180008-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2586621802, 37.8752319751]}, 'time': '20140320T180038-0700'}, {'track_location': {'type': 'Point', 'coordinates': [-122.2564625824, 37.8689639486]}, 'time': '20140320T181057-0700'}], 'type': 'move', 'manual': False, 'section_id': 0, 'source': 'Shankari', 'mode': 1, 'user_id': 'Fate', 'section_start_time': '20140320T175850-0700', '_id': 'Fate_20140320T175850-0700', 'trip_id': '20140320T175850-0700', 'confirmed_mode': ''} 
Test_Sections.insert(section_manypoints_3)
