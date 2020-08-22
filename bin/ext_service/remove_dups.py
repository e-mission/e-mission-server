from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.get_database as edb

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)


    for group_of_dups in edb.get_habitica_db().aggregate([
        { "$group": {
            "_id": { "user_id": "$user_id" },
            "dups": { "$push": "$user_id" },
            "count": { "$sum": 1 }
        }},
        { "$match": { "count": { "$gt": 1 } }}
    ])['result']:
        logging.debug("Group of Duplicates %s" % group_of_dups)
        bike = 0
        walk = 0
        for dup in group_of_dups['dups']:
            logging.debug("Each Duplicate %s" % dup)
            bike += list(edb.get_habitica_db().find({'user_id': dup}))[0]['metrics_data']['bike_count']
            walk += list(edb.get_habitica_db().find({'user_id': dup}))[0]['metrics_data']['walk_count']
            if edb.get_habitica_db().count_documents({'user_id': dup}) > 1:            
                edb.get_habitica_db().remove({'user_id': dup}, multi=False)
            elif edb.get_habitica_db().count_documents({'user_id': dup}) == 1:
                edb.get_habitica_db().update({"user_id": dup},{"$set": {'metrics_data.bike_count': bike, 'metrics_data.walk_count': walk}},upsert=True)
        





