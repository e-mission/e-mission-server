from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import attrdict as ad
import uuid

import emission.core.get_database as edb
import emission.storage.timeseries.aggregate_timeseries as estag

def reset_collection(coll, old_uuid, new_uuid):
    logging.debug(coll.update({"user_id": user.uuid},
              {"$set": {"user_id": new_uuid}}, multi=True))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    for user_dict in edb.get_uuid_db().find():
        user = ad.AttrDict(user_dict)
        if user.uuid in estag.TEST_PHONE_IDS:
            logging.debug("Found test phone, skipping reset")
        else:
            new_uuid = uuid.uuid4()
            logging.debug("Mapping %s -> %s" % (new_uuid, user.uuid))
            edb.get_uuid_db().update({"uuid" : user.uuid},
                                     {"$set": {"uuid" : new_uuid}})
            logging.debug("Resetting alternatives...")
            reset_collection(edb.get_alternatives_db(), user.uuid, new_uuid)
            logging.debug("Resetting analysis...")
            reset_collection(edb.get_analysis_timeseries_db(), user.uuid, new_uuid)
            logging.debug("Resetting client...")
            reset_collection(edb.get_client_db(), user.uuid, new_uuid)
            logging.debug("Resetting client_stats_backup...")
            reset_collection(edb.get_client_stats_db_backup(), user.uuid, new_uuid)
            logging.debug("Resetting server_stats_backup...")
            reset_collection(edb.get_server_stats_db_backup(), user.uuid, new_uuid)
            logging.debug("Resetting result_stats_backup...")
            reset_collection(edb.get_result_stats_db_backup(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_common_place_db...")
            reset_collection(edb.get_common_place_db(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_common_trip_db...")
            reset_collection(edb.get_common_trip_db(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_habitica_db...")
            reset_collection(edb.get_habitica_db(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_pipeline_state_db...")
            reset_collection(edb.get_pipeline_state_db(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_profile_db...")
            reset_collection(edb.get_profile_db(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_timeseries_db...")
            reset_collection(edb.get_timeseries_db(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_timeseries_error_db...")
            reset_collection(edb.get_timeseries_error_db(), user.uuid, new_uuid)
            logging.debug("Resetting edb.get_usercache_db...")
            reset_collection(edb.get_usercache_db(), user.uuid, new_uuid)
