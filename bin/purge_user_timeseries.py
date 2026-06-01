import logging
import argparse
import uuid
from datetime import datetime
import emission.core.wrapper.user as ecwu
import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp
import emission.core.wrapper.pipelinestate as ecwp
import emission.storage.pipeline_queries as esp

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog="purge_user_timeseries")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")

    args = parser.parse_args()

    if args.user_uuid:
        user_id = uuid.UUID(args.user_uuid)
    else:
        user_id = ecwu.User.fromEmail(args.user_email).uuid

    cstate = esp.get_current_state(user_id, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS)
    last_ts_run = cstate['last_ts_run']

    if not last_ts_run:
        logging.warning("No processed timeserie for user {}".format(user_id))
        exit(1)

    res = edb.get_timeseries_db().delete_many({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}})
    logging.info("{} deleted entries since {}".format(res.deleted_count, datetime.fromtimestamp(last_ts_run)))
    