"""
Reset only the mode inference objects
This helps us experiment with different mode inference methods without
resetting the entire pipeline.
Useful while mode inference is the most recent data object, and we need to
experiment with it to make it better.
"""

"""
This is similar to `bin/reset_pipeline.py` but *much* easier, since the mode
inference results are not linked to anything else. And the start_ts of the
inference result = start_ts of the section it maps to. We just need to delete
everything after the reset point and set the pipeline state accordingly.
"""
import logging
import argparse
import uuid
import arrow

import emission.analysis.classification.inference.mode.pipeline as eacimp
import emission.core.get_database as edb
import emission.storage.decorations.user_queries as esdu

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    # Options corresponding to
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312464984
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-a", "--all", action="store_true", default=False,
        help="reset the pipeline for all users")
    group.add_argument("-p", "--platform", choices = ['android', 'ios'],
                        help="reset the pipeline for all on the specified platform")
    group.add_argument("-u", "--user_list", nargs='+',
        help="user ids to reset the pipeline for")
    group.add_argument("-e", "--email_list", nargs='+',
        help="email addresses to reset the pipeline for")
    parser.add_argument("-d", "--date",
        help="date to reset the pipeline to. Format 'YYYY-mm-dd' e.g. 2016-02-17. Interpreted in UTC, so 2016-02-17 will reset the pipeline to 2016-02-16T16:00:00-08:00 in the pacific time zone")
    parser.add_argument("-n", "--dry_run", action="store_true", default=False,
                        help="do everything except actually perform the operations")

    args = parser.parse_args()
    print(args)

    # Handle the first row in the table
    if args.date is None:
        if args.all:
            eacimp.del_all_objects(args.dry_run)
        else:
            user_list = _get_user_list(args)
            logging.info("received list with %s users" % user_list)
            logging.info("first few entries are %s" % user_list[0:5])
            for user_id in user_list:
                logging.info("resetting user %s to start" % user_id)
                eacimp.del_objects_after(user_id, 0, args.dry_run)
    else:
    # Handle the second row in the table
        day_dt = arrow.get(args.date, "YYYY-MM-DD")
        logging.debug("day_dt is %s" % day_dt)
        day_ts = day_dt.timestamp
        logging.debug("day_ts is %s" % day_ts)
        user_list = _get_user_list(args)
        logging.info("received list with %s users" % user_list)
        logging.info("first few entries are %s" % user_list[0:5])
        for user_id in user_list:
            logging.info("resetting user %s to ts %s" % (user_id, day_ts))
            eacimp.del_objects_after(user_id, day_ts, args.dry_run)

