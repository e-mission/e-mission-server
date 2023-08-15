"""
Script to build and save the labeling model.
"""
import logging

import argparse
import uuid
import arrow
import pymongo

import emission.pipeline.reset as epr
import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.abstract_timeseries as esta
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.config as eamtc

def _get_user_list(args):
    if args.all:
        return _find_all_users()
    elif args.platform:
        return _find_platform_users(args.platform)
    elif args.email_list:
        return _email_2_user_list(args.email_list)
    else:
        assert args.user_list is not None
        return [uuid.UUID(u) for u in args.user_list]

def _find_platform_users(platform):
    # Since all new clients register a profile with the server, we don't have
    # to run a 'distinct' query over the entire contents of the timeseries.
    # Instead, we can simply query from the profile users, which is
    # significantly faster
    # Use the commented out line instead for better performance.
    # Soon, we can move to the more performant option, because there will be
    # no users that don't have a profile
    # return edb.get_timeseries_db().find({'metadata.platform': platform}).distinct(
    #    'user_id')
   return edb.get_profile_db().find({"curr_platform": platform}).distinct("user_id")

def _find_all_users():
   return esta.TimeSeries.get_uuid_list()

def _email_2_user_list(email_list):
    return [ecwu.User.fromEmail(e).uuid for e in email_list]

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--ignore_older_than_weeks", type=int,
        help="skip model build if last trip is older than this")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-a", "--all", action="store_true", default=False,
        help="build the model for all users")
    group.add_argument("-p", "--platform", choices = ['android', 'ios'],
                        help="build the model for all on the specified platform")
    group.add_argument("-u", "--user_list", nargs='+',
        help="user ids to build the model for")
    group.add_argument("-e", "--email_list", nargs='+',
        help="email addresses to build the model for")

    args = parser.parse_args()
    print(args)

    user_list = _get_user_list(args)
    logging.info("received list with %s users" % user_list)
    for user_id in user_list:
        logging.info("building model for user %s" % user_id)
        # these can come from the application config as default values

        if args.ignore_older_than_weeks:
            ts = esta.TimeSeries.get_time_series(user_id)
            last_confirmed_trip_end = arrow.get(
                    ts.get_first_value_for_field("analysis/confirmed_trip",
                        "data.end_ts", pymongo.DESCENDING))

            week_limit = arrow.utcnow().shift(weeks=-args.ignore_older_than_weeks)
            if last_confirmed_trip_end.timestamp() < week_limit.timestamp():
                logging.debug("last trip was at %s, three weeks ago was %s, gap = %s, skipping model building..." %
                    (last_confirmed_trip_end, week_limit, last_confirmed_trip_end.humanize(week_limit)))
                continue
            else:
                logging.debug("last trip was at %s, three weeks ago was %s, gap is %s, building model..." %
                    (last_confirmed_trip_end, week_limit, last_confirmed_trip_end.humanize(week_limit)))

        model_type = eamtc.get_model_type()
        model_storage = eamtc.get_model_storage()
        min_trips = eamtc.get_minimum_trips()
        ## Rebuild and save the trip model with the specified parameters
        eamur.update_trip_model(user_id, model_type, model_storage, min_trips)
