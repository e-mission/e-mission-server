import argparse
import emission.storage.decorations.user_queries as esdu
import emission.core.get_database as edb

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("old_deployment_name",
                        help="the deployment name that the data was copied from")
    parser.add_argument("new_deployment_name",
                        help="the deployment name that we want to rename to")
    args = parser.parse_args()

    all_users = edb.get_uuid_db().find()
    for u in all_users:
        u["user_email"] = u["user_email"].replace(args.old_deployment_name, args.new_deployment_name)
        update_result = edb.get_uuid_db().replace_one({"_id": u["_id"]}, u)
        if update_result.matched_count != 1 and update_result.modified_count != 1:
            logging.error("Result %s while updating entry for %s" % (update_result.raw_result, u["user_email"]))
