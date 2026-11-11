import logging
import argparse
import emission.core.get_database as edb

from bin.federation import run_on_all_deployments

def print_connect_url(print_collections, other_args):
    coll_list = edb._current_db.list_collection_names() if print_collections else "SKIPPED"
    print(f"Connecting to database {edb.url}, with collections {coll_list} and {other_args=}")
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="reset_invalid_pipeline_stage")
    parser.add_argument("-c", "--collections", action="store_true", default=False,
                        help="Display database collections")
    parser.add_argument("-f", "--federated", action="store_true", default=False,
                        help="Run against all active deployments")

    args = parser.parse_args()
    if not args.federated:
        print_connect_url(args.collections, 300)
    else:
        run_on_all_deployments(print_connect_url, args.collections, 300)
