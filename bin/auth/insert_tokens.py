import argparse
import emission.storage.decorations.token_queries as esdt
import emission.core.get_database as edb

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="insert_tokens")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--file", help="add tokens to DB from file")
    group.add_argument("--single", help="add a single token")
    group.add_argument("--uuid", help="add tokens to the DB off of the email field of UUIDS", action="store_true")
    group.add_argument("--show", help="show tokens that are in the DB", action="store_true")

    args = parser.parse_args()

    if args.file:
        token_list = (esdt.get_tokens_from_file(args.file))
        esdt.insert_many_tokens(token_list)

    elif args.single:
        token = args.single
        esdt.insert({"token":token})

    elif args.uuid:
        uuid_list = edb.get_uuid_db().find()
        for u in uuid_list:
            esdt.insert({"token":u["user_email"]})

    elif args.show:
        token_list = esdt.get_all_tokens()
        for t in token_list:
            print(t)

    else:
        print("Please provide the script with an argument. Use the \"--help\" option for more details")