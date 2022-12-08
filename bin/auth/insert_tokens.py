import argparse
import emission.storage.decorations.token_queries as esdt
import emission.core.get_database as edb

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="insert_tokens")

    parser.add_argument("--file", help="add tokens to DB from file")
    parser.add_argument("--single", help="add a single token")
    parser.add_argument("--uuid", help="add tokens to the DB off of the email field of UUIDS", action="store_true")
    parser.add_argument("--show", help="show tokens that are in the DB", action="store_true")

    args = parser.parse_args()

    if args.file:
        token_list = (esdt.get_tokens_from_file(args.file))
        esdt.insert_many_tokens(token_list)

    if args.single:
        token = args.single
        esdt.insert({"token":token})

    if args.uuid:
        uuid_list = edb.get_uuid_db().find()
        for u in uuid_list:
            esdt.insert({"token":u["user_email"]})

    if args.show:
        token_list = esdt.get_all_tokens()
        for t in token_list:
            print(t)