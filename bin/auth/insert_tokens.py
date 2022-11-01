import argparse
import emission.storage.decorations.token_queries as esdt

# def get_tokens_from_file(file):
#     with open(file) as tlf:
#         raw_token_list = tlf.readlines()
#     token_list = [t.strip() for t in raw_token_list]
#     return(token_list)

# def load_tokens_into_db(token_list):
#     esdt.insert_many_tokens(token_list)
#     # for t in token_list:
#     #     esdt.insert({"token":t})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="insert_tokens")

    parser.add_argument("file")

    args = parser.parse_args()

    token_list = (esdt.get_tokens_from_file(args.file))
    esdt.insert_many_tokens(token_list)
