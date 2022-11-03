import argparse
import emission.storage.decorations.token_queries as esdt

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="insert_tokens")

    parser.add_argument("file")

    args = parser.parse_args()

    token_list = (esdt.get_tokens_from_file(args.file))
    esdt.insert_many_tokens(token_list)
