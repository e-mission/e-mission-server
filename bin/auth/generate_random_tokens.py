import secrets
import argparse

def generateRandomToken(length):
    return secrets.token_urlsafe(length)

def generateRandomTokensForProgram(program, token_length, count):
    return [program+"_"+generateRandomToken(token_length) for i in range(count)]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="generate_random_tokens")

    parser.add_argument("program")
    parser.add_argument("token_length", type=int)
    parser.add_argument("count", type=int)

    args = parser.parse_args()

    tokens = generateRandomTokensForProgram(args.program, args.token_length, args.count)
    for t in tokens:
        print(t)
