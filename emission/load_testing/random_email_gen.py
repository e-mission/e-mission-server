import string
import random

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """Script borrowed from here: https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python"""
    return ''.join(random.choice(chars) for _ in range(size))