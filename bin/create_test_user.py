# Create a dummy user for the app store review testing
# This needs to be done every time we switch to a new database

import sys
from emission.core.wrapper.user import User

def create_fake_JWT_token(userEmail):
    """
    Creates a fake JWT token for the specified user. This has two major benefits.
    1. It allows us to finally write some tests for the api layer
    2. It allows the utility functions to communicate with the server via the
        API layer, making it easier to unify the implementation. But on the
        other hand, then it is hard to ensure proper separation between the API layer
        and the implementations, which makes it harder to replace bottle with something else later.
    """
    pass

def register_user(userEmail):
    """
        Does some sanity checking and then registers the user with the specified email address.
        This is useful for testing.
    """
    if User.fromEmail(userEmail) is not None:
        raise RuntimeError("User with email %s already exists" % userEmail)
    
    userObj = User.register(userEmail)
    
    print "Registration successful!"
    print "User profile is %s" % userObj.getProfile();

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "USAGE: %s userEmail\n" % sys.argv[0]
        exit(0)
    register_user(sys.argv[1])
