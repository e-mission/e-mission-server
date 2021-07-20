import emission.analysis.modelling.tour_model.data_preprocessing as preprocess


# to determine if the user is valid:
# valid user should have >= 10 trips for further analysis and the proportion of filter_trips is >=50%
def valid_user(filter_trips,trips):
    valid = False
    if len(filter_trips) >= 10 and len(filter_trips) / len(trips) >= 0.5:
        valid = True
    return valid


# - user_ls: a list of strings representing short user names, such as [user1, user2, user3...]
# - valid_user_ls: a subset of `user_ls` for valid users, so also string representation of user names
# - all_users: a collection of all user ids, in terms of user id objects
def get_user_ls(all_users,radius):
    user_ls = []
    valid_user_ls = []
    for i in range(len(all_users)):
        curr_user = 'user' + str(i + 1)
        user = all_users[i]
        trips = preprocess.read_data(user)
        filter_trips = preprocess.filter_data(trips,radius)
        if valid_user(filter_trips,trips):
            valid_user_ls.append(curr_user)
            user_ls.append(curr_user)
        else:
            user_ls.append(curr_user)
            continue
    return user_ls,valid_user_ls

