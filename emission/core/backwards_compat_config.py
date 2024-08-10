import json
import logging
import os
import numpy as np
import pandas as pd

# if there is a config file and the environment variable is set, we need to
# decide which one wins. I would argue for the environment variable, to allow
# for a migration to the new model and for us to remove the obsolete code.
# Although arguably, the converse will also work, since we can set the
# variable while the file is present, and then remove the file in a second
# round of changes. Let's keep the order unchanged for now for simplicity, and
# modify as needed later.

def get_config(config_file_name, var_path_mapping):
    # Since a `config_data` field would be at the module level, and we want
    # the module to be reusable, we are not going to cache the result. It is
    # not clear that we need to cache the result anyway, given that we
    # typically initialize the config variables at the beginning of the
    # modules in which they are used. If we feel like this is an issue, we can
    # switch to creating a class instead.
    ret_val = {}
    try:
        config_file = open(config_file_name)
        # we only have a single entry in the config json, not an array
        # and there is no way for json_normalize to return a series
        # so we will just take the first row of the dataframe
        loaded_val = pd.json_normalize(json.load(config_file)).iloc[0]
        for var, path in var_path_mapping.items():
            ret_val[var] = loaded_val[path]
            # Ensure that the returned values are regular ints
            # https://github.com/e-mission/e-mission-server/pull/961#issuecomment-2282206511
            if type(ret_val[var]) is np.int64:
                ret_val[var] = int(ret_val[var])
        config_file.close()
    except Exception as e:
        if isinstance(e, KeyError) or isinstance(e, json.decoder.JSONDecodeError):
            logging.exception(e)
        print("Config file not found, returning a copy of the environment variables instead...")
        # https://github.com/e-mission/e-mission-server/pull/961#issuecomment-2282209006
        ret_val = dict(os.environ)
    return ret_val
