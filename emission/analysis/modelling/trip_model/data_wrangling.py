import pandas as pd
import numpy as np
import logging


def expand_df_dict(df, column_name):
    """
        df: a dataframe that contains a column whose values are dictionaries
        column_name: name of the df's column containing dictionary entries

        Returns a dataframe with the desired column expanded into the main dataframe

        This is a generalized version of the expand_userinputs() function from
        e-mission-server/emission/storage/decorations/trip_queries.py 
    """
    if len(df) == 0:
        return df
    expanded_col = pd.DataFrame(df.loc[:, column_name].to_list(),
                                index=df.index)
    logging.debug(expanded_col.head())
    df = df.drop(columns=[column_name])
    expanded_df = pd.concat([df, expanded_col], axis=1)
    assert len(expanded_df) == len(df), \
        ("Mismatch after expanding labels, expanded_df.rows = %s != df.columns %s" %
            (len(expanded_df), len(df)))
    logging.debug("After expanding, columns went from %s -> %s" %
                  (len(df.columns), len(expanded_df.columns)))
    logging.debug(expanded_df.head())
    return expanded_df


# oops, this is actually just the same as pd's explode()
def expand_df_list_vert(df, column_name):
    """
        df: a dataframe that contains a column whose values are lists
        column_name: name of the df's column containing list entries. (the 
            length of the list entry can vary from row to row.)

        Returns a dataframe with the desired column expanded vertically into 
        the main dataframe, i.e. for each row in the original dataframe, there 
        will be n rows in the expanded dataframe where n is the length of its 
        list entry under 'column_name'
    """
    if len(df) == 0:
        return df

    expanded_df_list = []
    for i in range(len(df)):
        col_list = df.loc[i, column_name]
        for e in col_list:
            # add new row to new_df
            new_row = df.loc[i].to_dict()
            new_row[column_name] = e
            expanded_df_list += [new_row]

    if len(expanded_df_list) == 0:
        logging.debug(
            '{} only has empty lists; expansion failed.'.format(column_name))
        raise Exception('expansion failed; empty lists')

    expanded_df = pd.DataFrame(expanded_df_list)

    assert len(expanded_df.columns) == len(df.columns), \
        ("Mismatch after expanding labels, expanded_df.columns = %s != df.columns %s" %
            (len(expanded_df.columns), len(df.columns)))
    logging.debug("After expanding, rows went from %s -> %s" %
                  (len(df), len(expanded_df)))

    return expanded_df


def expand_df_list_horiz(df, column_name):
    """
        df: a dataframe that contains a column whose values are lists
        column_name: name of the df's column containing list entries. (the 
            length of the list entry must be consistent for all rows.)

        Returns a dataframe with the desired column expanded horizontally into 
        the main dataframe, i.e. 'column_name' will be replaced by n columns 
        where n is the length of each list entry
    """
    if len(df) == 0:
        return df
    expanded_col = pd.DataFrame(df.loc[:, column_name].to_list(),
                                index=df.index)
    logging.debug(expanded_col.head())
    df = df.drop(columns=[column_name])
    expanded_df = pd.concat([df, expanded_col], axis=1)
    assert len(expanded_df) == len(df), \
        ("Mismatch after expanding labels, expanded_df.rows = %s != df.columns %s" %
            (len(expanded_df), len(df)))
    logging.debug("After expanding, columns went from %s -> %s" %
                  (len(df.columns), len(expanded_df.columns)))
    logging.debug(expanded_df.head())
    return expanded_df


def add_top_pred(df, trip_id_column='trip_id', pred_conf_column='pred_conf'):
    """ df: dataframe containing trip ids, predicted labels and confidence level
        trip_id_column: string, the name of the column containing trip ids
        pred_conf_column: string, the name of the column containing prediction confidence
    """
    df['top_pred'] = False
    for trip_id in df[trip_id_column].unique():
        id_max = df[df[trip_id_column] == trip_id][pred_conf_column].idxmax(
            skipna=True)
        if not np.isnan(id_max):
            df.loc[id_max, 'top_pred'] = True

    return df


def trips_to_df(trips, user_id, os_df=None):
    datas = []
    for i in range(len(trips)):
        t = trips[i]
        if 'inferred_labels' not in t['data'] or t['data'][
                'inferred_labels'] == []:
            data = {'trip_id': t['_id']}
            data = update_labels(data, t['data']['user_input'], 'true')
            datas.append(data)

        else:
            for label in t['data']['inferred_labels']:
                data = {'trip_id': t['_id']}
                data['pred_conf'] = label['p']
                data = update_labels(data, label['labels'], 'pred')
                data = update_labels(data, t['data']['user_input'], 'true')
                datas.append(data)

    df = pd.DataFrame(datas,
                      columns=[
                          'user_id', 'trip_id', 'mode_pred', 'replaced_pred',
                          'purpose_pred', 'tuple_pred', 'pred_conf',
                          'mode_true', 'replaced_true', 'purpose_true',
                          'tuple_true'
                      ])
    df['user_id'] = user_id
    if os_df:
        df['os'] = os_df[os_df.user_id == user_id]['curr_platform'].item()
    df['tuple_pred'] = df.mode_pred.astype(
        str) + ', ' + df.purpose_pred.astype(
            str) + ', ' + df.replaced_pred.astype(str)
    df['tuple_true'] = df.mode_true.astype(
        str) + ', ' + df.purpose_true.astype(
            str) + ', ' + df.replaced_true.astype(str)

    #     df['tuple_pred'] = list(zip(df.mode_pred, df.purpose_pred, df.replaced_pred))
    #     df['tuple_true'] = list(zip(df.mode_true, df.purpose_true, df.replaced_true))

    # indicates if the predicted label was the top choice (i.e. the first suggestion to the user)
    df = add_top_pred(df)

    return df


def update_labels(data, user_input, label_type):
    """ helper function to populate a dictionary with trip labels.
    
        Args:
            data (dict): dictionary that we want to populate
            user_input (dict): the dictionary containing mode_confirm, 
                purpose_confirm, and replaced_mode information (e.g.
                t['data']['user_input'] or t['data']['inferred_labels'][i] )
            label_type (str): 'true' or 'pred'
    """
    if user_input != {}:
        if 'mode_confirm' in user_input.keys():
            data['mode_' + label_type] = user_input['mode_confirm']
            if data['mode_' + label_type] == 'not_a_trip':
                data['replaced_' + label_type] = 'not_a_trip'
                data['purpose_' + label_type] = 'not_a_trip'

            else:
                if 'replaced_mode' in user_input.keys():
                    data['replaced_' +
                         label_type] = user_input['replaced_mode']
                if 'purpose_confirm' in user_input.keys():
                    data['purpose_' +
                         label_type] = user_input['purpose_confirm']

    return data


def get_labels(trips):
    """ helper function to get lists of trip labels from a list of trip dicts."""
    mode_true = []
    purpose_true = []
    replaced_true = []

    for t in trips:
        if 'mode_confirm' in t['data']['user_input']:
            mode_true.append(t['data']['user_input']['mode_confirm'])
        else:
            mode_true.append(None)

        if 'purpose_confirm' in t['data']['user_input']:
            purpose_true.append(t['data']['user_input']['purpose_confirm'])
        else:
            purpose_true.append(None)

        if 'replaced_mode' in t['data']['user_input']:
            replaced_true.append(t['data']['user_input']['replaced_mode'])
        else:
            replaced_true.append(None)

    return mode_true, purpose_true, replaced_true


def get_trip_index(trips):
    """ helper function to get list of trip indices from a list of trip dicts."""
    trip_indices = []
    for t in trips:
        trip_indices.append(t['_id'])

    return trip_indices


def expand_coords(exp_df, purpose=None):
    """
        copied and modifed from get_loc_df_for_purpose() in the 'Radius
        selection' notebook
    """
    purpose_trips = exp_df
    if purpose is not None:
        purpose_trips = exp_df[exp_df.purpose_confirm == purpose]

    dfs = [purpose_trips]
    for loc_type in ['start', 'end']:
        df = pd.DataFrame(
            purpose_trips[loc_type +
                          "_loc"].apply(lambda p: p["coordinates"]).to_list(),
            columns=[loc_type + "_lon", loc_type + "_lat"])
        df = df.set_index(purpose_trips.index)
        dfs.append(df)

    # display.display(end_loc_df.head())
    return pd.concat(dfs, axis=1)