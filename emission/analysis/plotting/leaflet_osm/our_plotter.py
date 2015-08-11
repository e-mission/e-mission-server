import pandas as pd
import folium
import emission.analysis.classification.cleaning.location_smoothing as ls
import itertools

def df_to_string_list(df):
    """
    Convert the input df into a list of strings, suitable for using as popups in a map.
    This is a utility function.
    """
    # print "Converting df with size %s to string list" % df.shape[0]
    array_list = df.to_dict(orient='records')
    return [str(line) for line in array_list]

def get_map_list(df, potential_splits):
    mapList = []
    potential_splits_list = list(potential_splits)
    for start, end in zip(potential_splits_list, potential_splits_list[1:]):
        trip = df[start:end]
        print "Considering trip from %s to %s because start = %d and end = %d" % (df.formatted_time.loc[start], df.formatted_time.loc[end], start, end)
        if end - start < 4:
            # If there are only 3 entries, that means that there is only one
            # point other than the start and the end, bail
            print "Ignoring trip from %s to %s because start = %d and end = %d" % (df.formatted_time.loc[start], df.formatted_time.loc[end], start, end)
            continue
        mapList.append(get_map(trip))
    return mapList

def get_map(section_points):
    currMap = folium.Map([section_points.mLatitude.mean(), section_points.mLongitude.mean()])
    currMap.div_markers(section_points[['mLatitude', 'mLongitude']].as_matrix().tolist(),
        df_to_string_list(section_points), marker_size=5)
    currMap.line(section_points[['mLatitude', 'mLongitude']].as_matrix().tolist())
    return currMap

def evaluate_filtering(section_list, outlier_algos, filtering_algos):
    """
    TODO: Is this the best place for this? If not, what is?
    It almost seems like we need to have a separate evaluation module that is
    separate from the plotting and the calculation modules.
    But then, what is the purpose of this module?
    """
    nCols = 2 + len(outlier_algos) * len(filtering_algos)
    nRows = len(section_list)

    map_list = []

    for section in section_list:
        curr_compare_list = []
        section_df = ls.get_section_points(section)
        curr_compare_list.append(get_map(section_df))
        curr_compare_list.append(get_map(ls.filter_points(section_df, None, None)))
        for (oa, fa) in itertools.product(outlier_algos, filtering_algos):
            curr_compare_list.append(get_map(ls.filter_points(section_df, oa, fa)))
        assert(len(curr_compare_list) == nCols)
        map_list.append(curr_compare_list)
    assert(len(map_list) == nRows)
    return map_list
