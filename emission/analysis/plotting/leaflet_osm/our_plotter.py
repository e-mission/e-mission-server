import pandas as pd
import folium
import emission.analysis.classification.cleaning.location_smoothing as ls
import emission.storage.decorations.location_queries as lq
import itertools
import numpy as np

all_color_list = ['black', 'brown', 'blue', 'chocolate', 'cyan', 'fuschia', 'green', 'lime', 'magenta', 'navy', 'pink', 'purple', 'red', 'snow', 'yellow']
sel_color_list = ['black', 'blue', 'chocolate', 'cyan', 'fuschia', 'green', 'lime', 'magenta', 'pink', 'purple', 'red', 'yellow']

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

def get_map_list_after_segmentation(section_map):
    mapList = []
    for trip, section_list in section_map:
        trip_df = lq.get_points_for_section(trip)
        curr_map = folium.Map([trip_df.mLatitude.mean(), trip_df.mLongitude.mean()])
        last_section_end = None
        for (i, section) in enumerate(section_list):
            section_df = trip_df[np.logical_and(trip_df.mTime >= section.start_ts,
                                                trip_df.mTime <= section.end_ts)]
            print("for section %s, section_df.shape = %s, formatted_time.head() = %s" %
                    (section, section_df.shape, section_df["formatted_time"].head()))
            update_map(curr_map, section_df, line_color = sel_color_list[section.activity.value],
                        popup = "%s" % (section.activity))
            if section_df.shape[0] > 0:
                curr_section_start = section_df.iloc[0]
                if i != 0 and last_section_end is not None:
                    # We want to join this to the previous section.
                    curr_map.line([[last_section_end.mLatitude, last_section_end.mLongitude],
                                   [curr_section_start.mLatitude, curr_section_start.mLongitude]],
                                   line_color = sel_color_list[-1],
                                   popup = "%s -> %s" % (section_list[i-1].activity, section.activity))
                last_section_end = section_df.iloc[-1]
        mapList.append(curr_map)
    return mapList

def get_map(section_points, line_color = None, popup=None):
    currMap = folium.Map([section_points.mLatitude.mean(), section_points.mLongitude.mean()])
    update_map(currMap, section_points, line_color, popup)
    return currMap

def update_map(currMap, section_points, line_color = None, popup=None):
    currMap.div_markers(section_points[['mLatitude', 'mLongitude']].as_matrix().tolist(),
        df_to_string_list(section_points), marker_size=5)
    currMap.line(section_points[['mLatitude', 'mLongitude']].as_matrix().tolist(),
        line_color = line_color,
        popup = popup)

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
            curr_filtered_df = ls.filter_points(section_df, oa, fa)
            print ("After filtering with %s, %s, size is %s" % (oa, fa, curr_filtered_df.shape))
            if "activity" in section:
                curr_compare_list.append(get_map(curr_filtered_df,
                                            line_color = sel_color_list[section.activity.value],
                                                 popup = "%s" % (section.activity)))
            else:
                curr_compare_list.append(get_map(curr_filtered_df))
        assert(len(curr_compare_list) == nCols)
        map_list.append(curr_compare_list)
    assert(len(map_list) == nRows)
    return map_list
