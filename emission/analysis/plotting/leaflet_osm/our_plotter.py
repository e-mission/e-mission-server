import pandas as pd
import folium

def df_to_string_list(df):
    """
    Convert the input df into a list of strings, suitable for using as popups in a map.
    This is a utility function.
    """
    print "Converting df with size %s to string list" % df.shape[0]
    array_list = df.as_matrix().tolist()
    return [str(line) for line in array_list]

def get_map_list(df, potential_splits):
    mapList = []
    potential_splits_list = list(potential_splits)
    for start, end in zip(potential_splits_list, potential_splits_list[1:]):
        trip = df[start:end]
        currMap = folium.Map([trip.mLatitude.mean(), trip.mLongitude.mean()])
        currMap.div_markers(trip[['mLatitude', 'mLongitude']].as_matrix().tolist(),
            df_to_string_list(trip[['mLatitude', 'mLongitude', 'formatted_time', 'mAccuracy']]))
        currMap.line(trip[['mLatitude', 'mLongitude']].as_matrix().tolist())
        mapList.append(currMap)
    return mapList
