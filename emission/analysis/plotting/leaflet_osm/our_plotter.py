import pandas as pd
import folium

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
        currMap = folium.Map([trip.mLatitude.mean(), trip.mLongitude.mean()])
        currMap.div_markers(trip[['mLatitude', 'mLongitude']].as_matrix().tolist(),
            df_to_string_list(trip), marker_size=5)
        currMap.line(trip[['mLatitude', 'mLongitude']].as_matrix().tolist())
        mapList.append(currMap)
    return mapList
