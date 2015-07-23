import pandas as pd
import folium

def get_map_list(df, potential_splits):
    mapList = []
    potential_splits_list = list(potential_splits)
    for start, end in zip(potential_splits_list, potential_splits_list[1:]):
        trip = df[start:end]
        currMap = folium.Map([trip.mLatitude.mean(), trip.mLongitude.mean()])
        plot_point = lambda row: currMap.simple_marker([row['mLatitude'], row['mLongitude']], popup='%s' % row)
        trip.apply(plot_point, axis=1)
        currMap.line(zip(list(trip.mLatitude), list(trip.mLongitude)))
        mapList.append(currMap)
    return mapList
