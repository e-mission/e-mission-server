import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import cm
import folium
import branca.colormap as clm
import load_predict as predict

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# def get_scatter(percentage,homo_score,valid_users):
#     x=percentage
#     y=homo_score
#     v=valid_users
#     cmp = cm.get_cmap('Dark2', len(valid_users))
#
#     sc = []
#     for i in range(len(valid_users)):
#         for n in range(len(x[i])):
#             point = plt.scatter(x[i][n], y[i][n], color=cmp.colors[i], s=70, alpha=0.7)
#         sc.append(point)
#     plt.legend(sc,v,markerscale=0.8,scatterpoints=1,bbox_to_anchor=(1.23,1))
#     plt.xlabel('user input request percentage',fontsize=16)
#     plt.ylabel('homogeneity score',fontsize=16)
#     plt.xticks(np.arange(0.4,1.1,step=0.1),fontsize=14)
#     plt.yticks(np.arange(0.2,1.1,step=0.1),fontsize=14)
def get_scatter(valid_users,collect_filename,first_round,second_round):
    all_filename = predict.loadModelStage()
    sc = []
    for i in range(len(valid_users)):
        df = pd.read_csv(all_filename[i], index_col='split')
        plot_scatter(df, valid_users[i], i, sc, first_round, second_round)
    plt.legend(sc, valid_users, markerscale=0.8, scatterpoints=1, bbox_to_anchor=(1.1, 1))
    plt.show()

def plot_scatter(result_df,valid_user,user_index,sc,first_round,second_round):
    v = valid_user
    cmp = cm.get_cmap('Dark2', len(valid_users))

    if first_round:
        x = result_df['percentage of 1st round']
        y = result_df['homogeneity socre of 1st round']
    elif second_round:
        x = result_df['percentage of 2nd round']
        y = result_df['homogeneity socre of 2nd round']
    # print(x)
    point = plt.scatter(x, y, color=cmp.colors[user_index], s=70, alpha=0.7)
    sc.append(point)

    # plt.legend(sc,v,markerscale=0.8,scatterpoints=1,bbox_to_anchor=(1.23,1))
    # plt.legend(sc, v, markerscale=0.8, scatterpoints=1,bbox_to_anchor=(1.15,1))
    plt.xlabel('user input request percentage',fontsize=16)
    plt.ylabel('homogeneity score',fontsize=16)
    plt.xticks(np.arange(0.4,1.1,step=0.1),fontsize=14)
    plt.yticks(np.arange(0.2,1.1,step=0.1),fontsize=14)



    # for i in range(len(valid_users)):
    #     for n in range(len(x[i])):
    #         point = plt.scatter(x[i][n], y[i][n], color=cmp.colors[i], s=70, alpha=0.7)
    #     sc.append(point)
    # plt.legend(sc,v,markerscale=0.8,scatterpoints=1,bbox_to_anchor=(1.23,1))
    # plt.xlabel('user input request percentage',fontsize=16)
    # plt.ylabel('homogeneity score',fontsize=16)
    # plt.xticks(np.arange(0.4,1.1,step=0.1),fontsize=14)
    # plt.yticks(np.arange(0.2,1.1,step=0.1),fontsize=14)


def same_cluster_map(cluster,filter_trips,bins):
    color_map = clm.linear.Set1_07.to_step(len(bins), index=[i for i in range(len(bins)+1)])
    first_trip = filter_trips[cluster[0]]
    map = folium.Map(location=[first_trip.data.start_loc["coordinates"][1], first_trip.data.start_loc["coordinates"][0]],
                   zoom_start=12, max_zoom=30, control_scale=True)

    zoom_points = []
    for curr_trip_index in cluster:
        for i in range(len(bins)):
            curr_trip = filter_trips[curr_trip_index]
            if curr_trip_index in bins[i]:
                # We need polyline to plot the trip according to end_loc
                # We are more interested in the end points of particular trips(e.g. home) than the start points
                # Flip indices because points are in geojson (i.e. lon, lat),folium takes [lat,lon]
                end_points = folium.CircleMarker([curr_trip.data.end_loc["coordinates"][1], curr_trip.data.end_loc["coordinates"][0]],
                                    radius=3,color='red',fill=True,fill_color='red',fill_opacity=1)
                end_points.add_to(map)
                zoom_points.append([curr_trip.data.start_loc["coordinates"][1],
                                    curr_trip.data.start_loc["coordinates"][0]])
                zoom_points.append([curr_trip.data.end_loc["coordinates"][1],
                                    curr_trip.data.end_loc["coordinates"][0]])
    df = pd.DataFrame(zoom_points, columns=['Lat', 'Long'])
    sw = df[['Lat', 'Long']].min().values.tolist()
    ne = df[['Lat', 'Long']].max().values.tolist()
    map.fit_bounds([sw, ne])

    return map

if __name__ == '__main__':
    valid_users=['user1', 'user3']
    collect_filename = predict.loadModelStage("collect_filename")
    plt.figure()
    get_scatter(valid_users, collect_filename, first_round=True, second_round=False)
