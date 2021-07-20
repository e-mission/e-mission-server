import emission.analysis.modelling.tour_model.label_processing as lp
import emission.analysis.modelling.tour_model.data_preprocessing as preprocess
import copy

"""
This class is used in evaluation_pipeline. It takes the result from the first round for further analysis. 
The logic for the second round of clustering:
Each bin from the first round of clustering goes through the hierarchical(agglomerative）clustering (second round)
and is assigned a label. After that, the number of clusters in each bin (n_cluster) will be passed in Kmeans algorithm.
Then, the data in the bin will be assigned a new label. 

Hierarchical clustering and Kmeans do similar thing here. 
Since we use Kmeans for model building later(hierarchical clustering doesn't have "fit" and "predict" separated),
for better evaluation, we use the same clustering algorithm (Kmeans) here.
Also, Kmeans takes n_cluster as parameter. We need to run hierarchical clustering first to get number of clusters.
The tuning parameters passed in the second round are to tune hierarchical clustering.

The main goal for the second round of clustering is to get homogeneity scores and the percentages of user label request.
"""



class SecondRoundOfClustering(object):
    def __init__(self, data, first_labels):
        if not data:
            self.data = []
        self.data = data
        self.new_labels = copy.copy(first_labels)


    def get_sel_features_and_trips(self,first_labels,l):
        # store second round trips data
        second_round_trips = []
        # create a track to store indices and labels for the second round
        second_round_idx_labels = []
        for index, first_label in enumerate(first_labels):
            if first_label == l:
                second_round_trips.append(self.data[index])
                second_round_idx_labels.append([index, first_label])
        x = preprocess.extract_features(second_round_trips)
        self.x = x
        self.second_round_trips = second_round_trips
        self.second_round_idx_labels = second_round_idx_labels

    # We choose single-linkage clustering.
    # See examples and explanations at https://en.wikipedia.org/wiki/Single-linkage_clustering
    # It is based on grouping clusters in bottom-up fashion (agglomerative clustering),
    # at each step combining two clusters that contain the closest pair of elements not yet belonging
    # to the same cluster as each other.
    def hierarcial_clustering(self,low,dist_pct):
        method = 'single'
        # get the second label from the second round of clustering using hierarchical clustering
        second_labels = lp.get_second_labels(self.x, method, low, dist_pct)
        self.second_labels = second_labels

    # for test set, we use kmeans to re-run the clustering, then evaluate it later
    def kmeans_clustering(self):
        second_labels = lp.kmeans_clusters(self.second_labels, self.x)
        self.second_labels = second_labels

    # concatenate the first label (label from the first round) and the second label (label
    # from the second round) (e.g.first label[1,1,1], second label[1,2,3], new_labels is [11,12,13]
    def get_new_labels(self,first_labels):
        # new_labels temporary stores the labels from the first round, but later the labels in new_labels will be
        # updated with the labels after two rounds of clustering.
        new_labels = lp.get_new_labels(self.second_labels, self.second_round_idx_labels, self.new_labels)
        self.new_labels = new_labels
        return self.new_labels

    # change the labels in track with new_labels
    def get_new_track(self,track):
        track = lp.change_track_labels(track, self.new_labels)
        self.track = track
        return self.track


