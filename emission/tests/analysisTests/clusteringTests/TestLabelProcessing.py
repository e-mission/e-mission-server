from future import standard_library
standard_library.install_aliases()
import unittest
import emission.analysis.modelling.tour_model.label_processing as eamtl
import pandas as pd
import emission.tests.common as etc
import sklearn.cluster as sc
import numpy as np


class TestLabelProcessing(unittest.TestCase):

    def test_map_labels_sp2en(self):
        mode = ['placas_de carro','aseguranza','iglesia']
        user_input_df = pd.DataFrame(data={'mode':mode})
        user_input_df = eamtl.map_labels_sp2en(user_input_df)
        compare_mode = ['car plates','insurance','church']
        compare_df = pd.DataFrame(data={'mode':compare_mode})
        pd.testing.assert_frame_equal(user_input_df,compare_df)

    def test_map_labels_purpose(self):
        purpose = ['course','work_- lunch break','on_the way home','insurance_payment']
        user_input_df = pd.DataFrame(data={'purpose': purpose})
        compare_purpose = ['school','lunch_break','home','insurance']
        compare_df = pd.DataFrame(data={'purpose': compare_purpose})
        user_input_df = eamtl.map_labels_purpose(user_input_df)
        pd.testing.assert_frame_equal(user_input_df,compare_df)


    def test_map_labels_mode(self):
        mode_confirm = ['bike','ebike']
        replaced_mode = ['same_mode','walk']
        dict = {'mode_confirm':mode_confirm,'replaced_mode':replaced_mode}
        user_input_df = pd.DataFrame(dict)
        user_input_df = eamtl.map_labels_mode(user_input_df)
        compare_replaced_mode = ['bike','walk']
        compare_dict = {'mode_confirm':mode_confirm,'replaced_mode':compare_replaced_mode}
        compare_df = pd.DataFrame(compare_dict)
        pd.testing.assert_frame_equal(user_input_df,compare_df)

    def test_map_labels(self):
        mode_confirm = ['bike']
        purpose_confirm = ['iglesia']
        replaced_mode = ['same_mode']
        user_input = {'mode_confirm':mode_confirm,'purpose_confirm':purpose_confirm,'replaced_mode':replaced_mode}
        user_input_df = pd.DataFrame(user_input)
        user_input_df = eamtl.map_labels(user_input_df)
        compare_purpose_confirm = ['church']
        compare_replaced_mode = ['bike']
        compare_dict = {'mode_confirm':mode_confirm,'purpose_confirm':compare_purpose_confirm,'replaced_mode':compare_replaced_mode}
        compare_df = pd.DataFrame(compare_dict)
        pd.testing.assert_frame_equal(user_input_df,compare_df)

    def test_get_second_labels(self):
        x1 = [[1,2,3,4],[2,2,3,4],[3,3,3,3],[1,2,3,4]]
        x2 = [[1,1,1,1],[18,33,57,20],[30,34,67,3],[40,20,3,4]]
        method = 'single'
        low = 50
        dist_pct = 0.6
        # if features are close
        labels1 = eamtl.get_second_labels(x1, method, low, dist_pct)
        labels2 = eamtl.get_second_labels(x2, method, low, dist_pct)
        self.assertEqual(labels1, [0, 0, 0, 0])
        self.assertEqual(labels2.tolist(), [2,1,1,3])


    def test_kmeans_clusters(self):
        clusters = [1, 1, 1, 0, 0, 0]
        x = np.array([[1, 2], [1, 4], [1, 0],[10, 2], [10, 4], [10, 0]])
        n_clusters = len(set(clusters))
        k_clusters = eamtl.kmeans_clusters(clusters, x)
        self.assertEqual(k_clusters.tolist(), [1,1,1,0,0,0])


    def test_get_new_labels(self):
        second_labels = [2,1,1,3]
        second_round_idx_labels =[[0,1],[1,1],[2,1],[3,2]]
        new_labels = [1,1,1,2,3,3,3,3]
        new_labels = eamtl.get_new_labels(second_labels, second_round_idx_labels, new_labels)
        self.assertEqual(new_labels, [12, 11, 11, 23, 3, 3, 3, 3])

    def test_group_similar_trips(self):
        new_labels = [12, 11, 11, 23, 31, 31, 32, 32]
        track = [[11,12],[15,11],[20,11],[50,23],[57,31],[59,31],[67,32],[69,32]]
        new_bins = eamtl.group_similar_trips(new_labels,track)
        self.assertEqual(new_bins, [[67, 69], [15, 20], [11], [50], [57, 59]])

    def test_change_track_labels(self):
        track = [[11,1],[15,1],[20,1],[50,2],[57,3],[59,3],[67,3],[69,3]]
        new_labels = [12, 11, 11, 23, 31, 31, 32, 32]
        track = eamtl.change_track_labels(track,new_labels)
        self.assertEqual(track, [[11, 12], [15, 11], [20, 11], [50, 23], [57, 31], [59, 31], [67, 32], [69, 32]])

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()


