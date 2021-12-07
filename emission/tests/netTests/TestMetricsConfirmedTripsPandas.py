import unittest
import pandas as pd
import numpy as np

import emission.storage.decorations.trip_queries as esdt

class TestMetricsConfirmedTripsPandas(unittest.TestCase):

    # Pandas currently ignores NaN entries in groupby
    def testPandasNaNHandlingAndWorkaround(self):
        test_df = pd.DataFrame({"id": [1,2,3,4,5,6],
            "mode_confirm": ["walk", "bike", "bike", "walk", np.NaN, np.NaN]})

        # Current pandas behavior ignores NaN
        orig_grouping = test_df.groupby("mode_confirm").groups
        self.assertEqual(list(orig_grouping.keys()), ["bike", "walk"])

        # workaround replaces NaN with "unknown"
        new_test_df = test_df.fillna("unknown")

        # Now we should not ignore NaN
        new_grouping = new_test_df.groupby("mode_confirm").groups
        self.assertEqual(list(new_grouping.keys()), ["bike", "unknown", "walk"])

    # Pandas currently ignores NaN entries in groupby
    def testPandasConcatModeConfirm(self):
        test_df = pd.DataFrame({"id": [1,2,3,4,5,6], "user_input": [{}] * 6})

        # unlabeled trips result in no additional columns
        expanded_test_df = esdt.expand_userinputs(test_df)
        self.assertNotIn("mode_confirm", expanded_test_df.columns)

        dummy_col = pd.Series([np.NaN] * len(expanded_test_df), name="mode_confirm")
        self.assertEqual(len(dummy_col), len(expanded_test_df))
        filled_expanded_test_df = pd.concat([expanded_test_df, dummy_col],
            axis = 1, copy=False)

        self.assertIn("mode_confirm", filled_expanded_test_df.columns)
        self.assertEqual(len(filled_expanded_test_df.mode_confirm), len(dummy_col))

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
