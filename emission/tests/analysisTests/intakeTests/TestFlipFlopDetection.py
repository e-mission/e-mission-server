import unittest
import attrdict as ad

# our imports
import emission.core.wrapper.motionactivity as ecwm
import emission.analysis.intake.segmentation.section_segmentation_methods.flip_flop_detection as eaissf

# Test imports
import emission.tests.common as etc

class TestFlipFlopDetection(unittest.TestCase):
    def test_GetStreakOne(self):
        ffd = eaissf.FlipFlopDetection([], None)
        flip_flop_list = [False, False, False, True, True, False, False, False]
        sss_list = ffd.get_streaks(flip_flop_list)
        self.assertEqual(sss_list, [(3, 4)])

    def test_GetStreakMixed(self):
        ffd = eaissf.FlipFlopDetection([], None)
        flip_flop_list = [False, False, True, False, False, False, False, True, False, True, False, True, True, False, False]
        sss_list = ffd.get_streaks(flip_flop_list)
        self.assertEqual(sss_list, [(2,2), (7,7), (9,9), (11, 12)])

    def test_GetStreakLast(self):
        ffd = eaissf.FlipFlopDetection([], None)
        flip_flop_list = [False, False, True, False, False, False, False, True, False, True, False, True, True]
        sss_list = ffd.get_streaks(flip_flop_list)
        self.assertEqual(sss_list, [(2,2), (7,7), (9,9), (11,11)])

    def test_GetStreakOneFF(self):
        ffd = eaissf.FlipFlopDetection([], None)

        flip_flop_list = [False]
        sss_list = ffd.get_streaks(flip_flop_list)
        self.assertEqual(sss_list, [])

        flip_flop_list = [True]
        sss_list = ffd.get_streaks(flip_flop_list)
        self.assertEqual(sss_list, [(0,0)])

        flip_flop_list = [True, False]
        sss_list = ffd.get_streaks(flip_flop_list)
        self.assertEqual(sss_list, [(0,0)])

        flip_flop_list = [False, True]
        sss_list = ffd.get_streaks(flip_flop_list)
        self.assertEqual(sss_list, [(1,1)])

    def test_MergeStreaksPass1(self):
        ffd = eaissf.FlipFlopDetection([], None)
        unmerged_change_list = [({'idx': 'a'}, {'idx': 'b'}),
                                ({'idx': 'b'}, {'idx': 'c'}),
                                ({'idx': 'c'}, {'idx': 'd'}),
                                ({'idx': 'd'}, {'idx': 'e'}),
                                ({'idx': 'e'}, {'idx': 'f'}),
                                ({'idx': 'f'}, {'idx': 'g'}),
                                ({'idx': 'g'}, {'idx': 'h'}),
                                ({'idx': 'h'}, {'idx': 'i'})]
        forward_merged_list = [
            ad.AttrDict({"start": 1, "end": 2, "final_mode": ecwm.MotionTypes.IN_VEHICLE}),
            ad.AttrDict({"start": 6, "end": 6, "final_mode": ecwm.MotionTypes.WALKING})
        ]
        backward_merged_list = [
            ad.AttrDict({"start": 4, "end": 4, "final_mode": ecwm.MotionTypes.BICYCLING})
        ]
        ret_list = ffd.merge_streaks_pass_1(unmerged_change_list, forward_merged_list,
                                 backward_merged_list, [])
        self.assertEqual(ret_list, [({'idx': 'a'}, {'idx': 'd'}),
                                    ({'idx': 'd'}, {'idx': 'e'}),
                                    ({'idx': 'e'}, {'idx': 'h'}),
                                    ({'idx': 'h'}, {'idx': 'i'})])

        unmerged_change_list = [({'idx': 'a'}, {'idx': 'b'}),
                                ({'idx': 'b'}, {'idx': 'c'}),
                                ({'idx': 'c'}, {'idx': 'd'}),
                                ({'idx': 'd'}, {'idx': 'e'}),
                                ({'idx': 'e'}, {'idx': 'f'}),
                                ({'idx': 'f'}, {'idx': 'g'}),
                                ({'idx': 'g'}, {'idx': 'h'}),
                                ({'idx': 'h'}, {'idx': 'i'}),
                                ({'idx': 'k'}, {'idx': 'l'}),
                                ({'idx': 'm'}, {'idx': 'n'}),
                                ({'idx': 'o'}, {'idx': 'p'})]

        forward_merged_list = [
            ad.AttrDict({"start": 1, "end": 2, "final_mode": ecwm.MotionTypes.IN_VEHICLE}),
            ad.AttrDict({"start": 6, "end": 6, "final_mode": ecwm.MotionTypes.WALKING})
        ]
        backward_merged_list = [
            ad.AttrDict({"start": 4, "end": 4, "final_mode": ecwm.MotionTypes.BICYCLING})
        ]
        new_merged_list = [
            ad.AttrDict({"start": 8, "end": 10, "final_mode": ecwm.MotionTypes.BICYCLING})
        ]
        ret_list = ffd.merge_streaks_pass_1(unmerged_change_list, forward_merged_list,
                                 backward_merged_list, new_merged_list)
        self.assertEqual(ret_list, [({'idx': 'a'}, {'idx': 'd'}),
                                    ({'idx': 'd'}, {'idx': 'e'}),
                                    ({'idx': 'e'}, {'idx': 'h'}),
                                    ({'idx': 'h'}, {'idx': 'i'}),
                                    ({'idx': 'k', 'type': ecwm.MotionTypes.BICYCLING}, {'idx': 'p'})])

    def test_MergeStreaksPass2(self):
        ffd = eaissf.FlipFlopDetection([], None)
        unmerged_change_list = [
            (ad.AttrDict({'type': ecwm.MotionTypes.WALKING}),
                ad.AttrDict({'type': ecwm.MotionTypes.IN_VEHICLE})),
            (ad.AttrDict({'type': ecwm.MotionTypes.IN_VEHICLE}),
                ad.AttrDict({'type': ecwm.MotionTypes.WALKING})),
            (ad.AttrDict({'type': ecwm.MotionTypes.WALKING}),
                ad.AttrDict({'type': ecwm.MotionTypes.WALKING})),
            (ad.AttrDict({'type': ecwm.MotionTypes.WALKING}),
                ad.AttrDict({'type': ecwm.MotionTypes.WALKING})),
            (ad.AttrDict({'type': ecwm.MotionTypes.WALKING}),
                ad.AttrDict({'type': ecwm.MotionTypes.WALKING}))]

        ret_list = ffd.merge_streaks_pass_2(unmerged_change_list)
        self.assertEqual(ret_list, [
            (ad.AttrDict({'type': ecwm.MotionTypes.WALKING}),
                ad.AttrDict({'type': ecwm.MotionTypes.IN_VEHICLE})),
            (ad.AttrDict({'type': ecwm.MotionTypes.IN_VEHICLE}),
                ad.AttrDict({'type': ecwm.MotionTypes.WALKING})),
            (ad.AttrDict({'type': ecwm.MotionTypes.WALKING}),
                ad.AttrDict({'type': ecwm.MotionTypes.WALKING}))])

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
