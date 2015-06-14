import modeinfer.featurecalc as fc
import numpy as np
import main.gmap_display as mgp
import main.pygmaps_modified as pygmaps
import useful_queries as taug
import json
from get_database import get_section_db

class SmoothingEvalResult:
    def __str__(self):
        return ("%s, %s: precision %s, recall %s, false_positives %s, false_negatives %s" %
                (self._id, self.technique, self.precision, self.recall, self.false_positives,
                    self.false_negatives))

    def __repr__(self):
        return ("%s, %s: precision %s, recall %s, false_positives %s, false_negatives %s" %
                (self._id, self.technique, self.precision, self.recall, self.false_positives,
                    self.false_negatives))

ground_truth_map_list = [
    ("/Users/shankari/cluster_ground_truth/smoothing/eagle_park/smoothing_removed_points",
     "/Users/shankari/cluster_ground_truth/smoothing/eagle_park/smoothing_output"),
    ("/Users/shankari/cluster_ground_truth/smoothing/caltrain/smoothing_removed_points",
     "/Users/shankari/cluster_ground_truth/smoothing/caltrain/smoothing_output"),
]

def smooth_track_points(section, smooth_technique):
    """
        Returns a tuple of (retained_indices, deleted_indices)
    """
    tp = section["track_points"]
    # Returns a tuple of (retained indices, removed indices)
    return smooth_technique(tp)

def evaluate_smoothing_for_section(section, ground_truth_deleted_indices, smooth_technique):
    """
        Returns a class with the following fields:
        - section id
        - technique
        - retained_indices
        - deleted_indices
        - recall
        - precision 
        - false negatives
        - false positives
    """
    (retained_indices, deleted_indices) = smooth_track_points(section, smooth_technique)
    overlap = set(deleted_indices).intersection(set(ground_truth_deleted_indices))
    result = SmoothingEvalResult()
    result._id = section["_id"]
    result.technique = smooth_technique.func_name
    result.retained_indices = retained_indices
    result.deleted_indices = deleted_indices
    if len(ground_truth_deleted_indices) > 0:
        result.recall = float(len(overlap)) / len(ground_truth_deleted_indices) 
    else:
        result.recall = 0

    if len(deleted_indices) > 0:
        result.precision = float(len(overlap)) / len(deleted_indices)
    else:
        result.precision = 0

    result.false_negatives = set(ground_truth_deleted_indices).difference(overlap)
    result.false_positives = set(deleted_indices).difference(overlap)
    return result

def evaluate_smoothing_for_technique(ground_truth_map, smooth_technique):
    result_list = []
    for (sid, ground_truth_deleted_indices) in ground_truth_map.iteritems():
        sectionJSON = get_section_db().find_one({'_id': sid})
        result_list.append(evaluate_smoothing_for_section(sectionJSON, ground_truth_deleted_indices, smooth_technique))
    return result_list

def evaluate_all_smoothing(ground_truth_map):
    result_map = {}
    for technique in technique_list:
        result_map[technique.func_name] = evaluate_smoothing_for_technique(ground_truth_map, technique)
    return result_map

def evaluate_all_smoothing_for_all_clusters():
    for (path, result_path) in ground_truth_map_list:
        ground_truth_map = json.load(open(path))
        print 30 * "="
        print "For path %s, results are" % path
        result_map = evaluate_all_smoothing(ground_truth_map)
        print result_map
        print 30 * "="
        print_result_maps(result_path, result_map)

def print_result_maps(out_path, result_map):
    section_map = {}
    for (technique_name, result_list) in result_map.iteritems():
        for result in result_list:
            if result._id in section_map:
                section_map[result._id].append(result)
            else:
                section_map[result._id] = [result]

    for (sid, result_list) in section_map.iteritems():
        section = get_section_db().find_one({'_id': sid})
        sectionCenter = taug.get_center_for_section(section)
        gmap = pygmaps.maps(sectionCenter[0], sectionCenter[1], 10)
        orig_points = section["track_points"]
        get_color = lambda(x) : "#000000" if x == "smooth_boundary" else "#0000FF"
        print "-------- %s ----------" % out_path
        for result in result_list:
            pruned_points = delete_points(orig_points, result.deleted_indices)
            section["track_points"] = pruned_points
            title = "%s: precision: %s, recall: %s" % (result.technique, result.precision, result.recall)
            mgp.drawSection(section, mgp.ALL, gmap, get_color(result.technique), title)
            print "%s len(orig_points) %s len(deleted_indices) %s len(retained_indices) %s" % (result, len(orig_points), len(result.deleted_indices), len(result.retained_indices))
        gmap.draw("%s/%s.html" % (out_path, section["_id"]))

def delete_points(track_points, deleted_indices):
    copied_points = []
    for (i, pt) in enumerate(track_points):
        if i not in deleted_indices:
            copied_points.append(pt)
    return copied_points

def smooth_boundary(tp):
    prev_pt = {}
    removed_indices = []
    for (i, pt) in enumerate(tp):
        if i == 0:
            # Don't have enough data yet, so don't make any decisions
            prev_pt = pt
        else:
            currSpeed = fc.calSpeed(prev_pt, pt)
            # Should make this configurable
            if currSpeed > 50:
                removed_indices.append(i)
            else:
                prev_pt = pt
    retained_indices = [set(range(len(tp))).difference(set(removed_indices))]
    return (retained_indices, removed_indices)

def smooth_deviation(tp):
    prev_pt = {}
    last_3_speeds = []
    removed_indices = []
    for (i, pt) in enumerate(tp):
        if i == 0:
            prev_pt = pt
        else:
            currSpeed = fc.calSpeed(prev_pt, pt)
            if len(last_3_speeds) < 3:
                # We don't have enough data to filter, so let's keep adding
                last_3_speeds.append(currSpeed)
                prev_pt = pt
            else:
                # Compare curr speed with standard deviation of average
                speed_array = np.array(last_3_speeds)
                avgSpeed = speed_array.mean()
                spdDeviation = np.std(speed_array)
                if currSpeed > (avgSpeed + spdDeviation):
                    removed_indices.append(i)
                else:
                    last_3_speeds.append(currSpeed)
                    prev_pt = pt

    retained_indices = [set(range(len(tp))).difference(set(removed_indices))]
    return (retained_indices, removed_indices)

technique_list = [smooth_boundary, smooth_deviation]

if __name__ == '__main__':
    evaluate_all_smoothing_for_all_clusters()
