# Standard imports
import numpy as np
import json
import logging
from dateutil import parser
import math

# Our imports
import emission.analysis.section_features as sf
import emission.analysis.plotting.gmaps.gmap_display as eapg
import emission.analysis.plotting.gmaps.pygmaps_modified as pygmaps
import emission.storage.decorations.useful_queries as taug
import emission.core.get_database as edb

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
        sectionJSON = edb.get_section_db().find_one({'_id': sid})
        if sectionJSON is None:
            logging.error("Unable to find section object for id %s" % sid)
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
        section = edb.get_section_db().find_one({'_id': sid})
        sectionCenter = taug.get_center_for_section(section)
        orig_points = section["track_points"]
        get_color = lambda(x) : "#000000" if x == "smooth_max_boundary" else "#0000FF"
        print "-------- %s ----------" % out_path
        for result in result_list:
            gmap = pygmaps.maps(sectionCenter[0], sectionCenter[1], 10)
            pruned_points = delete_points(orig_points, result.deleted_indices)
            section["track_points"] = pruned_points
            title = "%s: precision: %s, recall: %s" % (result.technique, result.precision, result.recall)
            eapg.drawSection(section, eapg.ALL, gmap, get_color(result.technique), title)
            print "%s len(orig_points) %s len(deleted_indices) %s len(retained_indices) %s" % (result, len(orig_points), len(result.deleted_indices), len(result.retained_indices))
            gmap.draw("%s/%s_%s.html" % (out_path, result.technique, section["_id"]))

def delete_points(track_points, deleted_indices):
    copied_points = []
    for (i, pt) in enumerate(track_points):
        if i not in deleted_indices:
            copied_points.append(pt)
    return copied_points

def smooth_boundary(tp, already_removed_indices = None, maxSpeed = 150):
    prev_pt = {}
    if already_removed_indices is None:
        removed_indices = []
    else:
        removed_indices = already_removed_indices

    prev_pt = None
    for (i, pt) in enumerate(tp):
        if already_removed_indices is not None and i in already_removed_indices:
            # We want to skip points that are already deleted. We can do this in one of two ways:
            # 1. Iterate over filtered_tp
            # 2. Iterate over regular tp, but skip points that have been deleted
            # We went with #2 because with #1, the points deleted by this pass
            # will have indices from filtered_tp, not tp, which means that they
            # can't be combined with removed_indices
            logging.debug("Point at index %d has already been deleted, skipping it..." % i)
            continue
        if prev_pt is None:
            # Don't have enough data yet, so don't make any decisions
            prev_pt = pt
        else:
            currSpeed = sf.calSpeed(prev_pt, pt)
            logging.debug("while considering point %s, speed = %s" % (i, currSpeed))
            # Should make this configurable
            if currSpeed > maxSpeed:
                logging.debug("currSpeed > 50, removing index %s " % (i))
                removed_indices.append(i)
            else:
                logging.debug("currSpeed < 50, retaining index %s " % (i))
                prev_pt = pt
    retained_indices = [set(range(len(tp))).difference(set(removed_indices))]
    return (retained_indices, removed_indices)

def smooth_deviation(tp):
    prev_pt = {}
    last_3_speeds = []
    removed_indices = []
    for (i, pt) in enumerate(tp):
        logging.debug("Considering point %s at index %s while prev_pt = %s" % (pt, i, prev_pt))
        if i == 0:
            prev_pt = pt
        else:
            currSpeed = sf.calSpeed(prev_pt, pt)
            if len(last_3_speeds) < 3:
                # We don't have enough data to filter, so let's keep adding
                if currSpeed > 0:
                    last_3_speeds.append(currSpeed)
                prev_pt = pt
            else:
                # Compare curr speed with standard deviation of average
                speed_array = np.array(last_3_speeds)
                avgSpeed = speed_array.mean()
                spdDeviation = np.std(speed_array)
                logging.debug("while considering point %s, currSpeed = %s, speed array = %s, avg speed = %s, spdDeviation = %s, combo = %s"
                         % (i, currSpeed, speed_array, avgSpeed, spdDeviation, avgSpeed + 3 * spdDeviation))
                if currSpeed > (avgSpeed + 3 * spdDeviation):
                    # Check to see if it is greater than the 3 following speeds as well
                    next_3_speeds = sf.calSpeedsForList(tp[i+1:i+4])
                    next_3_speeds = next_3_speeds[np.nonzero(next_3_speeds)]
                    avgSpeedAfter = next_3_speeds.mean()
                    spdDeviationAfter = np.std(next_3_speeds)
                    logging.debug("while considering after point %s, currSpeed = %s, speed array = %s, avg speed = %s, spdDeviation = %s, combo = %s"
                             % (i, currSpeed, next_3_speeds, avgSpeedAfter, spdDeviationAfter, avgSpeedAfter + 3 * spdDeviationAfter))
                    if (currSpeed > avgSpeedAfter + 3 * spdDeviationAfter):
                        logging.debug("currSpeed greater, removing index %s " % (i))
                        removed_indices.append(i)
                    else:
                        logging.debug("currSpeed before greater, but after lower, retaining index %s " % (i))
                        if (currSpeed > 0):
                            last_3_speeds.pop(0)
                            last_3_speeds.append(currSpeed)
                        prev_pt = pt
                else:
                    logging.debug("currSpeed lower, retaining index %s " % (i))
                    if (currSpeed > 0):
                        last_3_speeds.pop(0)
                        last_3_speeds.append(currSpeed)
                    prev_pt = pt

    retained_indices = [set(range(len(tp))).difference(set(removed_indices))]
    return (retained_indices, removed_indices)

def smooth_posdap(tp, already_deleted_indices = None, maxSpeed = 150):
    prev_pt = {}
    quality_segments = []
    curr_segment = []
    if already_deleted_indices is None:
        removed_indices = []
        filtered_tp = tp
    else:
        removed_indices = already_deleted_indices
        filtered_tp = delete_points(tp, already_deleted_indices)

    prev_pt = None

    for (i, pt) in enumerate(tp):
        if already_deleted_indices is not None and i in already_deleted_indices:
            # We want to skip points that are already deleted. We can do this in one of two ways:
            # 1. Iterate over filtered_tp
            # 2. Iterate over regular tp, but skip points that have been deleted
            # We went with #2 because with #1, the points deleted by this pass
            # will have indices from filtered_tp, not tp, which means that they
            # can't be combined with removed_indices
            logging.debug("Point at index %d has already been deleted, skipping it..." % i)
            continue
        if prev_pt is None:
            # Don't have enough data yet, so don't make any decisions
            prev_pt = pt
        else:
            currSpeed = sf.calSpeed(prev_pt, pt)
            logging.debug("while considering point %s, speed = %s" % (i, currSpeed))
            # Should make this configurable
            if currSpeed > maxSpeed:
                logging.debug("currSpeed > %d, starting new quality segment at index %s " % (maxSpeed, i))
                quality_segments.append(curr_segment)
                curr_segment = []
            else:
                logging.debug("currSpeed < %d, retaining index %s in existing quality segment " % (maxSpeed, i))
            prev_pt = pt
            curr_segment.append(i)
    # Append the last segment once we are at the end
    quality_segments.append(curr_segment)

    logging.debug("Number of quality segments is %d" % len(quality_segments))

    last_segment = quality_segments[0]
    for curr_segment in quality_segments[1:]:
        logging.debug("Considering segments %s and %s" % (last_segment, curr_segment))
        get_coords = lambda(i): tp[i]['track_location']['coordinates']
        get_ts = lambda(i): parser.parse(tp[i]['time'])
        # I don't know why they would use time instead of distance, but
        # this is what the existing POSDAP code does.
        logging.debug("About to compare curr_segment duration %s with last segment duration %s" % 
                        (get_ts(curr_segment[-1]) - get_ts(curr_segment[0]), 
                         get_ts(last_segment[-1]) - get_ts(last_segment[0])))
        if (get_ts(curr_segment[-1]) - get_ts(curr_segment[0]) <=
            get_ts(last_segment[-1]) - get_ts(last_segment[0])):
            logging.debug("curr segment is shorter, cut it")
            ref_idx = last_segment[-1]
            for curr_idx in curr_segment:
                logging.debug("Comparing distance %s with speed %s * time %s = %s" % 
                    (math.fabs(sf.calDistance(get_coords(ref_idx), get_coords(curr_idx))),
                     maxSpeed, abs(get_ts(ref_idx) - get_ts(curr_idx)).seconds,
                     maxSpeed * abs(get_ts(ref_idx) - get_ts(curr_idx)).seconds))
                if (math.fabs(sf.calDistance(get_coords(ref_idx), get_coords(curr_idx))) > 
                    (maxSpeed * abs(get_ts(ref_idx) - get_ts(curr_idx)).seconds)):
                    logging.debug("Distance is greater than max speed * time, deleting %s" % curr_idx)
                    removed_indices.append(curr_idx)
        else:
            logging.debug("prev segment is shorter, cut it")
            ref_idx = curr_segment[-1]
            for curr_idx in reversed(last_segment):
                logging.debug("Comparing distance %s with speed %s * time %s = %s" % 
                    (math.fabs(sf.calDistance(get_coords(ref_idx), get_coords(curr_idx))),
                     maxSpeed, abs(get_ts(ref_idx) - get_ts(curr_idx)).seconds,
                     maxSpeed * abs(get_ts(ref_idx) - get_ts(curr_idx)).seconds))
                if (abs(sf.calDistance(get_coords(ref_idx), get_coords(curr_idx))) > 
                    (maxSpeed *  abs(get_ts(ref_idx) - get_ts(curr_idx)).seconds)):
                    logging.debug("Distance is greater than max speed * time, deleting %s" % curr_idx)
                    removed_indices.append(curr_idx)
        last_segment = curr_segment

    retained_indices = [set(range(len(tp))).difference(set(removed_indices))]
    return (retained_indices, removed_indices)

def smooth_max_boundary(tp, already_removed_indices=None):
    if already_removed_indices is None:
        filtered_tp = tp
    else:
        filtered_tp = delete_points(tp, already_removed_indices)

    all_speeds = sf.calSpeedsForList(filtered_tp)
    all_nonzero_speeds = all_speeds[np.nonzero(all_speeds)]
    np.set_printoptions(suppress=True)
    logging.debug("all_nonzero_speeds = %s" % all_nonzero_speeds)
    logging.debug("mean = %s, std = %s, mean + std = %s " % 
        (np.mean(all_nonzero_speeds), np.std(all_nonzero_speeds), np.mean(all_nonzero_speeds) + np.std(all_nonzero_speeds)))
    logging.debug("percentiles (90, 95, 99) = %s" % (np.percentile(all_nonzero_speeds, [90, 95, 99])))
    return smooth_boundary(tp, already_removed_indices, maxSpeed = np.percentile(all_nonzero_speeds, 95))

def smooth_max_posdap(tp, already_removed_indices = None):
    if already_removed_indices is None:
        filtered_tp = tp
    else:
        filtered_tp = delete_points(tp, already_removed_indices)
    all_speeds = sf.calSpeedsForList(filtered_tp)
    all_nonzero_speeds = all_speeds[np.nonzero(all_speeds)]
    np.set_printoptions(suppress=True)
    logging.debug("all_nonzero_speeds = %s" % all_nonzero_speeds)
    logging.debug("mean = %s, std = %s, mean + std = %s " % 
        (np.mean(all_nonzero_speeds), np.std(all_nonzero_speeds), np.mean(all_nonzero_speeds) + np.std(all_nonzero_speeds)))
    logging.debug("percentiles (90, 95, 99) = %s" % (np.percentile(all_nonzero_speeds, [90, 95, 99])))
    return smooth_posdap(tp, already_removed_indices, maxSpeed = np.percentile(all_nonzero_speeds, 95))

def smooth_zigzag_boundary(tp):
    removed_indices = strip_zigzag_points(tp)
    logging.debug("In first step, removed indices %s "% removed_indices)
    (retained_indices, deleted_indices) = smooth_max_boundary(tp, removed_indices)
    # We have added removed_indices in two stages, so they may not be in order.
    # Let us sort now to ensure that they are
    deleted_indices = sorted(deleted_indices)
    return (retained_indices, deleted_indices)

def smooth_zigzag_posdap(tp):
    removed_indices = strip_zigzag_points(tp)
    logging.debug("In first step, removed indices %s "% removed_indices)
    (retained_indices, deleted_indices) = smooth_max_posdap(tp, removed_indices)
    # We have added removed_indices in two stages, so they may not be in order.
    # Let us sort now to ensure that they are
    deleted_indices = sorted(deleted_indices)
    return (retained_indices, deleted_indices)

def strip_zigzag_points(tp):
    """
        Return array with zigzags that return to the same point stripped out.
        For this:
        - we find the set of unique points, and find the array for each.
        - then, we find the first gap in each array.
        - all other points in the array should be deleted
    """
    # In our data structure each coordinate pair is a list of floats.
    # Unfortunately, lists are not hashable
    # In [180]: testDict[[1,2]] = "foo"
    # ---------------------------------------------------------------------------
    # TypeError                                 Traceback (most recent call last)
    # <ipython-input-180-bbe83ea26088> in <module>()
    # ----> 1 testDict[[1,2]] = "foo"
    # TypeError: unhashable type: 'list'

    # However, tuples are:
    # In [181]: testDict[(1,2)] = "foo"
    # In [182]: 
    
    # So we will convert the lists to tuples before creating the hash table

    duplicatesMap = {}
    removed_indices = []

    totuple = lambda(ca) : (ca[0], ca[1])
    tocoordtuple = lambda(pt): totuple(pt["track_location"]["coordinates"])

    for (i, pt) in enumerate(tp):
        # logging.debug("Converting %s to tuple" % pt)
        ptuple = tocoordtuple(pt)
        if ptuple not in duplicatesMap:
            duplicatesMap[ptuple] = [i]
        else:
            duplicatesMap[ptuple].append(i)

    for (ptuple, indexList) in duplicatesMap.iteritems():
        logging.debug("Removing non consecutive points for %s" % indexList)
        remove_non_consecutive(indexList, removed_indices)

    return removed_indices

def remove_non_consecutive(indexList, removed_indices):
    """
        Add non consecutive indices to the removed list.
    """
    consecutive = True
    prev_index = indexList[0]
    for (i, index) in enumerate(indexList[1:]):
        if consecutive:
            # Check to see if it is still consecutive
            if index == prev_index + 1:
                logging.debug("While considering index %s, still consecutive, keep going", index)
                prev_index = index
            else:
                consecutive = False
                removed_indices.append(index)
        else:
            removed_indices.append(index)

technique_list = [smooth_max_boundary, smooth_zigzag_boundary]

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    evaluate_all_smoothing_for_all_clusters()
