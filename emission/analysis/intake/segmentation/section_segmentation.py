from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import logging

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl
import emission.core.wrapper.section as ecwc
import emission.core.wrapper.stop as ecws
import emission.core.wrapper.entry as ecwe

import emission.core.common as ecc

class SectionSegmentationMethod(object):
    def segment_into_sections(self, timeseries, distance_from_place, time_query):
        """
        Examines the timeseries database for a specific range and returns the
        points at which the trip needs to be segmented. Again, this allows
        algorithms to use whatever combination of data that they want from the sensor
        streams in order to determine segmentation points.

        Returns an array of location point tuples corresponding to the start
        and end of sections in this time range. The first section returned
        starts with the first location pointi n the time range.
        i.e. [(start1, end1), (start2, end2),...]. end_n and start_n+1 are
        generally assumed to be consecutive to avoid large holes in the data
        collection.

        If there are no segments, returns an empty array.
        TODO: Figure out if there can be a method that is not yet ready to segment into sections.
        """
        pass

def segment_current_sections(user_id):
    time_query = epq.get_time_range_for_sectioning(user_id)
    try:
        trips_to_process = esda.get_entries(esda.RAW_TRIP_KEY, user_id, time_query)
        for trip_entry in trips_to_process:
            logging.info("+" * 20 + ("Processing trip %s for user %s" % (trip_entry.get_id(), user_id)) + "+" * 20)
            segment_trip_into_sections(user_id, trip_entry, trip_entry.data.source)
        if len(trips_to_process) == 0:
            # Didn't process anything new so start at the same point next time
            last_trip_processed = None
        else:    
            last_trip_processed = trips_to_process[-1]
        epq.mark_sectioning_done(user_id, last_trip_processed)
    except:
        logging.exception("Sectioning failed for user %s" % user_id)
        epq.mark_sectioning_failed(user_id)

def segment_trip_into_sections(user_id, trip_entry, trip_source):
    ts = esta.TimeSeries.get_time_series(user_id)
    time_query = esda.get_time_query_for_trip_like(esda.RAW_TRIP_KEY, trip_entry.get_id())
    distance_from_place = _get_distance_from_start_place_to_end(trip_entry)

    if (trip_source == "DwellSegmentationTimeFilter"):
        import emission.analysis.intake.segmentation.section_segmentation_methods.smoothed_high_confidence_motion as shcm
        shcmsm = shcm.SmoothedHighConfidenceMotion(60, 100, [ecwm.MotionTypes.TILTING,
                                                        ecwm.MotionTypes.UNKNOWN,
                                                        ecwm.MotionTypes.STILL,
                                                        ecwm.MotionTypes.NONE,
                                                        ecwm.MotionTypes.STOPPED_WHILE_IN_VEHICLE])
    else:
        assert(trip_source == "DwellSegmentationDistFilter")
        import emission.analysis.intake.segmentation.section_segmentation_methods.smoothed_high_confidence_with_visit_transitions as shcmvt
        shcmsm = shcmvt.SmoothedHighConfidenceMotionWithVisitTransitions(
                                                        49, 50, [ecwm.MotionTypes.TILTING,
                                                        ecwm.MotionTypes.UNKNOWN,
                                                        ecwm.MotionTypes.STILL,
                                                        ecwm.MotionTypes.NONE, # iOS only
                                                        ecwm.MotionTypes.STOPPED_WHILE_IN_VEHICLE]) # iOS only
        
    segmentation_points = shcmsm.segment_into_sections(ts, distance_from_place, time_query)

    # Since we are segmenting an existing trip into sections, we do not need to worry about linking with
    # a prior place, since it will be linked through the trip object.
    # So this is much simpler than the trip case.
    # Again, since this is segmenting a trip, we can just start with a section

    prev_section_entry = None

    # TODO: Should we link the locations to the trips this way, or by using a foreign key?
    # If we want to use a foreign key, then we need to include the object id in the data df as well so that we can
    # set it properly.
    ts = esta.TimeSeries.get_time_series(user_id)

    get_loc_for_ts = lambda time: ecwl.Location(ts.get_entry_at_ts("background/filtered_location", "data.ts", time)["data"])
    trip_start_loc = get_loc_for_ts(trip_entry.data.start_ts)
    trip_end_loc = get_loc_for_ts(trip_entry.data.end_ts)
    logging.debug("trip_start_loc = %s, trip_end_loc = %s" % (trip_start_loc, trip_end_loc))

    for (i, (start_loc_doc, end_loc_doc, sensed_mode)) in enumerate(segmentation_points):
        logging.debug("start_loc_doc = %s, end_loc_doc = %s" % (start_loc_doc, end_loc_doc))
        get_loc_for_row = lambda row: ts.df_row_to_entry("background/filtered_location", row).data
        start_loc = get_loc_for_row(start_loc_doc)
        end_loc = get_loc_for_row(end_loc_doc)
        logging.debug("start_loc = %s, end_loc = %s" % (start_loc, end_loc))

        section = ecwc.Section()
        section.trip_id = trip_entry.get_id()
        if prev_section_entry is None:
            # This is the first point, so we want to start from the start of the trip, not the start of this segment
            start_loc = trip_start_loc
        if i == len(segmentation_points) - 1:
            # This is the last point, so we want to end at the end of the trip, not at the end of this segment
            # Particularly in this case, if we don't do this, then the trip end may overshoot the section end
            end_loc = trip_end_loc

        fill_section(section, start_loc, end_loc, sensed_mode)
        # We create the entry after filling in the section so that we know
        # that the data is included properly
        section_entry = ecwe.Entry.create_entry(user_id, esda.RAW_SECTION_KEY,
                                                section, create_id=True)

        if prev_section_entry is not None:
            # If this is not the first section, create a stop to link the two sections together
            # The expectation is prev_section -> stop -> curr_section
            stop = ecws.Stop()
            stop.trip_id = trip_entry.get_id()
            stop_entry = ecwe.Entry.create_entry(user_id,
                                                    esda.RAW_STOP_KEY,
                                                    stop, create_id=True)
            logging.debug("stop = %s, stop_entry = %s" % (stop, stop_entry))
            stitch_together(prev_section_entry, stop_entry, section_entry)
            ts.insert(stop_entry)
            ts.update(prev_section_entry)

        # After we go through the loop, we will be left with the last section,
        # which does not have an ending stop. We insert that too.
        ts.insert(section_entry)
        prev_section_entry = section_entry


def fill_section(section, start_loc, end_loc, sensed_mode):
    section.start_ts = start_loc.ts
    section.start_local_dt = start_loc.local_dt
    section.start_fmt_time = start_loc.fmt_time

    section.end_ts = end_loc.ts
    try:
        section.end_local_dt = end_loc.local_dt
    except AttributeError as e:
        print(end_loc)
    section.end_fmt_time = end_loc.fmt_time

    section.start_loc = start_loc.loc
    section.end_loc = end_loc.loc

    section.duration = end_loc.ts - start_loc.ts
    section.source = "SmoothedHighConfidenceMotion"
    section.sensed_mode = sensed_mode


def stitch_together(ending_section_entry, stop_entry, starting_section_entry):
    ending_section = ending_section_entry.data
    stop = stop_entry.data
    starting_section = starting_section_entry.data

    ending_section.end_stop = stop_entry.get_id()

    stop.enter_ts = ending_section.end_ts
    stop.enter_local_dt = ending_section.end_local_dt
    stop.enter_fmt_time = ending_section.end_fmt_time
    stop.ending_section = ending_section_entry.get_id()

    stop.enter_loc = ending_section.end_loc
    stop.exit_loc = starting_section.start_loc
    stop.duration = starting_section.start_ts - ending_section.end_ts
    stop.distance = ecc.calDistance(stop.enter_loc.coordinates,
                                    stop.exit_loc.coordinates)
    stop.source = "SmoothedHighConfidenceMotion"

    stop.exit_ts = starting_section.start_ts
    stop.exit_local_dt = starting_section.start_local_dt
    stop.exit_fmt_time = starting_section.start_fmt_time
    stop.starting_section = starting_section_entry.get_id()

    starting_section.start_stop = stop_entry.get_id()

    ending_section_entry["data"] = ending_section
    stop_entry["data"] = stop
    starting_section_entry["data"] = starting_section

def _get_distance_from_start_place_to_end(raw_trip_entry):
    import emission.core.common as ecc

    start_place_id = raw_trip_entry.data.start_place
    start_place = esda.get_object(esda.RAW_PLACE_KEY, start_place_id)
    dist = ecc.calDistance(start_place.location.coordinates,
                           raw_trip_entry.data.end_loc.coordinates)
    logging.debug("Distance from raw_place %s to the end of raw_trip_entry %s = %s" %
                  (start_place_id, raw_trip_entry.get_id(), dist))
    return dist

