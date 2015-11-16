# Standard imports
import logging

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.stop_queries as esdst

import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl


class SectionSegmentationMethod(object):
    def segment_into_sections(self, timeseries, time_query):
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
        trips_to_process = esdt.get_trips(user_id, time_query)
        for trip in trips_to_process:
            logging.info("+" * 20 + ("Processing trip %s for user %s" % (trip.get_id(), user_id)) + "+" * 20)
            segment_trip_into_sections(user_id, trip.get_id())
        if len(trips_to_process) == 0:
            # Didn't process anything new so start at the same point next time
            last_trip_processed = None
        else:    
            last_trip_processed = trips_to_process[-1]
        epq.mark_sectioning_done(user_id, last_trip_processed)
    except:
        logging.exception("Sectioning failed for user %s" % user_id)
        epq.mark_sectioning_failed(user_id)

def segment_trip_into_sections(user_id, trip_id):
    ts = esta.TimeSeries.get_time_series(user_id)
    trip = esdt.get_trip(trip_id)
    time_query = esdt.get_time_query_for_trip(trip_id)

    import emission.analysis.intake.segmentation.section_segmentation_methods.smoothed_high_confidence_motion as shcm
    shcmsm = shcm.SmoothedHighConfidenceMotion(60, [ecwm.MotionTypes.TILTING,
                                                    ecwm.MotionTypes.UNKNOWN,
                                                    ecwm.MotionTypes.STILL,
                                                    ecwm.MotionTypes.NONE, # iOS only
                                                    ecwm.MotionTypes.STOPPED_WHILE_IN_VEHICLE]) # iOS only
    segmentation_points = shcmsm.segment_into_sections(ts, time_query)

    # Since we are segmenting an existing trip into sections, we do not need to worry about linking with
    # a prior place, since it will be linked through the trip object.
    # So this is much simpler than the trip case.
    # Again, since this is segmenting a trip, we can just start with a section

    prev_section = None

    # TODO: Should we link the locations to the trips this way, or by using a foreign key?
    # If we want to use a foreign key, then we need to include the object id in the data df as well so that we can
    # set it properly.
    trip_start_loc = ecwl.Location(ts.get_entry_at_ts("background/filtered_location", "data.ts", trip.start_ts)["data"])
    trip_end_loc = ecwl.Location(ts.get_entry_at_ts("background/filtered_location", "data.ts", trip.end_ts)["data"])
    logging.debug("trip_start_loc = %s, trip_end_loc = %s" % (trip_start_loc, trip_end_loc))

    for (i, (start_loc_doc, end_loc_doc, sensed_mode)) in enumerate(segmentation_points):
        logging.debug("start_loc_doc = %s, end_loc_doc = %s" % (start_loc_doc, end_loc_doc))
        start_loc = ecwl.Location(start_loc_doc)
        end_loc = ecwl.Location(end_loc_doc)
        logging.debug("start_loc = %s, end_loc = %s" % (start_loc, end_loc))

        section = esds.create_new_section(user_id, trip_id)
        if prev_section is None:
            # This is the first point, so we want to start from the start of the trip, not the start of this segment
            start_loc = trip_start_loc
        if i == len(segmentation_points) - 1:
            # This is the last point, so we want to end at the end of the trip, not at the end of this segment
            # Particularly in this case, if we don't do this, then the trip end may overshoot the section end
            end_loc = trip_end_loc

        fill_section(section, start_loc, end_loc, sensed_mode)

        if prev_section is not None:
            # If this is not the first section, create a stop to link the two sections together
            # The expectation is prev_section -> stop -> curr_section
            stop = esdst.create_new_stop(user_id, trip_id)
            stitch_together(prev_section, stop, section)
            esdst.save_stop(stop)
            esds.save_section(prev_section) # Because we have now linked it to the stop, we need to save it again

        esds.save_section(section)
        prev_section = section


def fill_section(section, start_loc, end_loc, sensed_mode):
    section.start_ts = start_loc.ts
    section.start_local_dt = start_loc.local_dt
    section.start_fmt_time = start_loc.fmt_time

    section.end_ts = end_loc.ts
    section.end_local_dt = end_loc.local_dt
    section.end_fmt_time = end_loc.fmt_time

    section.start_loc = start_loc.loc
    section.end_loc = end_loc.loc

    section.duration = end_loc.ts - start_loc.ts
    section.source = "SmoothedHighConfidenceMotion"
    section.sensed_mode = sensed_mode


def stitch_together(ending_section, stop, starting_section):
    ending_section.end_stop = stop.get_id()

    stop.enter_ts = ending_section.end_ts
    stop.enter_local_dt = ending_section.end_local_dt
    stop.enter_fmt_time = ending_section.end_fmt_time
    stop.ending_section = ending_section.get_id()

    stop.enter_loc = ending_section.end_loc
    stop.exit_loc = starting_section.start_loc
    stop.duration = starting_section.start_ts - ending_section.end_ts
    stop.source = "SmoothedHighConfidenceMotion"

    stop.exit_ts = starting_section.start_ts
    stop.exit_local_dt = starting_section.start_local_dt
    stop.exit_fmt_time = starting_section.start_fmt_time
    stop.starting_section = starting_section.get_id()

    starting_section.start_stop = stop.get_id()
