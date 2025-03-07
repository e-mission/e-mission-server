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
import emission.analysis.configs.dynamic_config as eadc
import emission.storage.pipeline_queries as epq
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt

import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl
import emission.core.wrapper.localdate as ecwld
import emission.core.wrapper.section as ecwc
import emission.core.wrapper.stop as ecws
import emission.core.wrapper.entry as ecwe

import emission.core.common as ecc
import emcommon.bluetooth.ble_matching as emcble

class SectionSegmentationMethod(object):
    def segment_into_sections(self, timeseries, time_query, distance_from_place, ble_list, motion_df, unfiltered_loc_df, filtered_loc_df):
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
    try:
        ts = esta.TimeSeries.get_time_series(user_id)
        time_query = epq.get_time_range_for_sectioning(user_id)
        trips_to_process = ts.find_entries([esda.RAW_TRIP_KEY], time_query)

        time_query.timeType = 'data.exit_ts'
        places_in_range = ts.find_entries([esda.RAW_PLACE_KEY], time_query)

        time_query.timeType = 'data.ts'
        ble_list = ts.find_entries(['background/bluetooth_ble'], time_query)
        motion_df = ts.get_data_df('background/motion_activity', time_query)
        unfiltered_loc_df = ts.get_data_df('background/location', time_query)
        filtered_loc_df = ts.get_data_df('background/filtered_location', time_query)

        entries_to_insert = []
        for trip_entry in trips_to_process:
            dist_from_place = _get_distance_from_start_place_to_end(trip_entry, places_in_range)
            entries_to_insert += segment_trip_into_sections(
                ts,
                trip_entry,
                dist_from_place,
                trip_entry['data']['source'],
                ble_list,
                motion_df,
                unfiltered_loc_df,
                filtered_loc_df,
            )
        if entries_to_insert:
            ts.bulk_insert(entries_to_insert, esta.EntryType.ANALYSIS_TYPE)
        if len(trips_to_process) == 0:
            # Didn't process anything new so start at the same point next time
            last_trip_processed = None
        else:    
            last_trip_processed = trips_to_process[-1]
        epq.mark_sectioning_done(user_id, last_trip_processed)
    except:
        logging.exception("Sectioning failed for user %s" % user_id)
        epq.mark_sectioning_failed(user_id)


def segment_trip_into_sections(ts, trip_entry, distance_from_place, trip_source, ble_list, motion_df, unfiltered_loc_df, filtered_loc_df):
    trip_tq = estt.TimeQuery("data.ts", trip_entry['data']['start_ts'], trip_entry['data']['end_ts'])

    trip_ble_list = [e for e in ble_list
                    if e["data"]["ts"] >= trip_tq.startTs
                    and e["data"]["ts"] <= trip_tq.endTs]
    ts_in_tq = "@trip_tq.startTs <= ts <= @trip_tq.endTs"
    trip_motion_df = motion_df.query(ts_in_tq) if len(motion_df) else motion_df
    trip_unfiltered_loc_df = unfiltered_loc_df.query(ts_in_tq) if len(unfiltered_loc_df) else unfiltered_loc_df
    trip_filtered_loc_df = filtered_loc_df.query(ts_in_tq) if len(filtered_loc_df) else filtered_loc_df

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
    segmentation_points = shcmsm.segment_into_sections(
        ts,
        trip_tq,
        distance_from_place,
        trip_ble_list,
        trip_motion_df,
        trip_unfiltered_loc_df,
        trip_filtered_loc_df
    )

    # Since we are segmenting an existing trip into sections, we do not need to worry about linking with
    # a prior place, since it will be linked through the trip object.
    # So this is much simpler than the trip case.
    # Again, since this is segmenting a trip, we can just start with a section

    # TODO: Should we link the locations to the trips this way, or by using a foreign key?
    # If we want to use a foreign key, then we need to include the object id in the data df as well so that we can
    # set it properly.

    trip_start_loc = ecwl.Location(trip_filtered_loc_df.iloc[0])
    trip_end_loc = ecwl.Location(trip_filtered_loc_df.iloc[-1])
    logging.debug("trip_start_loc = %s, trip_end_loc = %s" % (trip_start_loc, trip_end_loc))

    section_entries = []
    stops_entries = []
    for (i, (start_loc_doc, end_loc_doc, sensed_mode)) in enumerate(segmentation_points):
        logging.debug("start_loc_doc = %s, end_loc_doc = %s" % (start_loc_doc, end_loc_doc))
        start_loc = ecwl.Location(start_loc_doc)
        end_loc = ecwl.Location(end_loc_doc)
        logging.debug("start_loc = %s, end_loc = %s" % (start_loc, end_loc))

        section = ecwc.Section()
        section.trip_id = trip_entry['_id']
        if len(section_entries) == 0:
            # This is the first point, so we want to start from the start of the trip, not the start of this segment
            start_loc = trip_start_loc
        if i == len(segmentation_points) - 1:
            # This is the last point, so we want to end at the end of the trip, not at the end of this segment
            # Particularly in this case, if we don't do this, then the trip end may overshoot the section end
            end_loc = trip_end_loc

        # ble_sensed_mode represents the vehicle that was sensed via BLE beacon during the section.
        # For now, we are going to rely on the current segmentation implementation and then fill in
        # ble_sensed_mode by looking at scans within the timestamp range of the section.
        # Later, we may want to actually use BLE sensor data as part of the basis for segmentation
        dynamic_config = eadc.get_dynamic_config()
        ble_sensed_mode = emcble.get_ble_sensed_vehicle_for_section(
            trip_ble_list, start_loc.ts, end_loc.ts, dynamic_config
        )

        fill_section(section, start_loc, end_loc, sensed_mode, ble_sensed_mode)
        # We create the entry after filling in the section so that we know
        # that the data is included properly
        section_entry = ecwe.Entry.create_entry(ts.user_id, esda.RAW_SECTION_KEY,
                                                section, create_id=True)

        if len(section_entries) > 0:
            # If this is not the first section, create a stop to link the two sections together
            # The expectation is prev_section -> stop -> curr_section
            stop = ecws.Stop()
            stop.trip_id = trip_entry['_id']
            stop_entry = ecwe.Entry.create_entry(ts.user_id,
                                                 esda.RAW_STOP_KEY,
                                                 stop, create_id=True)
            logging.debug("stop = %s, stop_entry = %s" % (stop, stop_entry))
            stitch_together(section_entries[-1], stop_entry, section_entry)
            stops_entries.append(stop_entry)
        section_entries.append(section_entry)
    
    return section_entries + stops_entries


def fill_section(section, start_loc, end_loc, sensed_mode, ble_sensed_mode=None):
    section.start_ts = start_loc.ts
    section.start_local_dt = ecwld.LocalDate.get_local_date(start_loc.ts, start_loc['local_dt_timezone'])
    section.start_fmt_time = start_loc.fmt_time

    section.end_ts = end_loc.ts
    try:
        section.end_local_dt = ecwld.LocalDate.get_local_date(end_loc.ts, end_loc['local_dt_timezone'])
    except AttributeError as e:
        print(end_loc)
    section.end_fmt_time = end_loc.fmt_time

    section.start_loc = start_loc.loc
    section.end_loc = end_loc.loc

    section.duration = end_loc.ts - start_loc.ts
    section.source = "SmoothedHighConfidenceMotion"
    section.sensed_mode = sensed_mode
    section.ble_sensed_mode = ble_sensed_mode


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


def _get_distance_from_start_place_to_end(raw_trip_entry, raw_places):
    start_place_id = raw_trip_entry['data']['start_place']
    start_place_coords = None
    for place in raw_places:
        if place['_id'] == start_place_id:
            logging.debug('start place found in list')
            start_place_coords = place['data']['location']['coordinates']
    if not start_place_coords:
        logging.debug('start place not found in list, getting by id')
        start_place = esda.get_object(esda.RAW_PLACE_KEY, start_place_id)
        start_place_coords = start_place.location.coordinates
    dist = ecc.calDistance(start_place_coords,
                            raw_trip_entry['data']['end_loc']['coordinates'])
    logging.debug("Distance from raw_place %s to the end of raw_trip_entry %s = %s" %
                (start_place_id, raw_trip_entry['_id'], dist))
    return dist
