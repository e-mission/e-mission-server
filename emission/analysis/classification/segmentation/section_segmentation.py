# Standard imports
import attrdict as ad
import numpy as np
import datetime as pydt

# Our imports
import emission.analysis.classification.cleaning.location_smoothing as ls
import emission.analysis.point_features as pf
import emission.storage.decorations.location_queries as lq

def segment_into_sections(trip):
    points_df = lq.get_activities_for_section(trip)
    no_tilting_points_df = points_df[points_df.activity != lq.Activities.TILTING]

    section_list = []
    curr_section = ad.AttrDict({"user_id": trip.user_id, "loc_filter": trip.loc_filter,
                                "start_ts": trip.start_ts, "start_time": trip.start_time,
                                "activity": no_tilting_points_df.iloc[0].activity})
    for idx, row in enumerate(no_tilting_points_df.to_dict('records')):
        if row["activity"] != curr_section.activity:
            # Let's add a second check here for confidence and types of activities
            if (row['agc'] > 60 and
                row['activity'] != lq.Activities.UNKNOWN and
                row['activity'] != lq.Activities.STILL):
                # Because the first section is initialized with the first activity.
                # So when idx == 0, the activities will be equal and this is
                # guaranteed to not be invoked
                assert(idx > 0)
                prev_ts = no_tilting_points_df.iloc[idx-1]["write_ts"]
                print("At %s, found new activity %s compared to current %s - creating new section with start_time %s" %
                    (str(pydt.datetime.fromtimestamp(row["write_ts"]/1000)),
                        row["activity"], curr_section.activity,
                        str(pydt.datetime.fromtimestamp(prev_ts/1000))))
                # complete this section
                curr_section.end_ts = prev_ts
                curr_section.end_time = str(pydt.datetime.fromtimestamp(curr_section.end_ts/1000))
                section_list.append(curr_section)
                # make a new section
                curr_section = ad.AttrDict({"user_id": trip.user_id, "loc_filter": trip.loc_filter,
                                    "start_ts": prev_ts,
                                    "start_time": pydt.datetime.fromtimestamp(prev_ts/1000),
                                    "activity": row["activity"]})
            else:
                print("At %s, retained existing activity %s because of low confidence" % 
                        (str(pydt.datetime.fromtimestamp(row["write_ts"]/1000)), curr_section.activity))
        else:
            print("At %s, retained existing activity %s because of no change" % 
                    (str(pydt.datetime.fromtimestamp(row["write_ts"]/1000)), curr_section.activity))

    print("Detected trip end! Ending section at %s" % trip.end_time)        
    # End the last section at the same time as the trip
    curr_section.end_ts = trip.end_ts
    curr_section.end_time = trip.end_time
    section_list.append(curr_section)

    # Merge short sections
    # Sometimes, the sections flip-flop around 
    return section_list
