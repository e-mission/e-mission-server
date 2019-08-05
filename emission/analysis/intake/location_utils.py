import logging
import scipy.interpolate as spi
import attrdict as ad
import pandas as pd
import numpy as np
import arrow
import geojson as gj

import emission.storage.decorations.local_date_queries as esdl

def resample(filtered_loc_df, interval):
    loc_df = filtered_loc_df
    # See https://github.com/e-mission/e-mission-server/issues/268 for log traces
    #
    # basically, on iOS, due to insufficient smoothing, it is possible for us to
    # have very small segments. Some of these contain zero points, and we skip them
    # in the segmentation stage. Some of them contain one point, and we don't.
    # Ideally, we would strip these sections too and merge the stops on the two sides
    # But that is going to take more time and effort than I have here.
    #
    # So let's just return the one point without resampling in that case, and move on for now
    if len(loc_df) == 0 or len(loc_df) == 1:
        logging.debug("len(loc_df) = %s, early return" % (len(loc_df)))
        return loc_df

    logging.debug("Resampling entry list %s of size %s" %
                  (loc_df[["fmt_time", "ts", "longitude", "latitude"]].head(), len(filtered_loc_df)))
    start_ts = loc_df.ts.iloc[0]
    end_ts = loc_df.ts.iloc[-1]
    return resample_for_range(loc_df, start_ts, end_ts, interval)

def resample_for_range(loc_df, start_ts, end_ts, interval):
    tz_ranges_df = _get_tz_ranges(loc_df)
    logging.debug("tz_ranges_df = %s" % tz_ranges_df)

    lat_fn = spi.interp1d(x=loc_df.ts, y=loc_df.latitude, bounds_error=False,
                          fill_value='extrapolate')

    lng_fn = spi.interp1d(x=loc_df.ts, y=loc_df.longitude, bounds_error=False,
                          fill_value='extrapolate')

    altitude_fn = spi.interp1d(x=loc_df.ts, y=loc_df.altitude, bounds_error=False,
                          fill_value='extrapolate')

    ts_new = np.append(np.arange(start_ts, end_ts, 30), [end_ts])
    logging.debug("After resampling, using %d points from %s -> %s" %
                  (ts_new.size, ts_new[0], ts_new[-1]))
    lat_new = lat_fn(ts_new)
    lng_new = lng_fn(ts_new)
    alt_new = altitude_fn(ts_new)
    tz_new = [_get_timezone(ts, tz_ranges_df) for ts in ts_new]
    ld_new = [esdl.get_local_date(ts, tz) for (ts, tz) in zip(ts_new, tz_new)]
    loc_new = [gj.Point((lng, lat)) for (lng, lat) in zip(lng_new, lat_new)]
    fmt_time_new = [arrow.get(ts).to(tz).isoformat() for
                        (ts, tz) in zip(ts_new, tz_new)]
    loc_df_new = pd.DataFrame({"latitude": lat_new, "longitude": lng_new,
                               "loc": loc_new, "ts": ts_new, "local_dt": ld_new,
                               "fmt_time": fmt_time_new, "altitude": alt_new})
    return loc_df_new

def _get_tz_ranges(loc_df):
    tz_ranges = []
    if len(loc_df) == 0:
        logging.debug("Called with loc_df of length 0, returning empty" % len(loc_df))
        return tz_ranges

    # We know that there is at least one entry, so we can access it with impunity
    curr_start_ts = loc_df.ts.iloc[0]
    curr_tz = loc_df.local_dt_timezone.iloc[0]
    for row in loc_df.to_dict('records'):
        loc_data = ad.AttrDict(row)
        if loc_data.local_dt_timezone != curr_tz:
            tz_ranges.append({'timezone': curr_tz,
                              'start_ts': curr_start_ts,
                              'end_ts': loc_data.ts})
            curr_start_ts = loc_data.ts
            curr_tz = loc_data.local_dt_timezone

    # At the end, always add an entry
    # For cases in which there is only one timezone (common case),
    # this will be the only entry
    tz_ranges.append({'timezone': curr_tz,
                      'start_ts': curr_start_ts,
                      'end_ts': loc_df.ts.iloc[-1]})
    logging.debug("tz_ranges = %s" % tz_ranges)
    return pd.DataFrame(tz_ranges)

def _get_timezone(ts, tz_ranges_df):
    if len(tz_ranges_df) == 1:
        sel_entry = tz_ranges_df
    else:
        # TODO: change this to a dataframe query instead
        sel_entry = tz_ranges_df[(tz_ranges_df.start_ts <= ts) &
                            (tz_ranges_df.end_ts >= ts)]
    if len(sel_entry) != 1:
        logging.warning("len(sel_entry) = %d, using the one with the bigger duration" % len(sel_entry))
        sel_entry["duration"] = sel_entry.end_ts - sel_entry.start_ts
        sel_entry = sel_entry[sel_entry.duration == sel_entry.duration.max()]
    return sel_entry.timezone.iloc[0]

