import numpy as np

def get_summary_fn(key):
    summary_fn_map = {
        "metrics/daily_user_count": get_count,
        "metrics/daily_mean_count": get_count,
        "metrics/daily_user_distance": get_distance,
        "metrics/daily_mean_distance": get_distance,
        "metrics/daily_user_duration": get_duration,
        "metrics/daily_mean_duration": get_duration,
        "metrics/daily_user_median_speed": get_median_speed,
        "metrics/daily_mean_median_speed": get_median_speed
    }
    return summary_fn_map[key]

def get_count(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        ret_dict[mode] = len(mode_section_df)
    return ret_dict

def get_distance(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        ret_dict[mode] = mode_section_df.distance.sum()
    return ret_dict

def get_duration(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        ret_dict[mode] = mode_section_df.duration.sum()
    return ret_dict

def get_median_speed(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        median_speeds = [np.median(sl) for sl
                            in mode_section_df.speeds]
        ret_dict[mode] = np.median(median_speeds)
    return ret_dict
