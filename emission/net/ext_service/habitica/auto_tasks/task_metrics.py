def get_short_section_mode_pct(mode_section_grouped_df):
    ret_dict = {}
    for (mode, mode_section_df) in mode_section_grouped_df:
        ret_dict[mode] = len(mode_section_df)
    return ret_dict
