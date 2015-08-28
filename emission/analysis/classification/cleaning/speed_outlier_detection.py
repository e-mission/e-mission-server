# Techniques for outlier detection of speeds. Each of these returns a speed threshold that 
# can be used with outlier detection techniques.

# Standard imports
import logging

logging.basicConfig(level=logging.DEBUG)

class BoxplotOutlier(object):
    MINOR = 1.5
    MAJOR = 3
    def __init__(self, multiplier = MAJOR, ignore_zeros = False):
        self.multiplier = multiplier
        self.ignore_zeros = ignore_zeros

    def get_threshold(self, with_speeds_df):
        if self.ignore_zeros:
            df_to_use = with_speeds_df[with_speeds_df.speed > 0]
        else:
            df_to_use = with_speeds_df
        quartile_vals = df_to_use.quantile([0.25, 0.75]).speed
        logging.debug("quartile values are %s" % quartile_vals)
        iqr = quartile_vals.iloc[1] - quartile_vals.iloc[0]
        logging.debug("iqr %s" % iqr)
        return quartile_vals.iloc[1] + self.multiplier * iqr

class SimpleQuartileOutlier(object):
    def __init__(self, quantile = 0.99, ignore_zeros = False):
        self.quantile = quantile
        self.ignore_zeros = ignore_zeros

    def get_threshold(self, with_speeds_df):
        if self.ignore_zeros:
            df_to_use = with_speeds_df[with_speeds_df.speed > 0]
        else:
            df_to_use = with_speeds_df
        return df_to_use.speed.quantile(self.quantile)

