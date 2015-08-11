# Techniques for outlier detection of speeds. Each of these returns a speed threshold that 
# can be used with outlier detection techniques.

# Standard imports
import logging

logging.basicConfig(level=logging.DEBUG)

class BoxplotOutlier(object):
    MINOR = 1.5
    MAJOR = 3
    def __init__(self, multiplier = MAJOR):
        self.multiplier = multiplier

    def get_threshold(self, with_speeds_df):
        quartile_vals = with_speeds_df.quantile([0.25, 0.75]).speed
        logging.debug("quartile values are %s" % quartile_vals)
        iqr = quartile_vals.iloc[1] - quartile_vals.iloc[0]
        logging.debug("iqr %s" % iqr)
        return quartile_vals.iloc[1] + self.multiplier * iqr

class SimpleQuartileOutlier(object):
    def __init__(self, quantile = 0.99):
        self.quantile = quantile

    def get_threshold(self, with_speeds_df):
        return with_speeds_df.speed.quantile(self.quantile)

