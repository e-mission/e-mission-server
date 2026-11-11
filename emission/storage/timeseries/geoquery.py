from builtins import *
from builtins import object
import logging

class GeoQuery(object):
    """
        Object that encapsulates a query for a particular GeoJSON polygon or
        multi-polygon.
    """
    def __init__(self, loc_field_list, poly_region):
        self.loc_field_list = loc_field_list
        self.region = poly_region

    def get_query(self):
        if self.loc_field_list is None or len(self.loc_field_list) == 0:
            raise AttributeError("loc_field_list = %s, need actual values",
                self.loc_field_list)
        logging.debug("region is %s" % self.region)
        if self.region is not None:
            if "geometry" in self.region and "type" in self.region["geometry"]:
                region_type = self.region["geometry"]["type"]
                logging.debug("type = %s" % region_type)
                if region_type is not None and (region_type != 'Polygon' and
                                                region_type != 'MultiPolygon'):
                    raise AttributeError("invalid poly region %s" % self.region)

        mongo_region = {}
        mongo_region['$geometry'] = self.region['geometry']
        ret_query = {}
        for field in self.loc_field_list:
            ret_query.update({field: {"$geoWithin": mongo_region}})
        return ret_query
