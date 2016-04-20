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
        if self.region is None or "type" not in self.region or \
            not(self.region.type == 'Polygon' or self.region.type == 'MultiPolygon'):
            raise AttributeError("invalid poly region %s" % self.region)

        ret_query = {}
        for field in self.loc_field_list:
            ret_query.update({field: {"$geoWithin": self.region}})
        return ret_query
