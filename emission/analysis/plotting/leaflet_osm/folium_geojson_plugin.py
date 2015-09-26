import folium.plugins.geo_json as fgj

class FoliumGeojsonPlugin(fgj.GeoJson):
    def __init__(self, data):
        from jinja2 import Environment, PackageLoader

        super(FoliumGeojsonPlugin, self).__init__(data)
        self.plugin_name = 'FoliumGeojsonPlugin'
        # TODO: Introspect the package name instead of hardcoding it
        self.env = Environment(loader=PackageLoader('emission.analysis.plotting', 'leaflet_osm'))
        self.template = self.env.get_template('folium_geo_json.tpl')

    def render_header(self, nb):
        """Generates the header part of the plugin."""
        header = self.template.module.__dict__.get('header',None)
        assert header is not None, "This template must have a 'header' macro."
        return header(nb)

    def render_js(self, nb):
        """Generates the Javascript part of the plugin."""
        js = self.template.module.__dict__.get('js',None)
        assert js is not None, "This template must have a 'js' macro."
        return js(nb,self)
