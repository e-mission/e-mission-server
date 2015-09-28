{% macro header(nb) %}
    {% if nb==0 %}
        <link rel="stylesheet" href="http://code.ionicframework.com/ionicons/1.5.2/css/ionicons.min.css">
    {% endif %}
{% endmacro %}

{% macro js(nb,self) %}
    {% if nb==0 %}
          function style_feature(feature) {
            switch(feature.properties.feature_type) {
                case "section": return style_section(feature);
                case "stop": return style_stop(feature);
                default: return {}
            }
          }

          function onEachFeature(feature, layer) {
            switch(feature.properties.feature_type) {
                case "stop": layer.bindPopup(""+feature.properties.duration); break;
                case "start_place": layer.bindPopup(feature.properties.exit_fmt_time); break;
                case "end_place": layer.bindPopup(feature.properties.enter_fmt_time); break;
                case "section": layer.bindPopup(getHumanReadable(feature.properties.sensed_mode)); break;
                case "location": layer.bindPopup(JSON.stringify(feature.properties)); break
            }
          }

          function getHumanReadable(sensed_mode) {
            ret_string = sensed_mode.split('.')[1]
            if(ret_string == 'ON_FOOT') {
                return 'WALKING';
            } else {
                return ret_string;
            }
          }

          function getColoredStyle(baseDict, color) {
            baseDict.color = color
            return baseDict
          }

          function style_section(feature) {
            var baseDict = {
                    weight: 5,
                    opacity: 1,
            };
            mode_string = getHumanReadable(feature.properties.sensed_mode);
            switch(mode_string) {
                case "WALKING": return getColoredStyle(baseDict, 'brown');
                case "BICYCLING": return getColoredStyle(baseDict, 'green');
                case "TRANSPORT": return getColoredStyle(baseDict, 'red');
                default: return getColoredStyle(baseDict, 'black');
            }
          }

          function style_stop(feature) {
            return {fillColor: 'yellow', fillOpacity: 0.8};
          }

          var pointIcon = L.divIcon({className: 'leaflet-div-icon', iconSize: [5, 5]});

          var startMarker = L.AwesomeMarkers.icon({
            icon: 'play',
            prefix: 'ion',
            markerColor: 'green'
          });

          var stopMarker = L.AwesomeMarkers.icon({
            icon: 'stop',
            prefix: 'ion',
            markerColor: 'red'
          });

          function pointFormat(feature, latlng) {
            switch(feature.properties.feature_type) {
                case "start_place": return L.marker(latlng, {icon: startMarker})
                case "end_place": return L.marker(latlng, {icon: stopMarker})
                case "stop": return L.circleMarker(latlng)
                case "location": return L.marker(latlng, {icon: pointIcon})
                default: alert("Found unknown type in feature"  + feature); return L.marker(latlng)
            }
          }
    {% endif %}

    var gjson_layer_{{nb}} = L.geoJson({{self.data}}, 
        {style: style_feature,
         onEachFeature: onEachFeature,
         pointToLayer: pointFormat}).addTo(map)

    var autobounds_{{nb}} = L.featureGroup([gjson_layer_{{nb}}]).getBounds()
    map.fitBounds(autobounds_{{nb}}, {"padding": [5, 5]});
    
{% endmacro %}
