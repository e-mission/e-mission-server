// Functions to help with displaying the data that we collected Note that this
// assumes that there is an SVG tag called "chart" which is where we want to
// display our pie chart

// This function converts data from the format returned by our API, which is of the form:
// { key1: value1, key2: value2, ... keyn: valuen}
// for example:
// { "walk": "40", "drive": "10", "bike": 10, "bus": 30}
// However, NVD3 prefers the data to be in the format
// [{ "label: "walk", "value": "40"}, ...}]
// So we convert it before passing it on
// Note also that we might be able to do this through clever jiggering of the
// data in the x() and y() functions, but we need to see what format the data is in for that

var cfc = cfc || {};
cfc.display = cfc.display || {};

cfc.display.convert = function(data) {
  var result = [];
  for (var key in data) {
    if (data.hasOwnProperty(key)) {
      var currEntry = {}
      currEntry['key'] = key;
      currEntry['value'] = data[key];
      result.push(currEntry);
    }
  }
  return result;
}

cfc.display.convertAndDisplayHeatMap = function(data, weightLabel) {
  if ('weightedLoc' in data) {
    convertedData = cfc.display.convertWeightedHeatMap(data['weightedLoc'], weightLabel);
    cfc.display.displayWeightedHeatMap(convertedData);
  }
  if ('latlng' in data) {
    convertedData = cfc.display.convertHeatMapPoints(data['latlng']);
    cfc.display.displayHeatMap(convertedData);
  }
}

cfc.display.convertWeightedHeatMap = function(data, label) {
  retArray = [];
  for (var i = 0; i < data.length; i++) {
    entry = data[i];
    entryLoc = entry['loc'];
    latlng = new google.maps.LatLng(entryLoc[0], entryLoc[1]);
    weightedLoc = {
      location: latlng,
      weight: entry[label]
    };
    retArray.push(weightedLoc);
  }
  return retArray;
}

cfc.display.convertHeatMapPoints = function(data) {
  retArray = [];
  for (var i = 0; i < data.length; i++) {
    entry = data[i];
    // Points are in GeoJSON format, i.e. (lng, lat)
    latlng = new google.maps.LatLng(entry[1], entry[0]);
    retArray.push(latlng);
  }
  return retArray;
}

cfc.display.sortLabels = function(a,b) {
  if (b['key'] == a['key']) {
    return 0;
  } else if (b['key'] < a['key']) {
    return 1;
  } else {
    return -1;
  }
}

cfc.display.prepChart = function() {
  $("#chart").empty();
  $("#mapcanvas").hide();
  $("#chart").show();
}

cfc.display.prepMap = function() {
  $("#mapcanvas").show();
  $("#chart").hide();
}


cfc.display.displayWeightedHeatMap = function(data) {
  var map, pointarray, heatmap;
  var mapOptions = {
    zoom: 13,
    center: new google.maps.LatLng(37.872186, -122.257984),
    // mapTypeId: google.maps.MapTypeId.SATELLITE
  };
  map = new google.maps.Map(document.getElementById('mapcanvas'),
      mapOptions);
  var pointArray = new google.maps.MVCArray(data);
  heatmap = new google.maps.visualization.HeatmapLayer({
    data: pointArray
  });
  heatmap.setMap(map);
  heatmap.set('radius', 75);
  heatmap.set('opacity', 0.8);
}

cfc.display.displayHeatMap = function(data) {
  var map, pointarray, heatmap;
  var mapOptions = {
    zoom: 13,
    center: new google.maps.LatLng(37.872186, -122.257984),
    // mapTypeId: google.maps.MapTypeId.SATELLITE
  };
  map = new google.maps.Map(document.getElementById('mapcanvas'),
      mapOptions);
  var pointArray = new google.maps.MVCArray(data);
  heatmap = new google.maps.visualization.HeatmapLayer({
    data: pointArray
  });
  heatmap.setMap(map);
  heatmap.set('dissipating', true);
  heatmap.set('opacity', 1.0);
}

cfc.display.pieChart = function(data, title) {
  nv.addGraph(function() {
      var width = nv.utils.windowSize().width - 5,
          height = nv.utils.windowSize().height - 5;

      var chart = nv.models.pieChart()
          .x(function(d) { return d.key })
          .y(function(d) { return d.value })
          .width(width)
          .height(height)
          .showLabels(true);

      chart.legend
        .margin({top:0, right:0, bottom: 0, left: 0})
        .width(width)
        .height(10);

      d3.select("#chart")
          .append("svg")
          .datum(data)
          .transition().duration(1200)
          .attr('width', width)
          .attr('height', height)
          .call(chart);

      chart.dispatch.on('stateChange', function(e) { nv.log('New State:', JSON.stringify(e)); });
      return chart;
  });
}

cfc.display.multiBarChart = function(data) {
  var chart;

  nv.addGraph(function() {
    var width = nv.utils.windowSize().width - 5,
        height = nv.utils.windowSize().height - 100;

    chart = nv.models.multiBarChart()
        .x(function(d) { return d.key })
        .y(function(d) { return d.value })
        .width(width)
        .height(height)
        //.margin({top: 30, right: 20, bottom: 50, left: 175})
        //.showValues(true)
        //.tooltips(false)
        .staggerLabels(true)    //Too many bars and not enough room? Try staggering labels.
        .barColor(d3.scale.category20().range())
        .transitionDuration(250)
        .stacked(false)
        .staggerLabels(true)    //Too many bars and not enough room? Try staggering labels.
        .showControls(false);

    chart.legend
      .margin({top:0, right:0, bottom: 0, left: 0})
      .width(width)
      .height(10);

    chart.yAxis
        .tickFormat(d3.format(',2d'));

    d3.select('#chart')
        .append("svg")
        .datum(data)
        .attr('width', width)
        .attr('height', height)
        .call(chart);

    // nv.utils.windowResize(chart.update);
    chart.dispatch.on('stateChange', function(e) { nv.log('New State:', JSON.stringify(e)); });
    return chart;
  });
}

cfc.display.barChart = function(data, yAxisLabel) {

  nv.addGraph(function() {
    var width = nv.utils.windowSize().width - 10,
        height = nv.utils.windowSize().height - 150;

    var chart = nv.models.discreteBarChart()
        .x(function(d) { return d.key })    //Specify the data accessors.
        .y(function(d) { return d.value })
        .width(width)
        .height(height)
        .staggerLabels(true)    //Too many bars and not enough room? Try staggering labels.
        .tooltips(true)        //Don't show tooltips
        .showValues(true)       //...instead, show the bar value right on top of each bar.
        .transitionDuration(350)
        ;

    chart.yAxis
      .axisLabel(yAxisLabel)
      .axisLabelDistance(40);

    d3.select('#chart')
        .append("svg")
        .datum(data)
        .attr('width', width)
        .attr('height', height)
        .call(chart);

    nv.utils.windowResize(chart.update);
    return chart;
  });
}

cfc.display.bulletChart = function(data) {
    /*
    var width = nv.utils.windowSize().height - 5,
        height = 100;
    */

    var margin = {top: 10, right: 40, bottom: 10, left: 80};
    var width = 80,
        height = nv.utils.windowSize().height - 50 - margin.top - margin.bottom;

/*
    var chart = nv.models.bulletChart()
                  .width(width)
                  .height(height)

    var vis = d3.select("#chart")
                .append("svg")
                .data(data)
                .attr("class", "bullet nvd3")
                .attr("width", width)
                .attr("height", height);

    // var wrap = vis.selectAll("g.nv-wrap.nv-bullet");
    vis.attr("transform", "rotate(90)");
    console.log(vis, vis.attr("transform"))

    vis.transition().duration(1000).call(chart);
*/

    var chart = d3.bullet()
      .orient("bottom")
      .width(width)
      .height(height);

    var svg = d3.select("#chart")
        .append("svg")
        .data(data)
        .attr("class", "bullet")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
        .call(chart);

    var title = svg.append("g")
        .style("text-anchor", "end")
        .attr("transform", "translate(" + width + "," + (height + 20) + ")");

    title.append("text")
        .attr("class", "title")
        .text(function(d) { return d.title; });

    title.append("text")
        .attr("class", "subtitle")
        .attr("dy", "1em")
        .text(function(d) { return d.subtitle; });
}


