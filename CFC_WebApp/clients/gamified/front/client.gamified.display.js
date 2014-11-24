var client = client || {};
client.gamified = client.gamified || {};

client.gamified.displayScore = function(prevScore, currScore) {
    console.log("displayScore called");

    var margin = {top: 25, right: 40, bottom: 50, left: 80},
    width = 150 - margin.left - margin.right,
    height = 450 - margin.top - margin.bottom;

    var chart = d3.bullet()
        .orient("bottom")
        .width(width)
        .height(height);

    data = [{'title': 'Score',
            'subtitle': 'Number of points',
            'ranges': [1000, 10000, 100000],
            'measures': [prevScore, currScore],
            'markers': [1000, 10000, 100000]
           }];

    console.log("data = "+data);

    var logScale = d3.scale.log()
            .domain([1, data[0]['ranges'][-1]])
            .range([0, height])
    chart.__chart__ = logScale

      var svg = d3.select("#chart").selectAll("svg")
          .data(data)
          .enter().append("svg")
          .attr("class", "bullet")
          .attr("width", width + margin.left + margin.right)
          .attr("height", height + margin.top + margin.bottom)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
          .call(chart);


/*
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

      d3.selectAll("button").on("click", function() {
        svg.datum(randomize).transition().duration(1000).call(chart);
      });
*/
}
