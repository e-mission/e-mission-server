var client = client || {};
client.gamified = client.gamified || {};

/*
 * From http://stackoverflow.com/questions/3895478/does-javascript-have-a-range-equivalent
 * but not tested
 */
function range(start, stop, step){
  var a=[start], b=start;
  while(b<stop){b+=step;a.push(b)}
  return a;
};

client.gamified.displayScore = function(prevScore, currScore) {
    console.log("displayScore called");

    var margin = {top: 10, right: 2, bottom: 2, left: 35},
    width = 50 - margin.left - margin.right,
    height = 350 - margin.top - margin.bottom;

    var chart = d3.bullet()
        .orient("bottom")
        .width(width)
        .height(height);

    data = [{'title': 'Score',
            'subtitle': 'Number of points',
            'ranges': [1000, 10000, 100000],
            'measures': [prevScore, currScore],
            'submarkers': range(200, 800, 200).concat(range(2000, 8000, 2000)).concat(range(20000, 80000, 20000)),
            'markers': [1000, 10000, 100000]
           }];

    console.log("data = "+data);

      var svg = d3.select("#chart").selectAll("svg")
          .data(data)
          .enter().append("svg")
          .attr("class", "bullet")
          .attr("width", width + margin.left + margin.right)
          .attr("height", height + margin.top + margin.bottom)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
          .call(chart);

      console.log("domains are "+d3.selectAll("path"))
      
      d3.selectAll("path").style("opacity", 0)


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
