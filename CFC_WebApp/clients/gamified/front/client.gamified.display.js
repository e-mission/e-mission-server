angular.module('e-mission-gamified', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicTabsDelegate, $http) {
    alert("RootCtrl initialized");
    $scope.displayScore = function() {
        alert("displayScore called");
        console.log("displayScore called with currScore = "+currScore+" and prevScore = "+prevScore);

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

        console.log("data = "+JSON.stringify(data));

          var svg = d3.select("#chart").selectAll("svg")
              .data(data)
              .enter().append("svg")
              .attr("class", "bullet")
              .attr("width", width + margin.left + margin.right)
              .attr("height", height + margin.top + margin.bottom)
            .append("g")
              .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
              .call(chart);

          console.log("domains are "+d3.selectAll("path"));
          
          d3.selectAll("path").style("opacity", 0);
    }
    /*
     * From http://stackoverflow.com/questions/3895478/does-javascript-have-a-range-equivalent
     * but not tested
     */
    function range(start, stop, step){
      var a=[start], b=start;
      while(b<stop){b+=step;a.push(b)}
      return a;
    };
});

/*
angular.module("e-mission-gamified", ['ionic'])


.controller('RootCtrl', function($scope) {
});
*/
