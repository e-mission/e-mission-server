angular.module('e-mission-gamified', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicPopup, $http) {
    // alert("RootCtrl initialized");
    $scope.showAlert = function() {
       var alertPopup = $ionicPopup.alert({
           title: 'Welcome to Penguin Land!',
           okText: 'Got it!',
           template: '<p>We need your help to revitalize a once fun and vibrant penguin community. You can help us by confirming your trips and choosing eco-friendly travel modes! As your points accumulate, you will see the gradual revival of Penguin Land.</p> <p> Point accruals are based on trip confirmation and your carbon footprint (the lower the carbon footprint, the more points!) Watch out - your points may decrease some days (depending on your behavior), but don&#39;t worry, you&#39;ll never go below 0. Trip confirmation is heavily weighed, so make sure you confirm for the penguins&#39; sake!</p> <p> The game starts once you have confirmed trips. Please confirm trips at the end of the day.  <p><b>IMPORTANT</b>: If you are have an iPhone, your trip log may not automatically update. To fix this, toggle "Force Sync" by going to Auth &gt; Force Sync. Your recent trips should then show up for your confirmation.  You may have to do this daily.</p>',
         });
         alertPopup.then(function(res) {
           console.log('Displayed results to the user');
           alertPopup.close();
         });
         $timeout(function() {
            console.log("timed out, closing popup");
            alertPopup.close(); //close the popup after 3 seconds for some reason
         }, 10000);
    }
    $scope.displayScore = function() {
        // alert("displayScore called");
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
                'ranges': [100, 1000, 10000],
                'measures': [prevScore, currScore],
                'submarkers': range(20, 80, 20).concat(range(200, 800, 200)).concat(range(2000, 8000, 2000)),
                'markers': [100, 1000, 10000]
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
