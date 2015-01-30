angular.module('e-mission-default', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicPopup, $http) {
    // alert("RootCtrl initialized");
    $scope.showAlert = function() {
       var alertPopup = $ionicPopup.alert({
           title: 'Welcome to E-Mission!',
           okText: 'Got it!',
           template: "<p>This app tracks your travel behavior and calculates your carbon footprint! You can then see how your daily carbon footprint measures up to the average user's footprint, your footprint if you were to drive everywhere, and other fun metrics.</p><p>The measurement is weekly, but we'll ask you to confirm your trips several times a day. You can confirm trips whenever convenient - most people confirm once a day.</p> <b>IMPORTANT</b>: If you are have an iPhone, your trip log may not automatically update. To fix this, toggle 'Force Sync' by going to Auth &gt; Force Sync. Your recent trips should then show up for your confirmation.  You may have to do this daily.</p> <p>Thank you for participating!</p>",
         });
         alertPopup.then(function(res) {
           console.log('Displayed results to the user');
           alertPopup.close();
         });
         $timeout(function() {
            console.log("timed out, closing popup");
            alertPopup.close(); //close the popup after 3 seconds for some reason
         }, 5000);
    }

    $scope.displayCompare = function() {
        return cfc.compare.displayCompare();
    }
});

var cfc = cfc || {};
cfc.compare = cfc.compare || {};

cfc.compare.displayCompare = function() {
  var mylist=$("#myList");
  var selIndex = myList.selectedIndex;
  console.log(selIndex);
  switch (selIndex) {
    case 0:
      cfc.compare.displayMySummary();
      break;
    case 1:
      cfc.compare.displayModeShareCountCompare();
      break;
    case 2:
      cfc.compare.displayModeShareDistanceCompare();
      break;
    case 3:
      cfc.compare.displayModeCarbonFootprintCompare();
      break;
  }
}

cfc.compare.displayModeShareCountCompare = function() {
  cfc.compare.compareMineAndAvg(myModeShareCount, avgModeShareCount,
    "Number of trips");
  // $("#message").text("Number of trips using various modes")
  // $("#carbon_info").hide()
}

cfc.compare.displayModeShareDistanceCompare = function() {
  cfc.compare.compareMineAndAvg(
    cfc.compare.scaleValues(myModeShareDistance, 1000),
    cfc.compare.scaleValues(avgModeShareDistance, 1000),
    "km");
  // $("#message").text("Share of travel distance using various modes")
  // $("#carbon_info").hide()
}

cfc.compare.displayModeCarbonFootprintCompare = function() {
  cfc.compare.compareMineAndAvg(myModeCarbonFootprint, avgModeCarbonFootprint,
    "kg CO2 per week");
  // $("#message").text("Carbon footprints of various modes")
  // $("#carbon_info").show()
}

cfc.compare.getSum = function(obj) {
  sum = 0;
  for(key in obj) {
    sum = sum + obj[key];
  }
  return sum;
}

cfc.compare.scaleValues = function(obj, scale) {
  retVal = []
  for(key in obj) {
    retVal[key] = obj[key]/scale;
  }
  return retVal;
}

cfc.compare.displayMySummary = function() {
  valueFormat = d3.format(',.2f')
  $("#chart").empty();
  summaryData = {
    "mine": cfc.compare.getSum(myModeCarbonFootprint),
    "optimal" : cfc.compare.getSum(myOptimalCarbonFootprint),
    // This is the hardcoded car value.
    // Should this be here or sent from the server
    // This is a webapp so in some case is all the server
    "all drive" : cfc.compare.getSum(myModeShareDistance) * (278/(1609 * 1000)),
    "mean" : cfc.compare.getSum(avgModeCarbonFootprint),
    "2035 mandate": 40.142892,
    "2050 goal": 8.28565
  };
  summaryDataSeries = {};
  summaryDataSeries['key'] = "Carbon footprint and targets";
  summaryDataSeries['values'] = cfc.display.convert(summaryData);
  cfc.display.barChart([summaryDataSeries], "kg CO2 per week");
  // $("#message").text("My footprint summary")
  // $("#carbon_info").show()
}

cfc.compare.compareMineAndAvg = function(myMap, avgMap, yAxisLabel) {
  $("#chart").empty();
  // $("#myFootprint").empty();
  filteredMaps = cfc.compare.filterZeros(myMap, avgMap);
  filteredMyMap = filteredMaps[0];
  filteredAvgMap = filteredMaps[1];

  convertedMyMap = cfc.display.convert(filteredMyMap);
  convertedMySeries = {};
  convertedMySeries['key'] = 'Mine';
  convertedMySeries['values'] = convertedMyMap;

  convertedAvg = cfc.display.convert(filteredAvgMap);
  convertedAvgSeries = {}
  convertedAvgSeries['key'] = 'Average';
  convertedAvgSeries['values'] = convertedAvg
  console.log([convertedMySeries, convertedAvgSeries]);
  // cfc.display.multiBarChart([convertedMySeries, convertedAvgSeries]);
  cfc.display.barChart([convertedMySeries], yAxisLabel);
}

cfc.compare.filterZeros = function(myMap, avgMap) {
  filteredMyMap = {};
  filteredAvgMap = {};
  for (key in myMap) {
    if (myMap[key] == 0 && avgMap[key] == 0) {
      // skip this entry
    } else {
      filteredMyMap[key] = myMap[key];
      filteredAvgMap[key] = avgMap[key];
    }
  }
  return [filteredMyMap, filteredAvgMap];
}


