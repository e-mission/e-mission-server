angular.module('e-mission-leaderboard', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicPopup, $http) {
    // alert("RootCtrl initialized");

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
