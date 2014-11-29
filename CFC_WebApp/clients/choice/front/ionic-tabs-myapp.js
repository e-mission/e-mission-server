angular.module('e-mission-choice', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicTabsDelegate, $http) {
  $scope.onControllerChanged = function(oldController, oldIndex, newController, newIndex) {
    alert('controller changed');
    console.log('Controller changed', oldController, oldIndex, newController, newIndex);
    console.log(arguments);
  };

  $scope.getIndexForTab = function(label) {
    var tabLabelMap = {'data': 0, 'game': 1};
    return tabLabelMap[label];
  }

  $scope.setCurrChoice = function(newChoiceLabel) {
      $http.get('/client/choice/switchChoice?choice='+newChoiceLabel).
        success(function(data, status, headers, config) {
          alert("call successful");
          $scope.currChoice = $scope.getIndexForTab(newChoiceLabel);
          $ionicTabsDelegate.select($scope.currChoice);
          // this callback will be called asynchronously
          // when the response is available
        }).
        error(function(data, status, headers, config) {
          alert("call error");
          // called asynchronously if an error occurs
          // or server returns response with an error status.
        });
  }

  /*
   * This needs to be called from the last ion-tab, otherwise, we won't know
   * whether or not the tab that we want to use has been created.
   */
  $scope.onInitDone = function() {
    alert("Init is done");
    console.log("Init is done");
    // $scope.currChoice = $scope.getIndexForTab(initialChoice);
    $scope.currChoice = 1;
    console.log("Switching to choice "+$scope.currChoice);
    $ionicTabsDelegate.select($scope.currChoice);
  }
})

.controller('DataCtrl', function($scope, $http, $window, $timeout, $ionicModal,
        $ionicActionSheet, $ionicTabsDelegate) {
  alert("DataCtrl called");

  $scope.onDataClicked = function() {
    $scope.setCurrChoice("data")
    console.log("myModeCarbonFootprint = "+myModeCarbonFootprint);
    // cfc.compare.displayCompare();
  }

  $scope.onDataSelected = function() {
    console.log("Data tab selected");
  }

  $scope.onRefresh = function() {
    alert("DataCtrl.ON REFRESH");
    console.log('ON REFRESH');

    $timeout(function() {
      $scope.$broadcast('scroll.refreshComplete');
    }, 1000);
  }
})

.controller('GameCtrl', function($scope, $ionicModal) {
  alert("GameCtrl called");

  $scope.onGameClicked = function() {
    $scope.setCurrChoice("game")
  }

  $scope.onGameSelected = function() {
    console.log("Game tab selected");
  }

  $scope.onRefresh = function() {
    alert("GameCtrl.ON REFRESH");
    console.log('ON REFRESH');

    $timeout(function() {
      $scope.$broadcast('scroll.refreshComplete');
    }, 1000);
  }
});
