angular.module('e-mission-choice', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicTabsDelegate, $http) {
  $scope.onControllerChanged = function(oldController, oldIndex, newController, newIndex) {
    // alert('controller changed');
    console.log('Controller changed', oldController, oldIndex, newController, newIndex);
    console.log(arguments);
  };

  $scope.getIndexForTab = function(label) {
    var tabLabelMap = {'data': 0, 'leaderboard': 1, 'game': 2};
    return tabLabelMap[label];
  }

  $scope.setCurrChoice = function(newChoiceLabel) {
    var queryParams = "client_key="+serverVariables['client_key']+
        "&uuid="+serverVariables['uuid']+
        "&new_view="+newChoiceLabel;
    console.log("queryParams = "+queryParams);
    $http.get("/client/choice/switchResultDisplay?"+queryParams).
      success(function(data, status, headers, config) {
        // alert("call successful");
        $scope.currChoice = $scope.getIndexForTab(newChoiceLabel);
        $ionicTabsDelegate.select($scope.currChoice);
        // this callback will be called asynchronously
        // when the response is available
      }).
      error(function(data, status, headers, config) {
        // alert("call error");
        // called asynchronously if an error occurs
        // or server returns response with an error status.
      });
  }

  /*
   * This needs to be called from the last ion-tab, otherwise, we won't know
   * whether or not the tab that we want to use has been created.
   */
  $scope.onInitDone = function() {
    // alert("Init is done");
    // console.log("Init is done with scope "+JSON.stringify($scope));
    // console.log("Init is done with this "+JSON.stringify(this));
    // $scope.currChoice = $scope.getIndexForTab(initialChoice);
    $scope.currChoice = $scope.getIndexForTab(serverVariables['curr_view']);
    console.log("Switching to choice "+$scope.currChoice);
    $ionicTabsDelegate.select($scope.currChoice);
  }
})

.directive('inlineResult', ['$document', function($document) {
  function link(scope, element, attrs) {
    console.log("inlineResult invoked with element = "+element[0]);
    console.log("child document = "+element[0].contentDocument);
    // console.log("curr base URI= "+element[0].contentDocument.baseURI);
    // console.log("currSrc = "+element[0].src);
    // console.log("current html = "+$document[0]);
    console.log("current base = "+$document[0].baseURI);
    var currSrc = element[0].src;
    var parts = currSrc.split(",");
    // console.log(parts);
    var decodedText = atob(parts[1]);
    // console.log("decodedText = "+decodedText);
    var currBaseURI = $document[0].baseURI;
    var decodedTextWithBase = decodedText.replace("<head>", "<head><base href=\""+currBaseURI+"\">");
    // console.log("decodedTextWithBase = "+decodedTextWithBase);
    var encodedTextWithBase = btoa(decodedTextWithBase);
    var srcWithBase = parts[0]+","+encodedTextWithBase;
    // console.log("srcWithBase "+srcWithBase);
    element[0].setAttribute("src",srcWithBase)
    // console.log("updatedSrc = "+element[0].src);
  }
  return {
    link: link
  }
}])

.controller('DataCtrl', function($scope, $http, $window, $timeout, $ionicModal,
        $ionicActionSheet, $ionicTabsDelegate) {
  // alert("DataCtrl called");

  $scope.onDataClicked = function() {
    $scope.setCurrChoice("data")
  }

  $scope.onDataSelected = function() {
    console.log("Data tab selected");
  }

  $scope.onRefresh = function() {
    // alert("DataCtrl.ON REFRESH");
    console.log('ON REFRESH');

    $timeout(function() {
      $scope.$broadcast('scroll.refreshComplete');
    }, 10000);
  }
})

.controller('LeaderboardCtrl', function($scope, $ionicModal) {
  // alert("GameCtrl called");

  $scope.onLeaderboardClicked = function() {
    $scope.setCurrChoice("leaderboard")
  }

  $scope.onLeaderboardSelected = function() {
    console.log("Leaderboard tab selected");
  }

  $scope.onRefresh = function() {
    // alert("GameCtrl.ON REFRESH");
    console.log('ON REFRESH');

    $timeout(function() {
      $scope.$broadcast('scroll.refreshComplete');
    }, 1000);
  }
})

.controller('GameCtrl', function($scope, $ionicModal) {
  // alert("GameCtrl called");

  $scope.onGameClicked = function() {
    $scope.setCurrChoice("game")
  }

  $scope.onGameSelected = function() {
    console.log("Game tab selected");
  }

  $scope.onRefresh = function() {
    // alert("GameCtrl.ON REFRESH");
    console.log('ON REFRESH');

    $timeout(function() {
      $scope.$broadcast('scroll.refreshComplete');
    }, 1000);
  }
});
