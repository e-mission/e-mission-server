angular.module('e-mission-socialgame', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicPopup, $http, $ionicModal) {
    // alert("RootCtrl initialized");
    $ionicModal.fromTemplateUrl('socialgame/modal.html', {
    scope: $scope
  }).then(function(modal) {
    $scope.modal = modal;
  });
});

/*
angular.module("e-mission-gamified", ['ionic'])


.controller('RootCtrl', function($scope) {
});
*/
