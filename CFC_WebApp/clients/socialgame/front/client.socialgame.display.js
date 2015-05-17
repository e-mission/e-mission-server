angular.module('e-mission-socailgame', ['ionic'])

.controller('RootCtrl', function($scope, $timeout, $ionicPopup, $http, $ionicModal) {
    // alert("RootCtrl initialized");
    $ionicModal.fromTemplateUrl('templates/modal.html', {
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
