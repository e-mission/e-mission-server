angular.module('starter.controllers', ['ui-leaflet'])

.controller('HeatmapCtrl', function($scope) {
  $scope.mapCtrl = {};

  angular.extend($scope.mapCtrl, {
    defaults : {
      tileLayer: 'http://tile.stamen.com/toner/{z}/{x}/{y}.png',
      tileLayerOptions: {
        attribution: 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, under <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. Data by <a href="http://openstreetmap.org">OpenStreetMap</a>, under <a href="http://www.openstreetmap.org/copyright">ODbL</a>.',
        opacity: 0.9,
        detectRetina: true,
        reuseTiles: true,
      },
      center: {
        lat: 37.87269,
        lng: -122.25921,
        zoom: 15
      }
    }
  });
})

.controller('ChatsCtrl', function($scope, Chats) {
  // With the new view caching in Ionic, Controllers are only called
  // when they are recreated or on app start, instead of every page change.
  // To listen for when this page is active (for example, to refresh data),
  // listen for the $ionicView.enter event:
  //
  //$scope.$on('$ionicView.enter', function(e) {
  //});

  $scope.chats = Chats.all();
  $scope.remove = function(chat) {
    Chats.remove(chat);
  };
})

.controller('ChatDetailCtrl', function($scope, $stateParams, Chats) {
  $scope.chat = Chats.get($stateParams.chatId);
})

.controller('AccountCtrl', function($scope) {
  $scope.settings = {
    enableFriends: true
  };
});
