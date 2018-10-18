// Ionic Starter App

// angular.module is a global place for creating, registering and retrieving Angular modules
// 'starter' is the name of this angular module example (also set in a <body> attribute in index.html)
// the 2nd parameter is an array of 'requires'
// 'starter.services' is found in services.js
// 'starter.controllers' is found in controllers.js
angular.module('starter', ['ionic', 'starter.controllers', 'starter.services',
                           'starter.directives', 'ui.bootstrap', 'monospaced.qrcode'])

.run(function($ionicPlatform) {
  $ionicPlatform.ready(function() {
    // Hide the accessory bar by default (remove this to show the accessory bar above the keyboard
    // for form inputs)
    if (window.cordova && window.cordova.plugins && window.cordova.plugins.Keyboard) {
      cordova.plugins.Keyboard.hideKeyboardAccessoryBar(true);
      cordova.plugins.Keyboard.disableScroll(true);

    }
    if (window.StatusBar) {
      // org.apache.cordova.statusbar required
      StatusBar.styleDefault();
    }
  });
})

.config(function($stateProvider, $urlRouterProvider, $compileProvider) {

  // Ionic uses AngularUI Router which uses the concept of states
  // Learn more here: https://github.com/angular-ui/ui-router
  // Set up the various states which the app can be in.
  // Each state's controller can be found in controllers.js
  $compileProvider.aHrefSanitizationWhitelist(/^\s*(http?|https?|emission):/);
  $stateProvider
  .state('home', {
    url: '/home',
    templateUrl: 'templates/home.html',
    controller: 'HomeCtrl'
  })
  .state('heatmap', {
    url: '/heatmap',
    templateUrl: 'templates/heatmap.html',
    controller: 'HeatmapCtrl'
  })
  .state('trip-planning', {
    url: '/trip-planning',
    templateUrl: 'templates/trip-planning.html'
  })
  .state('metrics', {
    url: '/metrics',
    templateUrl: 'templates/metrics.html',
    controller: 'MetricsCtrl'
  })
  .state('game', {
    url: '/game',
    templateUrl: 'templates/game.html',
  })
  .state('partners', {
    url: '/partners',
    templateUrl: 'templates/partners.html',
  })
  .state('setup', {
    url: '/setup?groupid&userid',
    templateUrl: 'templates/setup.html',
    controller: 'SetupCtrl'
  })
  .state('client_setup', {
    url: '/client_setup?base_app&new_client&clear_local_storage&clear_usercache',
    templateUrl: 'templates/client_setup.html',
    controller: 'ClientSetupCtrl'
  });

  // if none of the above states are matched, use this as the fallback
  $urlRouterProvider.otherwise('/home');

});
