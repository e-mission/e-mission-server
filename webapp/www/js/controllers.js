'use strict';

angular.module('starter.controllers', ['starter.heatmap', 'starter.metrics'])

.controller('HomeCtrl', function($scope) {
    $scope.active = 0;
    $scope.aggregateSlides = [];
    $scope.RIPSlides = [];
    $scope.personalSlides = [];
    $scope.introSlides = [];
    $scope.android_base_app = "https://play.google.com/store/apps/details?id=edu.berkeley.eecs.embase";
    $scope.ios_base_app = "https://itunes.apple.com/us/app/emtriplog/id1362434685";

    var addIntroSlides = function() {
        $scope.introSlides.push({
            image: 'img/intro/diary_explain.png',
            text: 'background data collection of trips and user reported incident data',
            id: 0
        });
        $scope.introSlides.push({
            image: 'img/intro/heatmap_explain.png',
            text: 'heatmaps for counting trips and visualizing stress',
            id: 1
        });
        $scope.introSlides.push({
            image: 'img/intro/incident_report_explain.png',
            text: 'users can be prompted at the end of every trip to report incidents',
            id: 2
        });
        $scope.introSlides.push({
            image: 'img/intro/targeted_survey_explain.png',
            text: 'targeted surveys can be sent to regular users to capture public input',
            id: 3
        });
        $scope.introSlides.push({
            image: 'img/intro/extension_explain.png',
            text: 'customizing the UI while retaining native functionality is simple',
            id: 4
        });
    };

    var addAggregateSlides = function() {
        $scope.aggregateSlides.push({
            image: 'img/aggregate/canbikeco_nov_dashboard.png',
            text: 'Multiple metrics for November',
            id: 0
        });
        $scope.aggregateSlides.push({
            image: 'img/aggregate/canbikeco_variation_over_time.png',
            text: 'Variation of one metric over time',
            id: 1
        });
    };

    var addRIPSlides = function() {
        $scope.RIPSlides.push({
            image: 'img/rip_emission/rip_play_store.png',
            text: 'RIP: e-mission on the play store',
            id: 0
        });
        $scope.RIPSlides.push({
            image: 'img/rip_emission/rip_app_store.png',
            text: 'RIP: e-mission on the app store',
            id: 1
        });
    };

    var addPersonalSlides = function() {
        $scope.personalSlides.push({
            image: 'img/personal/list_view.png',
            text: 'Timeline for a particular day',
            id: 0
        });
        $scope.personalSlides.push({
            image: 'img/personal/detail_view.png',
            text: 'Detail for a particular trip',
            id: 1
        });
    };

    addAggregateSlides();
    addRIPSlides();
    addPersonalSlides();
    addIntroSlides();
})

.controller('SetupCtrl', function($scope, $stateParams) {
    console.log("in setup, routeParams = "+JSON.stringify($stateParams));
    $scope.groupid = $stateParams.groupid;
    $scope.userid = $stateParams.userid;
    $scope.hasGroup = $scope.groupid != "";
    console.log("in setup, hasGroup = "+$scope.hasGroup);
})

.controller('ClientSetupCtrl', function($scope, $stateParams) {
    console.log("in client setup, routeParams = "+JSON.stringify($stateParams));
    var BASE_APP_URL = {
        "emTripLog": {
            "android": "https://play.google.com/store/apps/details?id=edu.berkeley.eecs.embase",
            "ios": "https://itunes.apple.com/us/app/emtriplog/id1362434685"
        },
        "emission": {
            "android": "https://play.google.com/store/apps/details?id=edu.berkeley.eecs.emission",
            "ios": "https://itunes.apple.com/us/app/emission/id1084198445"
        }
    }
    if (angular.isDefined($stateParams.base_app)) {
        if (angular.isDefined(BASE_APP_URL[$stateParams.base_app])) {
            $scope.base_app = $stateParams.base_app;
            console.log("base_app defined in paramters, setting to "+$scope.base_app);
        } else {
            // emTripLog is the default
            $scope.base_app = "emTripLog"
            console.log("invalid base_app "+$stateParams.base_app+" defined, defaulting to emTripLog");
        }
    } else {
        // emTripLog is the default
        $scope.base_app = "emTripLog"
        console.log("base_app not defined in paramters, defaulting to emTripLog");
    }
    $scope.android_base_app = BASE_APP_URL[$scope.base_app]["android"]
    $scope.ios_base_app = BASE_APP_URL[$scope.base_app]["ios"]
    $scope.client_label = $stateParams.new_client;
    $scope.clear_local_storage_flag = $stateParams.clear_local_storage;
    $scope.clear_usercache_flag = $stateParams.clear_usercache;
})
