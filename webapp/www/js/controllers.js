'use strict';

angular.module('starter.controllers', ['starter.heatmap', 'starter.metrics'])

.controller('HomeCtrl', function($scope) {
    $scope.active = 0;
    $scope.aggregateSlides = [];
    $scope.personalSlides = [];
    $scope.introSlides = [];

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
    };

    var addAggregateSlides = function() {
        $scope.aggregateSlides.push({
            image: 'img/aggregate/bike_march_2016.png',
            text: 'Bicycling trips in March 2016',
            id: 0
        });
        $scope.aggregateSlides.push({
            image: 'img/aggregate/bike_apr_2016.png',
            text: 'Bicycling trips in Apr 2016',
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
        $scope.personalSlides.push({
            image: 'img/personal/common_trips.png',
            text: 'Tour model',
            id: 2
        });
        $scope.personalSlides.push({
            image: 'img/personal/compare_with_common.png',
            text: 'Changes from normal',
            id: 3
        });
    };

    addAggregateSlides();
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

