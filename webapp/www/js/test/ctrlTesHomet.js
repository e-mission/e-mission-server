describe('Testing Home controller', function() {
  beforeEach(module('starter.controllers'));
  
  var $controller;// = function(_$controller_)
  
  beforeEach(inject(function(_$controller_){
    // The injector unwraps the underscores (_) from around the parameter names when matching
    $controller = _$controller_;
  }));
  
  it("sample slides are correctly added in for the Aggregate Slides", function() {
    var $scope = {}
    
    var controller = $controller('HomeCtrl', { $scope: $scope });
    expect($scope.aggregateSlides[0].id).toEqual(0);
    expect($scope.aggregateSlides[1].text).toEqual('Bicycling trips in Apr 2016');
  });
  
  it("sample slides are correctly added in for the Personal Slides", function() {
    var $scope = {}
    
    var controller = $controller('HomeCtrl', { $scope: $scope });
    //expect($scope).toEqual('fart')
    expect($scope.personalSlides[2].id).toEqual(2);
    expect($scope.personalSlides[3].text).toEqual('Changes from normal');
    expect($scope.personalSlides[0].image).toEqual('img/personal/list_view.png');
  });
  
});


