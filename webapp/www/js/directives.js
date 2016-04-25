angular.module('starter.directives', [])

.directive('em-local-date', function(){
  return {
    restrict: 'E',
    scope: {
      localDate: '=date'
    },
    templateUrl: 'templates/local-date-components.html'
  };
});
