angular.module('emission.plugin.logger', [])

.factory('Logger', function($window, $state, $interval, $rootScope) {
    var loggerJs = {}
    loggerJs.log = function(message) {
        console.log(message);
    }
    return loggerJs;
});
