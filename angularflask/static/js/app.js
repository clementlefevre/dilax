'use strict';   // See note about 'use strict'; below

var froggyApp = angular.module('froggyApp', [
 'ngRoute','angular.morris'
]);

froggyApp.config(['$routeProvider',
     function($routeProvider) {
         $routeProvider.
             when('/', {
                 templateUrl: '/static/partials/index.html',
             }).
             when('/about', {
                 templateUrl: '../static/partials/about.html',
             }).
            when('/forecasting', {
                 templateUrl: '../static/partials/forecasting.html',
             }).
            when('/chart', {
                 templateUrl: '../static/partials/chart.html',
                 controller: 'chartController'
             }).
             otherwise({
                 redirectTo: '/'
             });
    }]);