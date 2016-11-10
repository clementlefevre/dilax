froggyApp.controller('chartController', function ($scope,$http) {


    $http.get('/static/js/controller/demo.json').success(function(data) {
    $scope.demo_data = data;

    console.log($scope.demo_data)

    new Morris.Line({
  // ID of the element in which to draw the chart.
  element: 'myfirstchart',
  // Chart data records -- each entry in this array corresponds to a point on
  // the chart.
  data:$scope.demo_data,
  // The name of the data record attribute that contains x-values.
  xkey: 'index',
  // A list of names of data record attributes that contain y-values.
  ykeys: ['value'],
  // Labels for the ykeys -- will be displayed when you hover over the
  // chart.
  labels: ['Value'],
  pointSize : 0
});
    });


    

});