froggyApp.controller('chartController', function ($scope,$http) {


  function get_customers(){
    $http.get('/predictions/customers/list').success(function(data) {
      $scope.customers = data.result;

    });
  };

  function get_sites(customer){
    loadingDatas();
    $http.post('/predictions/customers/sites', customer).success(function (data) {
      closeLoading();
      $scope.sites = data.result;
      console.log($scope.sites);

    }).error(function (data, status) {
      closeLoading();
      alert("Search error. Please try again or contact administrator.");
      return status;
    });
  };

  $scope.setCustomer = function(customer){
    $scope.customer =customer
    get_sites(customer);
  }

  $scope.setSite = function(site){
    $scope.site =site
  };


  $scope.createPrediction = function(){
    predictor = {'db_params':$scope.customer,
    'period' : $scope.period,
    'site': $scope.site}

    loadingDatas();
    $http.post('/predictions/create_prediction', predictor).success(function (data) {
      closeLoading();
      $scope.prediction_data = data.result;
      console.log( $scope.prediction_data);
     chart2.setData($scope.prediction_data);


    }).error(function (data, status) {
      closeLoading();
      alert("Error. Please try again or contact administrator.");
      return status;
    });
  }


    new Morris.Line({
  // ID of the element in which to draw the chart.
  element: 'myfirstchart',
  // Chart data records -- each entry in this array corresponds to a point on
  // the chart.
  data:$scope.predictions_data,

  // The name of the data record attribute that contains x-values.
  xkey: 'index',
  // A list of names of data record attributes that contain y-values.
  ykeys: ['value'],
  // Labels for the ykeys -- will be displayed when you hover over the
  // chart.
  labels: ['Value'],
  pointSize : 0
});

    $(document).ready(function() {
    
        chart2 = Morris.Line({
          element: 'LastIncome',
          data: [],
          xkey: 'index',
          ykeys: ['value'],
          labels: ['Value']
        });
 
});

  



  

  $scope.customers = get_customers();
  $scope.customer = {"db_name": "Select a customer"};
  $scope.site = {"sname": "Select a site"};
  $scope.period = 'D'



  var loadingDatas = function () {
    bootbox.dialog({
      message: '<span class="fa fa-cog fa-spin fa-4x"></span>',
      title: '<div ng-show="loading"> Retrieving Data... </div > '
    }
    );
  };
  var closeLoading = function () {
    bootbox.hideAll();
  };


});