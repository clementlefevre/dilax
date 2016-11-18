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
    'site': $scope.site,
    'label': $scope.label}

    loadingDatas();
    $http.post('/predictions/create_prediction', predictor).success(function (data) {
      closeLoading();
      $scope.prediction_data = data.result;
      $scope.features = data.features;
      $scope.r2 = data.r2;
      

      console.log( $scope.prediction_data);

      chart.setData($scope.prediction_data);


    }).error(function (data, status) {
      closeLoading();
      alert("Error. Please try again or contact administrator.");
      return status;
    });
  }




  $(document).ready(function() {
    chart = Morris.Line({
      element: 'prediction_chart',
      data: [],
      xkey: 'index',
      ykeys: ['value'],
      labels: ['Value'],
      pointSize : $scope.pointSize
    });

  });

  



  

  $scope.customers = get_customers();
  $scope.customer = {"db_name": "Select a customer"};
  $scope.site = {"sname": "Select a site"};
  $scope.period = 'D';
  $scope.label = "compensatedin";
  $scope.pointSize = 3;



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