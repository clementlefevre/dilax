froggyApp.controller('chartController', function ($scope,$http) {


  function get_customers(){
    $http.get('/predictions/customers/list').success(function(data) {
      $scope.customers = data.result;

    });
  };

  function get_sites(customer){
    customer_object = {'customer':$scope.customer}
    loadingDatas();
    $http.post('/predictions/customers/sites', customer_object).success(function (data) {
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
    predictor = {'customer':$scope.customer,
    'period' : $scope.period,
    'site': $scope.site,
    'label': $scope.label,
    'retrocheck':$scope.retrocheck}

    loadingDatas();
    $http.post('/predictions/create_prediction', predictor).success(function (data) {
      closeLoading();
      $scope.prediction_data = data.prediction;
      $scope.features = data.features;
      $scope.r2 = data.r2;
      $scope.rmse_rfr = data.rmse_rfr;
      $scope.accuracy_rfr = data.accuracy_rfr;
      $scope.rmse_xgb = data.rmse_xgb;
      $scope.accuracy_xgb = data.accuracy_xgb;
      $scope.create_training_set = data.create_training_set;
      

      console.log( $scope.prediction_data);

      chart_day.setData($scope.prediction_data);
      chart_hour.setData($scope.prediction_data);



    }).error(function (data, status) {
      closeLoading();
      alert("Error. Please try again or contact administrator.");
      return status;
    });
  }




  $(document).ready(function() {
    chart_day = Morris.Line({
      element: 'prediction_chart_day',
      data: [],
      xkey: 'date_time',
      ykeys: ['observed','predicted_rfr','predicted_xgb'],
      labels: ['observed','predicted_randomForest','predicted_xgboost'],
   
    });

  });

   $(document).ready(function() {
    chart_hour = Morris.Bar({
      element: 'prediction_chart_hour',
      data: [],
      xkey: 'index',
      ykeys: ['value'],
      labels: ['Value'],
    });

  });



  
  $scope.customers = get_customers();
  $scope.customer = "Select a customer";
  $scope.site = {"sname": "Select a site"};
  $scope.period = 'D';
  $scope.label = "compensatedin";
  $scope.pointSize = 3;
  $scope.retrocheck= false;
  $scope.create_training_set = false;



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