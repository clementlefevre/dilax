from flask import jsonify
import pandas as pd
import json
from ..customers_config import all_db_params
from ..model.prediction import Prediction
from ..model.datastore.day import DayDatastore

global_predictions = {}

customers_list = [k for k, v in all_db_params.iteritems()]
print type(customers_list)


def customers():
    print customers_list
    return jsonify(result=customers_list), 200


def sites(json_req):
    global global_predictions

    customer = json_req['customer']

    db_params = all_db_params.get(customer)

    datastore_D = DayDatastore(db_params=db_params, intervals={
        'predict_from': '2016-12-5', 'predict_to': '2016-12-12'})
    datastore_D.get_data()
    prediction_D = Prediction(datastore_D)
    global_predictions[customer] = prediction_D
    sites = datastore_D.data.sites_infos.set
    json_ = sites.to_json(orient='records')
    json_array = json.loads(json_)
    return jsonify(result=json_array)


def get_prediction(json_req):
    global global_predictions
    label = json_req['label']
    customer = json_req['customer']
    idbldsite = json_req['site']['idbldsite']

    prediction = global_predictions[customer]

    prediction.make_prediction(
        json_req['site']['idbldsite'], label)

    if json_req['retrocheck']:
        observed = prediction.datastore.data.observed.set[
            ['date_time', 'idbldsite', label]]

        observed = observed[observed.idbldsite == idbldsite]

        predicted_and_observed = pd.merge(prediction.forecast_predictors[
            ['date_time', label, label + "_xgboost"]],
            observed, on='date_time', suffixes=["_predicted", '_observed'], how='left')

        predicted_and_observed.columns = [
            'date_time', 'predicted_rfr', 'predicted_xgb', 'idbldsite', 'observed']

        json_ = predicted_and_observed.to_json(orient='records')
        json_array = json.loads(json_)
        prediction_data = json_array
        print predicted_and_observed.head()

        rmse_rfr, accuracy_rfr = prediction.RMSE(
            predicted_and_observed, 'predicted_rfr')
        rmse_xgb, accuracy_xgb = prediction.RMSE(
            predicted_and_observed, 'predicted_xgb')

    else:
        json_ = prediction.forecast_predictors[
            ['date_time', label]].to_json(orient='records')
        json_array = json.loads(json_)
        prediction_data = json_array
        rmse, accuracy = 0, 0

    return jsonify(prediction=prediction_data, features=prediction.features_weighted,
                   r2=prediction.r2,
                   rmse_rfr=rmse_rfr, accuracy_rfr=accuracy_rfr,
                   rmse_xgb=rmse_xgb, accuracy_xgb=accuracy_xgb)
