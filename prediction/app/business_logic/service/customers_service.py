from flask import jsonify
import pandas as pd
import json
from ..customers_config import all_db_params
from ..model.prediction import Prediction
from ..model.datastore import Datastore

global_predictions = {}


def customers():
    return jsonify(result=all_db_params), 200


def sites(json_req):
    global global_predictions
    key = json_req['db_name'] + 'D'
    if key not in global_predictions:



        # datastore_D = Datastore(db_params=json_req, create=True, period='D')
        # datastore_H = Datastore(db_params=json_req, create=False, period='H')

        datastore_D_retro = Datastore(db_params=json_req, create=True, period='D', retrocheck=True, \
            dt_from="2016-11-17", dt_to="2016-11-24")

        # datastore_D.get_data()
        # datastore_D.create_forecasts()

        # datastore_H.get_data()
        # datastore_H.create_forecasts()


        datastore_D_retro.get_data()
        datastore_D_retro.create_forecasts()

        # prediction_D = Prediction(datastore_D)
        # prediction_H = Prediction(datastore_H)
        prediction_D_retro = Prediction(datastore_D_retro)

        # global_predictions[json_req['db_name'] + 'D'] = prediction_D
        # global_predictions[json_req['db_name'] + 'H'] = prediction_H
        global_predictions[json_req['db_name'] + 'D'+"_retro"] = prediction_D_retro

    sites = global_predictions[json_req['db_name'] + 'D'+"_retro"].datastore.sites_infos
    json_ = sites.to_json(orient='records')
    json_array = json.loads(json_)
    return jsonify(result=json_array)


def get_prediction(json_req):
    global global_predictions
    print "json_req['retrocheck']",json_req['retrocheck']
    if json_req['retrocheck']:
        retrocheck = "_retro"

    else:
        retrocheck=""

    

    prediction = global_predictions[
        json_req['db_params']['db_name'] + json_req['period']+retrocheck]

    prediction.make_prediction(
        json_req['site']['idbldsite'], json_req['label'])

    if json_req['retrocheck']:
        observed = prediction.datastore.observed_targets['counts']
        observed = observed[observed.idbldsite==json_req['site']['idbldsite']]
        observed = pd.merge(prediction.forecast_predictors[
            ['date_time', json_req['label']]], observed, on='date_time', suffixes=["_predicted",'_observed'])
        json_ = observed.to_json(orient = 'records')
        json_array = json.loads(json_)
        prediction_data= json_array

    else:
        prediction_data = prediction.export_to_json(json_req['label'])

    
    

    return jsonify(prediction=prediction_data, features=prediction.features_weighted, r2=prediction.r2)
