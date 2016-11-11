from flask import jsonify
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

        datastore_D = Datastore(db_params=json_req, create=True, period='D')
        datastore_H = Datastore(db_params=json_req, create=True, period='H')

        datastore_D.get_data()
        datastore_D.create_forecasts()

        datastore_H.get_data()
        datastore_H.create_forecasts()

        prediction_D = Prediction(datastore_D)
        prediction_H = Prediction(datastore_H)

        global_predictions[json_req['db_name'] + 'D'] = prediction_D
        global_predictions[json_req['db_name'] + 'H'] = prediction_H

    sites = global_predictions[json_req['db_name'] + 'D'].datastore.sites_infos
    json_ = sites.to_json(orient='records')
    json_array = json.loads(json_)
    return jsonify(result=json_array)


def get_prediction(json_req):
    global global_predictions
    print json_req
    prediction = global_predictions[
        json_req['db_params']['db_name'] + json_req['period']]

    prediction.make_prediction(
        json_req['site']['idbldsite'], json_req['label'])
    prediction_data = prediction.export_to_json("compensatedin")
    print prediction_data
    return jsonify(result=prediction_data)
