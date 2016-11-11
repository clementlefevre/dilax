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
    datastore = Datastore(db_params=json_req, create=False)
    datastore.get_data()
    datastore.create_forecasts()

    prediction = Prediction(datastore)
    global_predictions[json_req['db_name']] = prediction
    sites = prediction.datastore.sites_infos
    json_ = sites.to_json(orient='records')
    json_array = json.loads(json_)
    return jsonify(result=json_array)


def get_prediction(json_req):
    global global_predictions
    print json_req
    prediction = global_predictions[json_req['db_params']['db_name']]

    prediction.make_prediction(json_req['site']['idbldsite'], "compensatedin")
    prediction_data = prediction.export_to_json("compensatedin")
    print prediction_data
    return jsonify(result=prediction_data)
