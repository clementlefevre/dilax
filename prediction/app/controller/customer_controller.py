from flask import request, abort
from ..business_logic.service import customers_service

from . import predictions


@predictions.route('/customers/list', methods=['GET'])
def get_customers():
    return customers_service.customers()


@predictions.route('/customers/sites', methods=['POST'])
def get_sites():
    if request.method == 'POST':

        if not request.json:
            abort(400)
        return customers_service.sites(request.json)
    abort(400)


@predictions.route('/create_prediction', methods=['POST'])
def create_predictions():
    if request.method == 'POST':

        if not request.json or 'period' not in request.json:
            abort(400)
        return customers_service.get_prediction(request.json)
    abort(400)
