from flask import jsonify
from ..customers_config import all_db_params


def customers():
    return jsonify(result=all_db_params), 200
