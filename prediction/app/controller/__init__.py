from flask import Blueprint

predictions = Blueprint('predictions', __name__)

from . import customer_controller
