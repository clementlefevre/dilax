from ..business_logic.service import customers_service

from . import predictions


@predictions.route('/customers/', methods=['GET'])
def get_customers():
    print "coucoucoucouc"

    return customers_service.customers()
