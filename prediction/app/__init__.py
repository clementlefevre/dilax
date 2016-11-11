from flask import Flask


def create_app():
    app = Flask(__name__)

    from controller import predictions as predictions_blueprint
    app.register_blueprint(predictions_blueprint, url_prefix='/predictions')
    print "hello"

    return app
