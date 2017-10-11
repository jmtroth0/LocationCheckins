from flask import Flask, jsonify, request
from flask_registry import Registry
import logging
from lib.errors import ResourceNotFound, AuthenticationError
import traceback
import wtforms_json
from lib.clients.cass import SimpleClient
import sys


def _initialize_flask_app():
    app = Flask(
        __name__
    )

    return app


def _register_blueprints(app):
    from api.location import location
    app.register_blueprint(location, url_prefix='/')

    return app


def _configure_logging(app):
    app.logger.addHandler(logging.getLogger('dimagi'))
    app.logger_name = 'dimagi'
    return app


def _initialize_wtforms_json(app):
    wtforms_json.init()
    return app


def _initialize_registry(app):
    reg = Registry(app=app)

    reg['CASSANDRA_CLIENT'] = _initialize_client(SimpleClient)

    from lib.repositories.location import LocationRepo
    reg['DB_LOCATION'] = LocationRepo()


def _initialize_client(client):
    _client_logger = logging.getLogger('cassandra_client')

    if not client.instance:
        _client_logger.info("Creating new connection")
        client.instance = client()
    if not client.instance.session:
        _client_logger.info("Creating new session")
        client.instance.connect(
            ['127.0.0.1'],
            'dimagi'
        )
    client.MISSING_ROUTING_KEY_WARNING = True

    return client.instance


def create_app():
    app = _initialize_flask_app()
    _initialize_registry(app)
    app = _configure_logging(app)
    app = _initialize_wtforms_json(app)
    app = _register_blueprints(app)
    app = _register_error_handlers(app)
    return app


def _register_error_handlers(app):
    @app.errorhandler(AuthenticationError)
    def handle_auth_error(error):
        return jsonify({'error': error.message, 'url': request.host_url}), 401

    @app.errorhandler(ResourceNotFound)
    def handle_resource_not_found_error(error):
        return jsonify({'error': error.message, 'url': request.host_url}), 404

    @app.errorhandler(Exception)
    def handle_error(e):
        exc_info = sys.exc_info()
        _, exc_value, exc_traceback = exc_info
        _logToLogger(app, exc_value, traceback.extract_tb(exc_traceback))
        raise

    def _logToLogger(app, message, extracted_stack):
        stacktrace = '{}\n========BEGIN VERIFY FAILED\n'.format(message)
        stacktrace += ''.join(
            [x.decode('utf-8') for x in traceback.format_list(extracted_stack)]
        )
        stacktrace += '========END VERIFY FAILED\n'
        app.logger.error(stacktrace)

    return app