from flask import Flask, jsonify

from api_server.athena import AthenaClient
from api_server.queries import QueryFactory


class Router:
    def __init__(self, flask_app: Flask, athena_client: AthenaClient, query_factory: QueryFactory):
        self.client = athena_client
        self.queries = query_factory
        self.app = flask_app

    def run(self, host: str, port: int):
        app = self.app

        @app.route('/<string:kind>')
        def latest(kind: str):
            raise NotImplemented

        @app.route('/<string:kind>/timeseries')
        def timeseries(kind: str):
            raise NotImplemented

        @app.route('/<string:kind>/by/<string:by>')
        def latest_grouped(kind: str, by: str):
            raise NotImplemented

        @app.route('/<string:kind>/by/<string:by>/timeseries')
        def timeseries_grouped(kind: str, by: str):
            raise NotImplemented

        @app.errorhandler(404)
        def not_found(_):
            return jsonify('endpoint does not exist'), 404

        app.run(host, port)
