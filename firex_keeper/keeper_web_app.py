import logging
import os

from gevent import pywsgi
from flask import Flask, jsonify, request

from firex_keeper.keeper_helper import update_keeper_metadata
from firex_keeper import keeper_file_client

logger = logging.getLogger(__name__)

TASKS_ROOT_URL = '/api/tasks'


def create_web_app():
    web_app = Flask(__name__)
    web_app.secret_key = os.urandom(24)
    web_app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    # Trivial 'OK' endpoint for testing if the server is up.
    web_app.add_url_rule('/alive', 'alive', lambda: ('', 200))

    return web_app


def get_keeper_url(port, hostname=None):
    if hostname is None:
        from socket import gethostname
        hostname = gethostname()
    return 'http://%s:%d' % (hostname, int(port))


def create_keeper_rest_api(web_app, logs_dir):

    @web_app.route(TASKS_ROOT_URL)
    def get_all_tasks():
        expect_single_task = request.args.get('single')
        queried_state = request.args.get('state')
        queried_name = request.args.get('name')
        if queried_name:
            tasks_with_name = keeper_file_client.tasks_by_name(logs_dir, queried_name)
            if queried_state:
                tasks = [t for t in tasks_with_name if t.state == queried_state]
            else:
                tasks = tasks_with_name
        elif queried_state:
            tasks = keeper_file_client.tasks_by_state(logs_dir, queried_state)
        else:
            tasks = keeper_file_client.all_tasks(logs_dir)

        if expect_single_task:
            if len(tasks) != 1:
                return '', 400
            return tasks[0]._asdict()

        return jsonify([t._asdict() for t in tasks])

    @web_app.route('/api/tasks/<uuid>')
    def get_task_by(uuid):
        try:
            return jsonify(keeper_file_client.task_by_uuid(logs_dir, uuid))
        except keeper_file_client.FireXTaskQueryException:
            return '', 404
    #
    # @web_app.route('/api/run-metadata')
    # def get_run_metadata():
    #     return jsonify(_run_metadata_to_api_model(run_metadata, event_aggregator.root_uuid))


def start_keeper_web_server(run_metadata):
    web_app = create_web_app()
    create_keeper_rest_api(web_app, run_metadata.logs_dir)

    server = pywsgi.WSGIServer(('', 0), web_app)
    server.start()  # Need to start() to initialize port.

    update_keeper_metadata(run_metadata.logs_dir, {
        'keeper_url': get_keeper_url(server.server_port),
        'is_complete': False,
    })

    return server



