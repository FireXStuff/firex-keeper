import argparse
import logging
import os
import sys
import signal

# Prevent dependencies from taking module loading hit of pkg_resources.
noop_class = type('noop', (object,), {'iter_entry_points': lambda _: []})
sys.modules["pkg_resources"] = noop_class
sys.modules["celery.events.dispatcher"] = type('noop2', (object,), {'EventDispatcher': noop_class})

from gevent import monkey
monkey.patch_all()

from celery.app.base import Celery

from firexapp.broker_manager.broker_factory import RedisManager
from firexapp.events.model import FireXRunMetadata
from firexapp.common import wait_until

from firex_keeper.keeper_helper import get_keeper_dir
from firex_keeper.keeper_event_consumer import TaskDatabaseAggregatorThread

logger = logging.getLogger(__name__)


def celery_app_from_logs_dir(logs_dir):
    return Celery(broker=RedisManager.get_broker_url_from_logs_dir(logs_dir))


def _sig_handler(_, __):
    logger.warning("Received SIGTERM.")
    sys.exit(1)


def _sig_handler(_, __):
    logger.warning("Received SIGTERM.")
    sys.exit(1)


def init_keeper():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs_dir", help="Logs directory for the run to keep task data for.",
                        required=True)
    parser.add_argument("--uid", help="FireX UID for the run to keep task data for.",
                        required=True)
    parser.add_argument("--chain", help="Logs directory for the run to keep task data for.",
                        required=True)
    parser.add_argument('--broker_recv_ready_file', help='File to create immediately before capturing celery events.',
                        default=None)

    args = parser.parse_args()

    run_metadata = FireXRunMetadata(args.uid, args.logs_dir, args.chain, None)

    keeper_dir = get_keeper_dir(run_metadata.logs_dir)
    os.makedirs(keeper_dir, exist_ok=True)
    logging.basicConfig(filename=os.path.join(keeper_dir, 'keeper.log'), level=logging.DEBUG, filemode='w',
                        format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    logger.info('Starting Keeper with args: %s' % args)

    signal.signal(signal.SIGTERM, _sig_handler)

    celery_app = celery_app_from_logs_dir(run_metadata.logs_dir)
    return celery_app, run_metadata, args.broker_recv_ready_file


class ShutdownSignalHandler:

    def __init__(self):
        self.web_server = None
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

    def sigterm_handler(self, _, __):
        self.shutdown('SIGTERM detected')

    def sigint_handler(self, _, __):
        self.shutdown('SIGINT detected')

    def shutdown(self, reason):
        logging.info("Stopping entire Keeper for reason: %s" % reason)
        logging.shutdown()
        if self.web_server:
            self.web_server.stop()


def start_keeper(celery_app, run_metadata, receiver_ready_file):

    TaskDatabaseAggregatorThread(celery_app, run_metadata, receiver_ready_file=receiver_ready_file).start()
    wait_until(os.path.isfile, 10, 0.1, receiver_ready_file)

    # Delaying of importing of all web dependencies is a deliberate startup performance optimization.
    # The broker should be listening for events as quickly as possible.
    from firex_keeper.keeper_web_app import start_keeper_web_server
    return start_keeper_web_server(run_metadata)


def main():
    shutdown_handler = ShutdownSignalHandler()
    celery_app, run_metadata, receiver_ready_file = init_keeper()
    try:
        web_server = start_keeper(celery_app, run_metadata, receiver_ready_file)
        # Allow the shutdown handler to stop the web server before we serve_forever.
        shutdown_handler.web_server = web_server
        web_server.serve_forever()
    except Exception as e:
        logger.error("Keeper process terminating due to error.")
        logger.exception(e)
        shutdown_handler.shutdown(str(e))
    else:
        logger.info("Keeper process terminating gracefully.")
    finally:
        logger.info("Keeper finished.")
