import argparse
import logging
import os
from psutil import Process, TimeoutExpired
import subprocess

from celery.app.base import Celery

from firexapp.broker_manager.broker_factory import RedisManager
from firexapp.submit.tracking_service import TrackingService
from firexapp.submit.uid import Uid
from firexapp.common import qualify_firex_bin
from firexapp.submit.console import setup_console_logging
from firexapp.events.model import RunMetadata
from firex_keeper.keeper_event_consumer import TaskDatabaseAggregatorThread


logger = setup_console_logging(__name__)


def get_keeper_dir(logs_dir):
    return os.path.join(logs_dir, Uid.debug_dirname, 'keeper')


def celery_app_from_logs_dir(logs_dir):
    return Celery(broker=RedisManager.get_broker_url_from_metadata(logs_dir))


class FireXKeeperLauncher(TrackingService):

    def __init__(self):
        self.broker_recv_ready_file = None

    def start(self, args, uid=None, **kwargs)->{}:
        logs_dir = uid.logs_dir
        self.broker_recv_ready_file = os.path.join(get_keeper_dir(logs_dir), 'keeper_celery_recvr_ready')

        cmd = [qualify_firex_bin("firex_keeper"),
               "--uid", str(uid),
               "--logs_dir", uid.logs_dir,
               "--chain", args.chain,
               "--broker_recv_ready_file", self.broker_recv_ready_file,
               ]
        pid = subprocess.Popen(cmd, close_fds=True).pid

        try:
            Process(pid).wait(0.1)
        except TimeoutExpired:
            logger.debug("Started background FireXKeeper with pid %s" % pid)
        else:
            logger.error("Failed to start FireXKeeper -- task DB will not be available.")

        return {}

    def ready_for_tasks(self, **kwargs) -> bool:
        return os.path.isfile(self.broker_recv_ready_file)


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

    run_metadata = RunMetadata(args.uid, args.logs_dir, args.chain)

    keeper_dir = get_keeper_dir(run_metadata.logs_dir)
    os.makedirs(keeper_dir, exist_ok=True)
    logging.basicConfig(filename=os.path.join(keeper_dir, 'keeper.log'), level=logging.DEBUG, filemode='w',
                        format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

    celery_app = celery_app_from_logs_dir(run_metadata.logs_dir)
    return celery_app, run_metadata, args.broker_recv_ready_file


def main():
    celery_app, run_metadata, receiver_ready_file = init_keeper()
    TaskDatabaseAggregatorThread(celery_app, run_metadata,
                                 receiver_ready_file=receiver_ready_file).run()
