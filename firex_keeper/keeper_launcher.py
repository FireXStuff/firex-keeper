import os
from psutil import Process, TimeoutExpired
import subprocess

from firexapp.submit.tracking_service import TrackingService
from firexapp.common import qualify_firex_bin
from firexapp.submit.console import setup_console_logging

from firex_keeper.keeper_helper import get_keeper_dir


logger = setup_console_logging(__name__)


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
        pid = subprocess.Popen(cmd,
                               stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                               close_fds=True).pid

        try:
            Process(pid).wait(0.1)
        except TimeoutExpired:
            logger.debug("Started background FireXKeeper with pid %s" % pid)
        else:
            logger.error("Failed to start FireXKeeper -- task DB will not be available.")

        return {}

    def ready_for_tasks(self, **kwargs) -> bool:
        return os.path.isfile(self.broker_recv_ready_file)
